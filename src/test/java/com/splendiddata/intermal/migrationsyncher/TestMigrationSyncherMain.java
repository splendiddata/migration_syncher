/*
 * Copyright (c) Splendid Data Product Development B.V. 2013
 * 
 * This program is free software: You may redistribute and/or modify under the terms of the GNU General Public License
 * as published by the Free Software Foundation, either version 3 of the License, or (at Client's option) any later
 * version.
 * 
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program. If not, Client should
 * obtain one via www.gnu.org/licenses/.
 */

package com.splendiddata.intermal.migrationsyncher;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Objects;
import java.util.TreeSet;
import java.util.stream.Stream;

import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.lib.ObjectId;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.splendiddata.internal.migrationsyncher.MigrationSyncherDbInterface;
import com.splendiddata.internal.migrationsyncher.MigrationSyncherGitInterface;
import com.splendiddata.internal.migrationsyncher.MigrationSyncherMain;

/**
 * Keeps the database in synch with some historic commits from this project
 *
 * @author Splendid Data Product Development B.V.
 * @since 0.0.1
 */
public class TestMigrationSyncherMain extends MigrationSyncherMain {
    /**
     * Creates a test database
     * 
     * @see TestMigrationSyncherDbInterface#init()
     * @throws IOException
     *             from the properties file
     * @throws ClassNotFoundException
     *             if the database driver appears absent
     */
    @BeforeAll
    static void init() throws ClassNotFoundException, IOException {
        TestMigrationSyncherDbInterface.init();
    }

    /**
     * Removes the test database
     * 
     * @see TestMigrationSyncherDbInterface#cleanup()
     * @throws IOException
     *             doesn't. Would have done that in {@link #init()}
     * @throws ClassNotFoundException
     *             doesn't. Would have done that in {@link #init()}
     */
    @AfterAll
    static void cleanup() throws ClassNotFoundException, IOException {
        TestMigrationSyncherDbInterface.cleanup();
    }

    /**
     * Tests if an initial properties file is created when the {@link MigrationSyncherMain#main(String[])} method is
     * invoked without arguments
     */
    @Test
    public void testMissingProperties() {
        try {
            main(new String[0]);
            Assertions.fail("Invocation of main() with no arguments is supposed to fail");
        } catch (Exception e) {
            Assertions.assertEquals("Not all files have properly been synched into the database", e.getMessage(),
                    "exception from main() without arguments");
        }
        Assertions.assertTrue(new File(DEFAULT_PROPERTIES_FILE).exists(),
                "expecting main() without parameters to have created properties file: " + DEFAULT_PROPERTIES_FILE);
        // cleanup
        new File(DEFAULT_PROPERTIES_FILE).delete();
    }

    static Stream<Arguments> liveTest() {
        return Stream.of(Arguments.arguments("9726dfd85ff6cd4ce336dedd8e339ccf17a8993e", false, Collections.emptySet()),
                Arguments.arguments("682adee3f03392aa7210238822c5d2818ddea35e", true,
                        Arrays.asList("src/test/resources/sql/create/create table a.sql")),
                Arguments.arguments("e62d515af4e59b10b3e9aea9e49ac6c56ca2d650", true, Arrays.asList(
                        "src/test/resources/sql/create/create table a.sql",
                        "src/test/resources/sql/update/file containing an error that is supposed to be removed later",
                        "src/test/resources/sql/update/some_function.sql")),
                Arguments.arguments("adb47fb9b5abdceb5a369a3c9e6fe53f9a927535", true,
                        Collections.singletonList("src/test/resources/sql/update/some_function.sql")),
                Arguments.arguments("head", false, Collections.emptySet()));
    }

    /**
     * Largely copied from {@link MigrationSyncherMain#main(String[])}. But it takes a historic commitId as commitId
     * that it would have got from git as head.
     * <p>
     * The idea is to execute the MigrationSincher using historical commits from this project
     * </p>
     * 
     * @param gitCommitId
     *            The commitId that is to be taken as "head" for this test. If null, the original main function is
     *            invoked as "the rest of the commit history".
     * @param expectedToFail
     *            Does this commit any file that will fail execution in the database
     * @param failedFiles
     *            The failed files that are supposed to be registered in the database at this commit
     * @throws IOException
     *             when applicable
     * @throws SQLException
     *             when applicable
     * @throws ClassNotFoundException
     *             when applicable
     * @throws URISyntaxException
     *             when applicable
     * @throws GitAPIException
     *             when applicable
     */
    @ParameterizedTest
    @MethodSource
    public void liveTest(String gitCommitId, boolean expectedToFail, Collection<String> failedFiles)
            throws IOException, ClassNotFoundException, SQLException, GitAPIException, URISyntaxException {
        /*
         * When the second argument is null, the MigrationSyncherMain.main(args) itself is invoked
         */
        if ("head".equals(gitCommitId)) {
            try {
                MigrationSyncherMain.main(new String[] { PropertiesPath.PROPERTIES_PATH.toString() });
                Assertions.assertFalse(expectedToFail, "Unexpected success of MigrationSyncherMain.main()");
            } catch (RuntimeException e) {
                Assertions.assertTrue(expectedToFail, "Unexpected failure of MigrationSyncherMain.main()");
            }
            try (MigrationSyncherDbInterface dbConn = new MigrationSyncherDbInterface(properties)) {
                Assertions.assertEquals(new TreeSet<String>(failedFiles),
                        new TreeSet<String>(dbConn.getErroneousFiles()), "registered erroneous files not as expected");
            }
            return;
        }

        processPropertiesFile(PropertiesPath.PROPERTIES_PATH.toString());

        try (MigrationSyncherDbInterface dbConn = new MigrationSyncherDbInterface(properties);
                MigrationSyncherGitInterface gitInterface = new MigrationSyncherGitInterface(properties)) {
            db = dbConn;
            git = gitInterface;

            dbCommitId = db.getLastCommitId();

            /*
             * Here is a deviation from the original main() function:
             */
            fsCommitId = ObjectId.fromString(gitCommitId);
            Git.open(new File(properties.getGitLocalRepository())).checkout().setName(gitCommitId).call();

            if (Objects.equals(dbCommitId, fsCommitId)) {
                log.info("Nothing changed");
            } else {
                if (dbCommitId != null) {
                    doSynchronise(true);
                }
                db.setLastCommitId(fsCommitId);
            }

            Assertions.assertEquals(expectedToFail, errorEncountered, "errorEncountered value");
            Assertions.assertEquals(new TreeSet<String>(failedFiles), new TreeSet<String>(db.getErroneousFiles()),
                    "registered erroneous files not as expected");
        }
    }

}
