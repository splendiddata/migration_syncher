/*
 * Copyright (c) Splendid Data Product Development B.V. 2013
 * 
 * This program is free software: You may redistribute and/or modify under the 
 * terms of the GNU General Public License as published by the Free Software 
 * Foundation, either version 3 of the License, or (at Client's option) any 
 * later version.
 * 
 * This program is distributed in the hope that it will be useful, but WITHOUT 
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU General Public License for more
 * details.
 *
 * You should have received a copy of the GNU General Public License along with 
 * this program.  If not, Client should obtain one via www.gnu.org/licenses/.
 */

package com.splendiddata.intermal.migrationsyncher;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Paths;

import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.lib.ObjectId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.splendiddata.internal.migrationsyncher.MigrationSyncherGitInterface;
import com.splendiddata.internal.migrationsyncher.MigrationSyncherProperties;

/**
 * Tests the MigrationSyncherGitInterface
 *
 * @author Splendid Data Product Development B.V.
 * @since 0.0.1
 */
public class TestMigrationSyncherGitInterface {
    static MigrationSyncherProperties properties;

    @BeforeAll
    static void init() throws IOException {
        properties = new MigrationSyncherProperties(Paths.get("src/test/resources/test.properties"));
    }

    @Test
    public void connect() throws IOException, GitAPIException, URISyntaxException {
        /*
         * First time: git clone
         */
        MigrationSyncherGitInterface gitInterface = new MigrationSyncherGitInterface(properties);
        Assertions.assertNotNull(gitInterface, "first invocation of new MigrationSyncherGitInterface(properties)");
        /*
         * Second time: git pull
         */
        gitInterface = new MigrationSyncherGitInterface(properties);
        Assertions.assertNotNull(gitInterface, "second invocation of new MigrationSyncherGitInterface(properties)");

        ObjectId head = gitInterface.getLastCommitId();
        Assertions.assertNotNull(head, "Expecting a hash from gitInterface.getLastCommitId()");

        ObjectId tree = gitInterface.getTreeId(head);
        Assertions.assertNotNull(tree, "Expecting a hash from gitInterface.getTreeId(head)");
        Assertions.assertNotEquals(head, tree, "Expecting different hashes for the head and it's tree");
    }
}
