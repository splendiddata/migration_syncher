/*
 * Copyright (c) Splendid Data Product Development B.V. 2020
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

package com.splendiddata.internal.migrationsyncher;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.diff.DiffEntry;
import org.eclipse.jgit.errors.IncorrectObjectTypeException;
import org.eclipse.jgit.errors.MissingObjectException;
import org.eclipse.jgit.lib.ObjectId;

/**
 * Main program to keep a database definition in synch with a git repository
 * 
 * @author Splendid Data Product Development B.V.
 * @since 0.0.1
 */
public class MigrationSyncherMain {
    protected static final Logger log = LogManager.getLogger(MigrationSyncherMain.class);

    public static final String DEFAULT_PROPERTIES_FILE = "MigrationSyncher.properties";
    protected static MigrationSyncherProperties properties;
    protected static MigrationSyncherDbInterface db;
    protected static MigrationSyncherGitInterface git;

    protected static ObjectId dbCommitId;
    protected static ObjectId fsCommitId;
    private static List<DiffEntry> alteredFiles;

    protected static boolean errorEncountered = false;

    /**
     * @param args
     *            The first argument is the properties file. Following arguments are ignored.
     * @throws RuntimeException
     *             if not all files are processed correctly. The main reason for this exception is to draw attention in
     *             the Jenkins job status.
     */
    public static void main(String[] args) {
        log.info("start MigrationSyncherMain");
        List<String> remainingFilesInError = Collections.emptyList();
        try {
            errorEncountered = processPropertiesFile(args.length > 0 ? args[0] : DEFAULT_PROPERTIES_FILE);

            if (!errorEncountered) {
                try (MigrationSyncherDbInterface dbConn = new MigrationSyncherDbInterface(properties);
                        MigrationSyncherGitInterface gitInterface = new MigrationSyncherGitInterface(properties)) {
                    db = dbConn;
                    git = gitInterface;

                    dbCommitId = db.getLastCommitId();
                    fsCommitId = git.getLastCommitId();
                    if (Objects.equals(dbCommitId, fsCommitId)) {
                        log.info(new StringBuilder().append("Nothing changed at commit id: ")
                                .append(dbCommitId.getName()).append(", just perform retries"));
                        doSynchronise(false);
                    } else {
                        if (dbCommitId == null) {
                            log.info("setting initial commit id " + fsCommitId.getName());
                        } else {
                            log.info(new StringBuilder().append("processing files between commit id: ")
                                    .append(dbCommitId.getName()).append(" and: ").append(fsCommitId.getName()));
                            doSynchronise(true);
                        }
                        db.setLastCommitId(fsCommitId);
                    }
                    if (errorEncountered) {
                        remainingFilesInError = db.getErroneousFiles();
                    }
                }
            }
        } catch (Exception e) {
            log.error(e, e);
            errorEncountered = true;
        }
        if (errorEncountered) {
            log.warn("finish MigrationSyncherMain - not ok" + (remainingFilesInError.isEmpty() ? ""
                    : "\n" + remainingFilesInError.stream().collect(Collectors.joining("\n"))));
            throw new RuntimeException("Not all files have properly been synched into the database");
        }
        log.info("finish MigrationSyncherMain - ok");
    }

    /**
     * Reads the properties file if it exists or creates an initial one if it doesn't
     *
     * @param propertiesArg
     *            The (path) name of the properties file from the first argument in the {@link #main(String[])} method
     * @return boolean true if ok, false if the properties file didn't exist
     * @throws IOException
     *             if the properties file does exist but is not readable
     */
    protected static boolean processPropertiesFile(String propertiesArg) throws IOException {
        Path propertiesPath = Paths.get(propertiesArg);
        properties = new MigrationSyncherProperties(propertiesPath);
        if (propertiesPath.toFile().exists()) {
            log.info("properties:" + properties);
        } else {
            Files.createDirectories(propertiesPath.toAbsolutePath().getParent());
            try (Writer writer = new FileWriter(propertiesPath.toFile())) {
                writer.append(properties.generateInitialContent());
            }
            log.info("Created example properties file: " + propertiesPath.toAbsolutePath());
            log.info("Please correct that properties file and run the " + MigrationSyncherMain.class.getName()
                    + " program using the properties filename as argument");
            log.info("finish MigrationSyncherMain");
            return true;
        }
        return false;
    }

    /**
     * Applies all changed files from the Git repository and then retries files that failed in a previous run (they
     * might succeed now because of a change in the latest committed files)
     * 
     * @param changedFilesExpected
     *            Can we expect new files from Git (true) or is this just a retry of historically failed files (false)
     * @throws IncorrectObjectTypeException
     *             from JGit
     * @throws MissingObjectException
     *             from JGit
     * @throws IOException
     *             from the file system
     * @throws GitAPIException
     *             from JGit
     * @throws SQLException
     *             from the database if the file administration caused a problem
     */
    protected static void doSynchronise(boolean changedFilesExpected)
            throws IncorrectObjectTypeException, MissingObjectException, IOException, GitAPIException, SQLException {
        if (changedFilesExpected) {
            /*
             * Obtain the changes in Git since the last commit that was processed into the database
             */
            alteredFiles = git.getAlteredFiles(dbCommitId, fsCommitId);
        } else {
            alteredFiles = Collections.emptyList();
        }
        /*
         * Remove files that have changed and list the ones that remain
         */
        List<String> historicalErronoeousFiles = getRemainingErroneousFiles();

        if (changedFilesExpected) {
            /*
             * Execute the altered files into the database
             */
            for (String fileName : filterAlteredFiles()) {
                executeFile(fileName);
            }
        }

        /*
         * Retry the files that failed in a previous run. The latest applied files might have removed the error cause of
         * a historical failure
         */
        for (String fileToRetry : historicalErronoeousFiles) {
            if (executeFile(fileToRetry)) {
                db.removeErroneousFile(fileToRetry);
            }
        }
    }

    /**
     * Selects the files that are to be applied to the database from the list of altered files from Git.
     * <p>
     * Removed files will be ignored.
     * </p>
     * <p>
     * if include directories are specified in the properties, then only files in these directories will pass.
     * </p>
     *
     * @return List&lt;String&gt; with the relative (to the repository) pathnames of files that are to be executed in
     *         the database
     */
    private static List<String> filterAlteredFiles() {
        List<String> fileNames = new LinkedList<>();

        for (DiffEntry diffEntry : alteredFiles) {
            switch (diffEntry.getChangeType()) {
            case ADD:
            case COPY:
            case RENAME:
                fileNames.add(diffEntry.getNewPath());
                break;
            case MODIFY:
                fileNames.add(diffEntry.getOldPath());
                break;
            case DELETE:
            default:
                break;
            }
        }

        if (properties.getIncludeDirectories().isEmpty()) {
            return fileNames;
        }

        for (ListIterator<String> it = fileNames.listIterator(); it.hasNext();) {
            boolean remove = true;
            Path filePath = Paths.get(properties.getGitLocalRepository(), it.next());
            for (Path includeDir : properties.getIncludeDirectories()) {
                if (filePath.startsWith(includeDir)) {
                    remove = false;
                    break;
                }
            }
            if (remove) {
                it.remove();
            }
        }
        return fileNames;
    }

    /**
     * Removes all erroneous files that are altered since the last git commit that was applied to the database and
     * returns all erroneous files remaining.
     *
     * @return List&lt;String&gt; with pathnames
     * @throws SQLException
     *             if the database feels to do so
     */
    private static List<String> getRemainingErroneousFiles() throws SQLException {
        /*
         * Remove possible old registrations of erroneous files that have been changed now
         */
        for (DiffEntry file : alteredFiles) {
            switch (file.getChangeType()) {
            case ADD:
                db.removeErroneousFile(file.getNewPath());
                break;
            case COPY:
            case RENAME:
                db.removeErroneousFile(file.getOldPath());
                db.removeErroneousFile(file.getNewPath());
                break;
            case DELETE:
            case MODIFY:
                db.removeErroneousFile(file.getOldPath());
                break;
            default:
                log.error(new StringBuilder().append("unknown changeType: ").append(file.getChangeType().name())
                        .append(" for old path: ").append(file.getOldPath()).append(" and new path: ")
                        .append(file.getNewPath()).append(" in: ").append(file));
            }
        }
        return db.getErroneousFiles();
    }

    /**
     * Read the specified file from the git repository and executes it in the database
     *
     * @param file
     *            The pathname, relative to the git repository, of the file that is to be executed
     * @return boolean true if ok, false if not ok
     * @throws IOException
     *             on problems with the file
     * @throws SQLException
     *             if registration of the file in the database failed.
     */
    private static boolean executeFile(String file) throws IOException, SQLException {
        String fileContent = Files.readString(Paths.get(properties.getGitLocalRepository(), file));
        if (fileContent.charAt(0) == 0xFEFF) {
            // Remove the BOM character
            fileContent = fileContent.substring(1);
        }

        SQLException ex = db.executeFileContent(fileContent);
        if (ex == null) {
            log.info(file + " - ok");
            db.logFileExecution(file, null, null);
            return true;
        }

        log.info(file + " - not ok");
        errorEncountered = true;
        db.logFileExecution(file, ex.getSQLState(), ex.getMessage());
        db.setErroneousFile(file, ex.getSQLState(), ex.getMessage());
        return false;
    }
}
