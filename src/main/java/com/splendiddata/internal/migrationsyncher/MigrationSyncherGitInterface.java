/*
 * Copyright (c) Splendid Data Product Development B.V. 2020 - 2021
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

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.eclipse.jgit.api.CreateBranchCommand.SetupUpstreamMode;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.CheckoutConflictException;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.api.errors.InvalidRefNameException;
import org.eclipse.jgit.api.errors.RefAlreadyExistsException;
import org.eclipse.jgit.api.errors.RefNotFoundException;
import org.eclipse.jgit.diff.DiffEntry;
import org.eclipse.jgit.errors.AmbiguousObjectException;
import org.eclipse.jgit.errors.IncorrectObjectTypeException;
import org.eclipse.jgit.errors.MissingObjectException;
import org.eclipse.jgit.errors.RepositoryNotFoundException;
import org.eclipse.jgit.errors.RevisionSyntaxException;
import org.eclipse.jgit.lib.Constants;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.ObjectReader;
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.revwalk.RevWalk;
import org.eclipse.jgit.treewalk.CanonicalTreeParser;

/**
 * Represents a Git repository
 *
 * @author Splendid Data Product Development B.V.
 * @since 0.0.1
 */
public class MigrationSyncherGitInterface implements Closeable {
    private static final org.apache.logging.log4j.Logger log = LogManager.getLogger(MigrationSyncherGitInterface.class);

    /**
     * Environment variable that could contain a proxy server for https connections
     */
    private static final String ENV_HTTPS_PROXY = "https_proxy";
    private final Git git;
    private Path repoDirectory;
    private final List<String> environmentVariables = new ArrayList<>();
    private final File passwordScriptFile;

    /**
     * Constructor
     * <p>
     * Attaches a git repository in the file system. If the local repository is already present, then a git pull will be
     * executed. Otherwise a git clone will be executed.
     * </p>
     * <p>
     * If the repository is not at the requested branch (see properties) then a checkout will be performed.
     * </p>
     *
     * @param properties
     *            Properties from the provided properties file that contains information on the where-abouts of the
     *            local and remote repository, git credentials and desired branch
     * @throws IOException
     *             when applicable
     * @throws GitAPIException
     *             when applicable
     * @throws URISyntaxException
     *             when applicable
     */
    public MigrationSyncherGitInterface(MigrationSyncherProperties properties)
            throws IOException, GitAPIException, URISyntaxException {
        /*
         * The git password can be provided by registering a script file in the GIT_ASKPASS environment variable that
         * returns the password.
         */
        if (properties.getGitPassword() == null) {
            passwordScriptFile = null;
        } else {
            passwordScriptFile = Paths.get(System.getProperty("java.io.tmpdir", "/tmp"), "gitaskpass.sh").toFile();
            try (PrintWriter pwFile = new PrintWriter(new FileWriter(passwordScriptFile))) {
                pwFile.println("#!/bin/bash");
                pwFile.println("exec echo \"$GIT_PASSWORD\"");
            }
            passwordScriptFile.setExecutable(true);
            environmentVariables.add("GIT_PASSWORD=" + properties.getGitPassword());
            environmentVariables.add("GIT_ASKPASS=" + passwordScriptFile.getAbsolutePath());
        }
        String proxy = System.getenv(ENV_HTTPS_PROXY);
        if (proxy != null) {
            proxy = ENV_HTTPS_PROXY + "=" + proxy;
            environmentVariables.add(proxy);
            log.info("Using: " + proxy);
        }

        repoDirectory = Paths.get(properties.getGitLocalRepository());
        Git gitInstance = null;
        if (repoDirectory.toFile().exists()) {
            try {
                gitInstance = Git.open(repoDirectory.toFile());
            } catch (RepositoryNotFoundException e) {
                log.debug(repoDirectory + " is not (yet) a git repository");
            }
        }
        if (gitInstance == null) {
            if (!repoDirectory.toFile().exists()) {
                Files.createDirectories(repoDirectory);
            }
            gitInstance = clone(properties);
            checkoutBranch(properties, gitInstance);
            log.debug("Initialised repository " + repoDirectory.toAbsolutePath());
        } else {
            checkoutBranch(properties, gitInstance);
            if (properties.getGitRemoteRepositoryUrl() != null) {
                gitInstance = pull(properties);
            }
            log.debug("Pulled " + repoDirectory.toAbsolutePath());
        }
        git = gitInstance;
    }

    /**
     * Performs a git checkout if necessary
     *
     * @param properties
     *            The properties that contain the branch name to check out
     * @param gitInsance
     *            The Git repository that is supposed to have the branch
     * @throws IOException
     *             for the file system
     * @throws RefAlreadyExistsException
     *             from git
     * @throws RefNotFoundException
     *             from git
     * @throws InvalidRefNameException
     *             from git
     * @throws CheckoutConflictException
     *             from git
     * @throws GitAPIException
     *             from git
     */
    private void checkoutBranch(MigrationSyncherProperties properties, Git gitInsance)
            throws IOException, RefAlreadyExistsException, RefNotFoundException, InvalidRefNameException,
            CheckoutConflictException, GitAPIException {
        /*
         * Check if we are in the desired branch
         */
        String actualBranch = gitInsance.getRepository().getBranch();
        if (properties.getGitBranch() != null && !properties.getGitBranch().equals(actualBranch)) {
            boolean existingBranch = false;
            for (Ref ref : gitInsance.branchList().call()) {
                log.trace(() -> "existing branch: " + ref.getName());
                if (ref.getName().endsWith(properties.getGitBranch())) {
                    existingBranch = true;
                    break;
                }
            }

            log.info(new StringBuilder().append("Switching repository from branch \"").append(actualBranch)
                    .append("\" to branch \"").append(properties.getGitBranch()).append("\""));
            gitInsance.checkout().setName(properties.getGitBranch()).setUpstreamMode(SetupUpstreamMode.TRACK)
                    .setStartPoint("origin/" + properties.getGitBranch()).setForceRefUpdate(true)
                    .setCreateBranch(!existingBranch).call();
        }
    }

    /**
     * Executes a git clone to obtain the repository
     * <p>
     * The git clone command is executed in a separate process because setting up an https session using JGit is a
     * dread.
     * </p>
     *
     * @param properties
     *            with the local and remote repository location and git credentials
     * @return Git the Git object that represents the cloned repository
     * @throws IOException
     *             where applicable
     * @throws GitAPIException
     *             where applicable
     * @throws URISyntaxException
     *             when applicable
     */
    private Git clone(MigrationSyncherProperties properties) throws IOException, GitAPIException, URISyntaxException {
        /*
         * Build the git command line
         */
        StringBuilder commandLine = new StringBuilder().append("git clone");
        URI remoteRepoUrl = new URI(properties.getGitRemoteRepositoryUrl());
        if ("https".equals(remoteRepoUrl.getScheme())) {
            if (properties.getGitCertificate() != null) {
                commandLine.append(" -c http.sslcert=").append(properties.getGitCertificate());
            }
            if (properties.getGitKey() != null) {
                commandLine.append(" -c http.sslkey=").append(properties.getGitKey());
            }
            commandLine.append(" -c http.sslverify=false");
        }

        commandLine.append(' ').append(remoteRepoUrl.getScheme()).append("://");
        if (remoteRepoUrl.getUserInfo() == null && properties.getGitUser() != null) {
            commandLine.append(properties.getGitUser().replace("@","%40")).append('@');
        }
        commandLine.append(remoteRepoUrl.getHost());
        if (remoteRepoUrl.getPort() > 0) {
            commandLine.append(':').append(remoteRepoUrl.getPort());
        }
        commandLine.append(remoteRepoUrl.getPath()).append(' ')
                .append(Paths.get(properties.getGitLocalRepository()).toAbsolutePath());

        /*
         * Execute the clone command
         */
        log.info("Execute: " + commandLine);
        Process cmd = Runtime.getRuntime().exec(commandLine.toString(),
                environmentVariables.toArray(new String[environmentVariables.size()]));
        listenToStdoutAndStderr(cmd);

        /*
         * Now open the repository
         */
        return Git.open(repoDirectory.toFile());
    }

    /**
     * Returns the current commit hash
     *
     * @return ObjectId of the last commit in the current repository
     * @throws RevisionSyntaxException
     *             when applicable
     * @throws AmbiguousObjectException
     *             when applicable
     * @throws IncorrectObjectTypeException
     *             when applicable
     * @throws IOException
     *             when applicable
     */
    public ObjectId getLastCommitId()
            throws RevisionSyntaxException, AmbiguousObjectException, IncorrectObjectTypeException, IOException {
        log.trace(() -> "@>getLastCommitId() = result");
        ObjectId result = git.getRepository().resolve(Constants.HEAD);
        log.debug(() -> "@<getLastCommitId() = result");
        return result;
    }

    /**
     * Returns the tree for the given commit id
     *
     * @param commitId
     *            The hash that represents the desired (historical) commit. This id has probably been obtained via
     *            {@link #getLastCommitId()}, maybe in a previous session.
     * @return ObjectId The hash that represents the tree for the commitId
     * @throws MissingObjectException
     *             when applicable
     * @throws IncorrectObjectTypeException
     *             when applicable
     * @throws IOException
     *             when applicable
     */
    public ObjectId getTreeId(ObjectId commitId)
            throws MissingObjectException, IncorrectObjectTypeException, IOException {
        log.trace(() -> new StringBuilder().append("@>getTreeId(commitId=").append(commitId).append(")"));
        ObjectId result;
        try (RevWalk revWalk = new RevWalk(git.getRepository())) {
            result = revWalk.parseCommit(commitId).getTree().getId();
        }
        log.debug(() -> new StringBuilder().append("@<getTreeId(commitId=").append(commitId).append(") = ")
                .append(result));
        return result;
    }

    /**
     * Returns all pathnames of files that have been altered between the two commits (excluding the historicalCommitId,
     * including the latestCommmitId).
     *
     * @param historicalCommitId
     *            A commit id from a previous run
     * @param latestCommmitId
     *            The current commit id, probably obtained via {@link #getLastCommitId()}
     * @return List&lt;DiffEntry&gt; containing the pathnames of files that have changed between the two commits
     * @throws IncorrectObjectTypeException
     *             when applicable
     * @throws MissingObjectException
     *             when applicable
     * @throws IOException
     *             when applicable
     * @throws GitAPIException
     *             when applicable
     */
    public List<DiffEntry> getAlteredFiles(ObjectId historicalCommitId, ObjectId latestCommmitId)
            throws IncorrectObjectTypeException, MissingObjectException, IOException, GitAPIException {
        log.trace(() -> new StringBuilder().append("@>getAlteredFiles(=").append(historicalCommitId)
                .append(", latestCommmitId=").append(latestCommmitId).append(")"));
        List<DiffEntry> diffs;
        Repository repository = git.getRepository();
        try (ObjectReader reader = repository.newObjectReader()) {
            CanonicalTreeParser oldTreeIter = new CanonicalTreeParser();
            oldTreeIter.reset(reader, getTreeId(historicalCommitId));
            CanonicalTreeParser newTreeIter = new CanonicalTreeParser();
            newTreeIter.reset(reader, getTreeId(latestCommmitId));
            diffs = git.diff().setShowNameAndStatusOnly(true).setNewTree(newTreeIter).setOldTree(oldTreeIter).call();
            log.debug(() -> new StringBuilder().append("@<getAlteredFiles(=").append(historicalCommitId)
                    .append(", latestCommmitId=").append(latestCommmitId).append(") = ").append(diffs.size())
                    .append(" DiffEntires"));
        }
        return diffs;
    }

    /**
     * Executes git pull
     * <p>
     * The git pull command is executed in a separate process because setting up an https session using JGit is a dread.
     * </p>
     *
     * @param properties
     *            with the local and remote repository location and git credentials
     * @return Git the Git object that represents the repository
     * @throws IOException
     *             where applicable
     * @throws GitAPIException
     *             where applicable
     */
    private Git pull(MigrationSyncherProperties properties) throws IOException, GitAPIException {

        /*
         * Execute the clone command
         */
        log.info("Execute: git pull at " + repoDirectory);
        Process cmd = Runtime.getRuntime().exec("git pull",
                environmentVariables.toArray(new String[environmentVariables.size()]), repoDirectory.toFile());
        listenToStdoutAndStderr(cmd);

        /*
         * Now open the repository
         */
        return Git.open(repoDirectory.toFile());
    }

    /**
     * Closes the Git repository
     * 
     * @see java.io.Closeable#close()
     */
    @Override
    public void close() {
        git.close();

        /*
         * If a password script file was created, it will be removed here.
         */
        if (passwordScriptFile != null && passwordScriptFile.exists()) {
            passwordScriptFile.delete();
        }
    }

    /**
     * Logs everything that comes from stdout and stderr of the process.
     * <p>
     * This method will block until the process has finished with a maximum of five minutes. If the process takes more
     * than five minutes to finish, it will be destroyed.
     * </p>
     *
     * @param process
     *            The process to log stdout and stderr from
     */
    private void listenToStdoutAndStderr(Process process) {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        executor.execute(new ProcessOutputLogger("stdout", process.getInputStream()));
        executor.execute(new ProcessOutputLogger("stderr", process.getErrorStream()));
        try {
            if (!process.waitFor(5, TimeUnit.MINUTES)) {
                process.destroy();
            }
        } catch (InterruptedException e) {
            log.warn("Git clone command interrupted", e);
        }
        executor.shutdown();
    }

    private static class ProcessOutputLogger implements Runnable {
        private final String type;
        private final InputStream stream;

        /**
         * Constructor
         *
         * @param type
         *            one of "stdout" or "stderr"
         * @param stream
         *            The input stream from the process
         */
        ProcessOutputLogger(String type, InputStream stream) {
            this.type = type + ": ";
            this.stream = stream;
        }

        /**
         * Just logs every line that was passed through the stream
         * 
         * @see java.lang.Runnable#run()
         */
        @Override
        public void run() {
            try (BufferedReader stdErr = new BufferedReader(new InputStreamReader(stream))) {
                for (String stdoutLine = stdErr.readLine(); stdoutLine != null; stdoutLine = stdErr.readLine()) {
                    log.info(type + stdoutLine);
                }
                log.debug(() -> type + "<EOF>");
            } catch (IOException e) {
                log.error("IOException in the runner that listens to " + type, e);
            }
        }
    }
}
