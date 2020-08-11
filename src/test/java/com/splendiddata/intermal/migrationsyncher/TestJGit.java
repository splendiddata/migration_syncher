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

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;

import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.diff.DiffEntry;
import org.eclipse.jgit.diff.DiffEntry.ChangeType;
import org.eclipse.jgit.errors.IncorrectObjectTypeException;
import org.eclipse.jgit.errors.MissingObjectException;
import org.eclipse.jgit.errors.RepositoryNotFoundException;
import org.eclipse.jgit.lib.Constants;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.ObjectReader;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.revwalk.RevTree;
import org.eclipse.jgit.revwalk.RevWalk;
import org.eclipse.jgit.treewalk.CanonicalTreeParser;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.platform.commons.logging.Logger;
import org.junit.platform.commons.logging.LoggerFactory;

/**
 * Tests JGit to get some examples to build on
 *
 * @author Splendid Data Product Development B.V.
 * @since 0.0.1
 */
public class TestJGit {
    Logger log = LoggerFactory.getLogger(TestJGit.class);

    static Path repodir;
    static int filenr;
    static ObjectId lastCommitId;

    /**
     * Initialises a new local git repository under target/testrepo.
     *
     * @throws IOException
     * @throws GitAPIException
     */
    @BeforeAll
    static void initARepository() throws IOException, GitAPIException {
        repodir = Paths.get("target", "testrepo");
        if (repodir.toFile().exists()) {
            Files.walk(repodir).map(Path::toFile).sorted((o1, o2) -> -o1.compareTo(o2)).forEach(File::delete);
        }
        Files.createDirectories(repodir);
        Git git = Git.init().setDirectory(repodir.toFile()).setBare(false).call();

        createAndCommitFiles(git, 1);
    }

    @Test
    public void notARepo() throws IOException {
        Path notARepo = Paths.get("target", "notARepo");
        if (notARepo.toFile().exists()) {
            Files.walk(notARepo).map(Path::toFile).sorted((o1, o2) -> -o1.compareTo(o2)).forEach(File::delete);
        }
        Files.createDirectories(notARepo);
        try {
            Git.open(notARepo.toFile());
            Assertions.fail("Git.open(notARepo.toFile()) expected an exception on an empty directory");
        } catch (RepositoryNotFoundException e) {
            log.info(() -> "caught exception on empty repository directory: " + e);
        }
    }

    @Test
    public void repoIsFile() throws IOException {
        Path filePath = Paths.get("target", "justAFile");
        if (!filePath.toFile().exists()) {
            try (Writer writer = new FileWriter(filePath.toFile())) {
                writer.append("This is just a file");
            }
        }
        try {
            Git.open(filePath.toFile());
            Assertions.fail(
                    "Git.open(notARepo.toFile()) expected an exception on a file that is supposed to be a Git repository");
        } catch (RepositoryNotFoundException e) {
            log.info(() -> "caught exception on git repository appears to be a file: " + e);
        }
    }

    /**
     * Just an attempt to get the hash of the first commit. This is the start of the tests
     *
     * @throws IOException
     *             From the Git instance
     */
    @Test
    public void obtainHeadId() throws IOException {
        Git git = Git.open(repodir.toFile());
        lastCommitId = git.getRepository().resolve(Constants.HEAD);
        Assertions.assertNotNull(lastCommitId, "git.getRepository().resolve(Constants.HEAD)");
        log.info(() -> "lastCommitId = " + lastCommitId);

        try (RevWalk revWalk = new RevWalk(git.getRepository())) {
            RevCommit commit = revWalk.parseCommit(lastCommitId);
            RevTree tree = commit.getTree();
            Assertions.assertNotNull(tree, "commit.getTree()");
            Assertions.assertEquals(git.getRepository().resolve("HEAD^{tree}"), tree.getId(), "tree id");
        }
    }

    @Nested
    class NestedTest {
        @Test
        public void someFilesAddedForASecondCommit() throws IncorrectObjectTypeException, IOException, GitAPIException {
            Git git = Git.open(repodir.toFile());
            Repository repository = git.getRepository();

            createAndCommitFiles(git, 2);

            ObjectId currentCommitId = repository.resolve(Constants.HEAD);
            log.info(() -> "currentCommitId = " + currentCommitId);

            ObjectReader reader = repository.newObjectReader();
            CanonicalTreeParser oldTreeIter = new CanonicalTreeParser();
            oldTreeIter.reset(reader, getTreeId(repository, lastCommitId));
            CanonicalTreeParser newTreeIter = new CanonicalTreeParser();
            newTreeIter.reset(reader, getTreeId(repository, currentCommitId));
            List<DiffEntry> diffs = git.diff().setShowNameAndStatusOnly(true).setNewTree(newTreeIter)
                    .setOldTree(oldTreeIter).call();
            Assertions.assertEquals(2, diffs.size(), "diffs.size()");

            DiffEntry diffEntry = diffs.get(0);
            Assertions.assertEquals("file_2", diffEntry.getNewPath(), "diffEntry.getNewPath() on file_2");
            Assertions.assertEquals(ChangeType.ADD, diffEntry.getChangeType(), "diffEntry.getChangeType() on file_2");

            diffEntry = diffs.get(1);
            Assertions.assertEquals("file_3", diffEntry.getNewPath(), "diffEntry.getNewPath() on file_3");
            Assertions.assertEquals(ChangeType.ADD, diffEntry.getChangeType(), "diffEntry.getChangeType() on file_3");

            lastCommitId = currentCommitId;
        }

        @Nested
        class NestedNestedTest {
            @Test
            public void someMoreFileMovements() throws IncorrectObjectTypeException, IOException, GitAPIException {
                Git git = Git.open(repodir.toFile());
                Repository repository = git.getRepository();

                try (PrintWriter writer = new PrintWriter(
                        new FileWriter(Paths.get(repodir.toString(), "file_3").toFile(), true))) {
                    writer.println("This file is altered");
                }
                git.add().addFilepattern("file_3").call();
                Files.delete(Paths.get(repodir.toString(), "file_1"));
                git.rm().addFilepattern("file_1").call();
                git.commit().setMessage("committing altered and deleted file").call();

                ObjectId currentCommitId = repository.resolve(Constants.HEAD);
                log.info(() -> "currentCommitId = " + currentCommitId);

                ObjectReader reader = repository.newObjectReader();
                CanonicalTreeParser oldTreeIter = new CanonicalTreeParser();
                oldTreeIter.reset(reader, getTreeId(repository, lastCommitId));
                CanonicalTreeParser newTreeIter = new CanonicalTreeParser();
                newTreeIter.reset(reader, getTreeId(repository, currentCommitId));
                List<DiffEntry> diffs = git.diff().setShowNameAndStatusOnly(true).setNewTree(newTreeIter)
                        .setOldTree(oldTreeIter).call();
                Assertions.assertEquals(2, diffs.size(), "diffs.size()");

                DiffEntry diffEntry = diffs.get(0);
                Assertions.assertEquals(ChangeType.DELETE, diffEntry.getChangeType(),
                        "diffEntry.getChangeType() on file_1");
                Assertions.assertEquals("file_1", diffEntry.getOldPath(), "diffEntry.getOldPath() on file_1");

                diffEntry = diffs.get(1);
                Assertions.assertEquals(ChangeType.MODIFY, diffEntry.getChangeType(),
                        "diffEntry.getChangeType() on file_3");
                Assertions.assertEquals("file_3", diffEntry.getOldPath(), "diffEntry.getOldPath() on file_3");
            }
        }
    }

    /**
     * Add one or more files to the repository and commit
     *
     * @param git
     *            The git instance
     * @param nr
     *            The number of files to create
     * @throws IOException
     *             From file manipulations
     * @throws GitAPIException
     *             From the git instance
     */
    static void createAndCommitFiles(Git git, int nr) throws IOException, GitAPIException {
        for (int i = 0; i < nr; i++) {
            String fileName = "file_" + ++filenr;
            Path file = Paths.get(repodir.toString(), fileName);
            try (PrintWriter out = new PrintWriter(Files.newBufferedWriter(file, StandardOpenOption.CREATE_NEW))) {
                out.println("This is file " + file.toAbsolutePath().toString());
            }
            git.add().addFilepattern(fileName).call();
        }

        git.commit().setMessage("committing " + nr + (nr == 1 ? " file" : " files")).call();
    }

    static ObjectId getTreeId(Repository repository, ObjectId commitId)
            throws MissingObjectException, IncorrectObjectTypeException, IOException {
        try (RevWalk revWalk = new RevWalk(repository)) {
            return revWalk.parseCommit(commitId).getTree().getId();
        }
    }
}
