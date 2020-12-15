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

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Properties file support
 *
 * @author Splendid Data Product Development B.V.
 * @since 0.0.1
 */
public class MigrationSyncherProperties {
    private static final Logger log = LogManager.getLogger(MigrationSyncherProperties.class);

    /**
     * Property name for the host name or ip address of the database
     * <p>
     * The property in the provided properties file can be overridden by system property DB_HOST
     */
    private static final String DB_HOST = "DB_HOST";

    /**
     * Property name for the portnumber of the database
     * <p>
     * The property in the provided properties file can be overridden by the system property DB_PORT
     * <p>
     * Default value: 5432
     */
    private static final String DB_PORT = "DB_PORT";

    /**
     * Property name for the database name
     * <p>
     * The property in the provided properties file can be overridden by system property DB_NAME
     * <p>
     * If no property DB_NAME can be found, then an attempt is made to retrieve the database name from the PGDATABASE
     * environment variable
     */
    private static final String DB_NAME = "DB_NAME";

    /**
     * Property name for the user name to use for logging on to the database
     * <p>
     * The property in the provided properties file can be overridden by the system property DB_USER
     * <p>
     * If no property DB_USER is provided, then attempt is made to retrieve the user name from the PGUSER environment
     * variable
     */
    private static final String DB_USER = "DB_USER";

    /**
     * Property name for the password to use for logging on to the database
     * <p>
     * The property in the provided properties file can be overridden by the system property DB_PASSWORD
     * <p>
     * If no property DB_PASSWORD is provided, then attempt is made to read it from the .pgpass file. To find the
     * .pgpass file, the Postgres rules are applied: If environment variable PGPASSFILE exists, then the file appointed
     * by that variable is used. Otherwise the .pgpass file that exists in the user's home directory will be used.
     */
    private static final String DB_PASSWORD = "DB_PASSWORD";

    /**
     * Property name for the schema name in which the migration_syncher will log it's current state
     * <p>
     * The property in the provided properties file can be overridden by the system property DB_SYNCHER_SCHEMA
     * <p>
     * Default value: splendiddata_migration_syncher
     */
    private static final String DB_SYNCHER_SCHEMA = "DB_SYNCHER_SCHEMA";

    /**
     * Property name for the search_path that can be specified if the application needs that
     * <p>
     * The property in the provided properties file can be overridden by system property DB_SEARCH_PATH
     */
    private static final String DB_SEARCH_PATH = "DB_SEARCH_PATH";

    /**
     * Property name for the initial sql script if the application needs that. Typically used to set a role.
     * <p>
     * The property in the provided properties file can be overridden by system property DB_INITIAL_SQL
     */
    private static final String DB_INITIAL_SQL = "DB_INITIAL_SQL";

    private static final String GIT_LOCAL_REPOSITORY = "GIT_LOCAL_REPOSITORY";
    private static final String GIT_REMOTE_REPOSITORY_URL = "GIT_REMOTE_REPOSITORY_URL";
    private static final String GIT_USER = "GIT_USER";
    private static final String GIT_PASSWORD = "GIT_PASSWORD";
    private static final String GIT_CERTIFICATE = "GIT_CERTIFICATE";
    private static final String GIT_KEY = "GIT_KEY";
    private static final String GIT_BRANCH = "GIT_BRANCH";

    private static final String INCLUDE_DIRECTORIES = "INCLUDE_DIRECTORIES";

    private static final String NOT_APPLICABLE = "n.a.";

    private final String propertiesPath;

    private final String dbHost;
    private final int dbPort;
    private final String dbName;
    private final String dbUser;
    private final String dbPassword;
    private final String dbSyncherSchema;
    private final String dbSearchPath;
    private final String dbInitialSql;

    private final String gitLocalRepository;
    private final String gitRemoteRepositoryUrl;
    private final String gitUser;
    private final String gitPassword;
    private final String gitCertificate;
    private final String gitKey;
    private final String gitBranch;

    private final Set<Path> includeDirectories;

    /**
     * Constructor
     *
     * @param propertiesFile
     *            The file that contains all necessary properties
     * @throws IOException
     *             from the properties file
     */
    public MigrationSyncherProperties(Path propertiesFile) throws IOException {
        propertiesPath = propertiesFile.toAbsolutePath().toString();
        Properties properties = new Properties();
        try (InputStream in = new FileInputStream(propertiesFile.toFile())) {
            properties.load(in);
        } catch (FileNotFoundException e) {
            if (log.isTraceEnabled()) {
                log.error(e, e);
            } else {
                log.error(e);
            }
        }
        dbHost = System.getProperty(DB_HOST, properties.getProperty(DB_HOST, "<unknown host>")).trim();
        int portInt = 5432;
        try {
            portInt = Integer.parseInt(System.getProperty(DB_PORT, properties.getProperty(DB_PORT, "5432")).trim());
        } catch (NumberFormatException e) {
            log.error("DB_PORT property must be an integer in " + propertiesPath + ", 5432 assumed", e);
        }
        dbPort = portInt;
        dbName = System.getProperty(DB_NAME,
                properties.getProperty(DB_NAME, System.getenv().getOrDefault("PGDATABASE", "<unknown database>")))
                .trim();
        dbUser = System
                .getProperty(DB_USER,
                        properties.getProperty(DB_USER, System.getenv().getOrDefault("PGUSER", "<unknown user>")))
                .trim();
        String pwd = System.getProperty(DB_PASSWORD, properties.getProperty(DB_PASSWORD, "")).trim();
        if ("".equals(pwd)) {
            Path pgPassFile = Paths.get(System.getenv().getOrDefault("PGPASSFILE",
                    System.getProperty("user.home") + System.getProperty("file.separator") + ".pgpass"));
            if (Files.isRegularFile(pgPassFile)) {
                try (BufferedReader reader = new BufferedReader(
                        new InputStreamReader(Files.newInputStream(pgPassFile)))) {
                    for (String line = reader.readLine(); line != null; line = reader.readLine()) {
                        String[] fields = line.split("(?<!\\\\):");
                        if (fields.length >= 5
                                && (fields[0].equalsIgnoreCase(dbHost)
                                        || ("localhost".equalsIgnoreCase(fields[0]) && "127.0.0.1".equals(dbHost))
                                        || ("\\:\\:1".equals(fields[0]) && "localhost".equalsIgnoreCase(dbHost))
                                        || ("localhost".equalsIgnoreCase(fields[0]) && "::1".equals(dbHost))
                                        || ("127.0.0.1".equals(fields[0]) && "localhost".equalsIgnoreCase(dbHost)))
                                && fields[1].equals(Integer.toString(dbPort)) && fields[3].equalsIgnoreCase(dbUser)) {
                            pwd = fields[4];
                            break;
                        }
                    }
                }
            }
        }
        dbPassword = pwd;
        dbSyncherSchema = System.getProperty(DB_SYNCHER_SCHEMA,
                properties.getProperty(DB_SYNCHER_SCHEMA, "splendiddata_migration_syncher")).trim();
        dbSearchPath = System.getProperty(DB_SEARCH_PATH, properties.getProperty(DB_SEARCH_PATH, NOT_APPLICABLE))
                .trim();
        dbInitialSql = System.getProperty(DB_INITIAL_SQL, properties.getProperty(DB_INITIAL_SQL, "")).trim();

        gitLocalRepository = System
                .getProperty(GIT_LOCAL_REPOSITORY, properties.getProperty(GIT_LOCAL_REPOSITORY, "<unknown repo>"))
                .trim();
        gitRemoteRepositoryUrl = System.getProperty(GIT_REMOTE_REPOSITORY_URL,
                properties.getProperty(GIT_REMOTE_REPOSITORY_URL, NOT_APPLICABLE)).trim();
        gitUser = System.getProperty(GIT_USER, properties.getProperty(GIT_USER, NOT_APPLICABLE)).trim();
        gitPassword = System.getProperty(GIT_PASSWORD, properties.getProperty(GIT_PASSWORD, NOT_APPLICABLE)).trim();
        gitCertificate = System.getProperty(GIT_CERTIFICATE, properties.getProperty(GIT_CERTIFICATE, NOT_APPLICABLE))
                .trim();
        gitKey = System.getProperty(GIT_KEY, properties.getProperty(GIT_KEY, NOT_APPLICABLE)).trim();
        gitBranch = System.getProperty(GIT_BRANCH, properties.getProperty(GIT_BRANCH, "master")).trim();

        String inclDirsString = System.getProperty(INCLUDE_DIRECTORIES, properties.getProperty(INCLUDE_DIRECTORIES, ""))
                .trim();
        if (Util.isNotEmpty(inclDirsString) && !"*".equals(inclDirsString)) {
            Set<Path> dirs = new HashSet<Path>();
            for (String dir : inclDirsString.split(",")) {
                dirs.add(Paths.get(gitLocalRepository, dir.trim()));
            }
            includeDirectories = Collections.unmodifiableSet(dirs);
        } else {
            includeDirectories = Collections.emptySet();
        }
    }

    /**
     * @return String the dbHost
     */
    public String getDbHost() {
        return dbHost;
    }

    /**
     * @return int the dbPort
     */
    public int getDbPort() {
        return dbPort;
    }

    /**
     * @return String the dbName
     */
    public String getDbName() {
        return dbName;
    }

    /**
     * @return String the dbUser
     */
    public String getDbUser() {
        return dbUser;
    }

    /**
     * @return String the dbPassword
     */
    public String getDbPassword() {
        return dbPassword;
    }

    /**
     * @return String the dbSyncherSchema
     */
    public String getDbSyncherSchema() {
        return dbSyncherSchema;
    }

    /**
     * @return String the dbSearchPath or null if not filled in
     */
    public String getDbSearchPath() {
        if (NOT_APPLICABLE.equals(dbSearchPath) || Util.isEmpty(dbSearchPath)) {
            return null;
        }
        return dbSearchPath;
    }

    /**
     * @return String the dbInitialSql
     */
    public String getDbInitialSql() {
        return dbInitialSql;
    }

    /**
     * @return String the gitLocalRepository
     */
    public String getGitLocalRepository() {
        return gitLocalRepository;
    }

    /**
     * @return String the gitRemoteRepositoryUrl or null if not filled in
     */
    public String getGitRemoteRepositoryUrl() {
        if (NOT_APPLICABLE.equals(gitRemoteRepositoryUrl) || Util.isEmpty(gitRemoteRepositoryUrl)) {
            return null;
        }
        return gitRemoteRepositoryUrl;
    }

    /**
     * @return String the gitUser or null if not filled in
     */
    public String getGitUser() {
        if (NOT_APPLICABLE.equals(gitUser) || Util.isEmpty(gitUser)) {
            return null;
        }
        return gitUser;
    }

    /**
     * @return String the gitPassword or null if not filled in
     */
    public String getGitPassword() {
        if (NOT_APPLICABLE.equals(gitPassword) || Util.isEmpty(gitPassword)) {
            return null;
        }
        return gitPassword;
    }

    /**
     * @return String the gitCertificate or null if not filled in
     */
    public String getGitCertificate() {
        if (NOT_APPLICABLE.equals(gitCertificate) || Util.isEmpty(gitCertificate)) {
            return null;
        }
        return gitCertificate;
    }

    /**
     * @return String the gitKey or null if not filled in
     */
    public String getGitKey() {
        if (NOT_APPLICABLE.equals(gitKey) || Util.isEmpty(gitKey)) {
            return null;
        }
        return gitKey;
    }

    /**
     * @return String the gitBranch
     */
    public String getGitBranch() {
        return gitBranch;
    }

    /**
     * @return Set&lt;Path&gt; the includeDirectories
     */
    public Set<Path> getIncludeDirectories() {
        return includeDirectories;
    }

    /**
     * @see java.lang.Object#toString()
     *
     * @return String showing all properties in a formatted way
     */
    @Override
    public String toString() {
        Writer writer = new StringWriter();
        PrintWriter out = new PrintWriter(writer);
        out.println();
        out.print("# Properties file:          ");
        out.println(propertiesPath);
        out.println();
        out.print("DB_HOST                   = ");
        out.println(getDbHost());
        out.print("DB_PORT                   = ");
        out.println(getDbPort());
        out.print("DB_NAME                   = ");
        out.println(getDbName());
        out.print("DB_USER                   = ");
        out.println(getDbUser());
        out.print("DB_SYNCHER_SCHEMA         = ");
        out.println(getDbSyncherSchema());
        if (getDbSearchPath() != null) {
            out.print("DB_SEARCH_PATH            = ");
            out.println(getDbSearchPath());
        }
        if (!"".equals(getDbInitialSql())) {
            out.print("DB_INITIAL_SQL            = ");
            out.println(getDbInitialSql());
        }
        out.println();
        out.print("GIT_LOCAL_REPOSITORY      = ");
        out.println(getGitLocalRepository());
        out.print("GIT_REMOTE_REPOSITORY_URL = ");
        out.println(getGitRemoteRepositoryUrl() == null ? NOT_APPLICABLE : getGitRemoteRepositoryUrl());
        out.print("GIT_USER                  = ");
        out.println(getGitUser() == null ? NOT_APPLICABLE : getGitUser());
        out.print("GIT_CERTIFICATE           = ");
        out.println(getGitCertificate() == null ? NOT_APPLICABLE : getGitCertificate());
        out.print("GIT_KEY                   = ");
        out.println(getGitKey() == null ? NOT_APPLICABLE : getGitKey());
        out.print("GIT_BRANCH                = ");
        out.println(getGitBranch());
        out.println();
        out.print("INCLUDE_DIRECTORIES       = ");
        out.println(getIncludeDirectories().isEmpty() ? "*"
                : getIncludeDirectories().stream().sorted().map(Path::toString).collect(Collectors.joining(",")));
        out.close();
        return writer.toString();
    }

    /**
     * Generates an initial content for a properties file, with a bit of explanation about the properties
     *
     * @return String The content for an initial properties file
     */
    public String generateInitialContent() {
        Writer writer = new StringWriter();
        PrintWriter out = new PrintWriter(writer);
        out.println();
        out.print("# Properties file:          ");
        out.println(propertiesPath);
        out.println();
        out.println("# ##############################################################################");
        out.println("# #  Database connection properties                                            #");
        out.println("# # -------------------------------------------------------------------------- #");
        out.println("# #  The DB_SYNCHER_SCHEMA is used for administration of the synchronisation   #");
        out.println("# #  process. PLease use a distinct name for it and don't include this schema  #");
        out.println("# #  when the database is delivered to a customer. It will be created if it    #");
        out.println("# # -------------------------------------------------------------------------- #");
        out.println("# #  doesn't exist.                                                            #");
        out.println("# #  DB_HOST                    The host name or ip address of the database    #");
        out.println("# #  DB_PORT                    The port number at which the database listens  #");
        out.println("# #  DB_NAME                    Name of the database to connect to             #");
        out.println("# #  DB_USER                    User name to login to the database             #");
        out.println("# #  DB_PASSWORD                Password to login to the database              #");
        out.println("# #  DB_SYNCHER_SCHEMA          Schema that is to be used for the syncher      #");
        out.println("# #                             process. By default:                           #");
        out.println("# #                             splendiddata_migration_syncher                 #");
        out.println("# #  DB_SEARCH_PATH             The search path to use while synching. This    #");
        out.println("# #                             is a comma separated list of schemas.          #");
        out.println("# #  DB_INITIAL_SQL             Sql command to execute directly after logon.   #");
        out.println("# #                             Usuably this will be empty, but it may contain #");
        out.println("# #                             something like: set role = admin;              #");
        out.println("# ##############################################################################");
        out.print("DB_HOST                   = ");
        out.println(getDbHost());
        out.print("DB_PORT                   = ");
        out.println(getDbPort());
        out.print("DB_NAME                   = ");
        out.println(getDbName());
        out.print("DB_USER                   = ");
        out.println(getDbUser());
        out.print("DB_PASSWORD               = ");
        out.println("*******");
        out.print("DB_SYNCHER_SCHEMA         = ");
        out.println(getDbSyncherSchema());
        out.print("DB_SEARCH_PATH            = ");
        out.println(getDbSearchPath());
        out.print("DB_INITIAL_SQL            = ");
        out.print(getDbInitialSql());
        out.println();
        out.println("# ##############################################################################");
        out.println("# #  Git repository properties                                                 #");
        out.println("# # -------------------------------------------------------------------------- #");
        out.println("# #  GIT_LOCAL_REPOSITORY       Directory in the local machine for the git     #");
        out.println("# #                             repository. Will be created if necessary.      #");
        out.println("# #  GIT_REMOTE_REPOSITORY_URL  URL of the remote git repository with which    #");
        out.println("# #                             the database is to be kept in synch.           #");
        out.println("# #  GIT_USER                   Username to connect to the remote repository   #");
        out.println("# #  GIT_PASSWORD               Password for the remote repository.            #");
        out.println("# #                             Works only for HTTPS repositories. If the      #");
        out.println("# #                             remote repository uses SSH, please perform     #");
        out.println("# #                             SSH-COPY-ID in the OS and forget about the     #");
        out.println("# #                             password.                                      #");
        out.println("# # GIT_CERTIFICATE             The certificate file when using HTTPS. Will    #");
        out.println("# #                             be used as http.sslcert config entry in Git.   #");
        out.println("# # GIT_KEY                     The key file when using HTTPS. Will be used as #");
        out.println("# #                             http.sslkey config entry in Git.               #");
        out.println("# # GIT_BRANCH                  The branch to check out. Usually: master       #");
        out.println("# ##############################################################################");
        out.print("GIT_LOCAL_REPOSITORY      = ");
        out.println(getGitLocalRepository());
        out.print("GIT_REMOTE_REPOSITORY_URL = ");
        out.println(getGitRemoteRepositoryUrl() == null ? NOT_APPLICABLE : getGitRemoteRepositoryUrl());
        out.print("GIT_USER                  = ");
        out.println(getGitUser() == null ? NOT_APPLICABLE : getGitUser());
        out.print("GIT_PASSWORD              = ");
        out.println("*******");
        out.print("GIT_CERTIFICATE           = ");
        out.println(getGitCertificate() == null ? NOT_APPLICABLE : getGitCertificate());
        out.print("GIT_KEY                   = ");
        out.println(getGitKey() == null ? NOT_APPLICABLE : getGitKey());
        out.print("GIT_BRANCH                = ");
        out.println(getGitBranch());
        out.println();
        out.println("# ##############################################################################");
        out.println("# #  Comma separated list of directories in the git repository that contain    #");
        out.println("# #  files that are to be applied to the database. If empty or star (\"*\")      #");
        out.println("# #  then all changed files in the repository are applied to the database.     #");
        out.println("# ##############################################################################");
        out.print("INCLUDE_DIRECTORIES       = ");
        out.println(getIncludeDirectories().isEmpty() ? "*"
                : getIncludeDirectories().stream().sorted().map(Path::toString).collect(Collectors.joining(",")));
        out.close();
        return writer.toString();
    }
}
