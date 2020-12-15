/*
 * Copyright (c) Splendid Data Product Development B.V. 2020
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

package com.splendiddata.internal.migrationsyncher;

import java.io.Closeable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.jgit.lib.ObjectId;
import org.postgresql.Driver;
import org.postgresql.PGProperty;

/**
 * Database interface for the migration syncher
 *
 * @author Splendid Data Product Development B.V.
 * @since 0.0.1
 */
public class MigrationSyncherDbInterface implements Closeable {
    private static final Logger log = LogManager.getLogger(MigrationSyncherDbInterface.class);
    private final Connection conn;
    private final String schema;
    private final String searchPath;

    /**
     * Constructor
     * <p>
     * Connects to the database
     * </p>
     *
     * @param properties
     *            From the properties file that was read at MigrationSyncherMain start
     * @throws ClassNotFoundException
     *             When the Postgres JDBC jar isn't present
     * @throws SQLException
     *             If the communication with the database fails
     */
    public MigrationSyncherDbInterface(MigrationSyncherProperties properties)
            throws ClassNotFoundException, SQLException {
        log.trace(() -> new StringBuilder().append("@>MigrationSyncherDbInterface(properties=").append(properties)
                .append(')'));
        Class.forName(Driver.class.getName());
        Properties connectionProperties = new Properties();
        PGProperty.APPLICATION_NAME.set(connectionProperties, "migration_syncher");
        PGProperty.PG_HOST.set(connectionProperties, properties.getDbHost());
        PGProperty.PG_PORT.set(connectionProperties, properties.getDbPort());
        PGProperty.PG_DBNAME.set(connectionProperties, properties.getDbName());
        PGProperty.USER.set(connectionProperties, properties.getDbUser());
        if (properties.getDbPassword() != null) {
            PGProperty.PASSWORD.set(connectionProperties, properties.getDbPassword());
        }

        String url = new StringBuilder().append("jdbc:postgresql://").append(properties.getDbHost()).append(':')
                .append(properties.getDbPort()).append('/').append(properties.getDbName().toLowerCase()).toString();
        conn = DriverManager.getConnection(url, connectionProperties);

        schema = properties.getDbSyncherSchema();
        String sql = "select nspname from pg_catalog.pg_namespace where nspname = ?";
        boolean syncherSchemaPresent = true;
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, properties.getDbSyncherSchema());
            try (ResultSet rs = ps.executeQuery()) {
                syncherSchemaPresent = rs.next();
            }
        }
        if (!syncherSchemaPresent) {
            createSyncherSchema();
        }
        if (properties.getDbSearchPath() == null || "null".equalsIgnoreCase(properties.getDbSearchPath())|| properties.getDbSearchPath().matches("^\\s*$")) {
            searchPath = null;
        } else {
            searchPath = "set search_path = " + properties.getDbSearchPath();
        }
        if (!"".equals(properties.getDbInitialSql())) {
            try (Statement st = conn.createStatement()) {
                log.trace(() -> "execute: " + properties.getDbInitialSql());
                st.execute(properties.getDbInitialSql());
            }
        }

        log.trace(() -> new StringBuilder().append("@<MigrationSyncherDbInterface(properties=").append(properties)
                .append(')'));
    }

    /**
     * Closes the database connection.
     * 
     * @see java.io.Closeable#close()
     */
    @Override
    public void close() {
        log.trace("@>close()");
        try {
            conn.close();
        } catch (SQLException e) {
            log.error("close", e);
        }
        log.trace("@<close()");
    }

    /**
     * Executes the fileContent in the database and returns the SQL exception if any. So a null return is what we hope
     * for.
     *
     * @param fileContent
     *            The content of some file that needs to be executed in the database
     * @return SQLException if anything went wrong, but hopefully null
     */
    public SQLException executeFileContent(String fileContent) {
        log.trace(
                () -> new StringBuilder().append("@>executeFileContent(fileContent=").append(fileContent).append(')'));
        String sql = fileContent;
        try (Statement st = conn.createStatement()) {
            if (searchPath != null) {
                sql = searchPath;
                if (log.isTraceEnabled()) {
                    log.trace(new StringBuilder("st.execute(searchPath=\"").append(sql).append("\")"));
                }
                st.execute(sql);
                sql = fileContent;
            }
            st.execute(fileContent);
        } catch (SQLException e) {
            if (log.isDebugEnabled()) {
                log.debug(new StringBuilder().append("@<executeFileContent(sql=\"").append(sql).append("\") = ")
                        .append(e));
            }

            /*
             * Rollback is performed here as a separate statement to coop with situations where the fileContent contains
             * a "begin" statement and fails. In that case an explicit "rollback" is to be given but the
             * Connection.rollback() method will not work because the connection thinks it is in autocommit mode.
             */
            try (Statement st = conn.createStatement()) {
                st.execute("rollback");
            } catch (SQLException er) {
                log.error("executeFileContent() -> failed to rollback", er);
            }

            return e;
        }
        log.trace(() -> new StringBuilder().append("@<executeFileContent(fileContent=").append(fileContent)
                .append(") = null"));
        return null;
    }

    /**
     * Returns the id of the last commit that was processed in this database
     *
     * @return ObjectId the id of the last commit or null if there isn't any yet
     * @throws SQLException
     *             when applicable
     */
    public ObjectId getLastCommitId() throws SQLException {
        log.trace("@>getLastCommitId()");
        String sql = "select commit_id from " + schema + ".last_commit";
        String commitIdString = null;
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            if (rs.next()) {
                commitIdString = rs.getString(1);
            }
        }
        if (commitIdString == null) {
            log.debug("@<getLastCommitId() = null");
            return null;
        }
        log.debug("@<getLastCommitId() = " + commitIdString);
        return ObjectId.fromString(commitIdString);
    }

    /**
     * Updates the id of the last git commit that is processed in this database
     *
     * @param head
     *            The git commit id to store
     * @throws SQLException
     *             if the database feels an urge to do so
     */
    public void setLastCommitId(ObjectId head) throws SQLException {
        log.trace(() -> new StringBuilder().append("@>setLastCommitId(head=").append(head).append(')'));
        String sql = "update " + schema + ".last_commit set commit_id = ?";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, head.getName());
            ps.executeUpdate();
        }
        log.debug(() -> new StringBuilder().append("@<setLastCommitId(head=").append(head).append(')'));
    }

    /**
     * inserts or updates a file in the failed_file table
     *
     * @param filePath
     *            The identification of the file
     * @param sqlState
     *            The database error code
     * @param errorMessage
     *            The error message from the database
     * @throws SQLException
     *             When the database feels a need for that
     */
    public void setErroneousFile(String filePath, String sqlState, String errorMessage) throws SQLException {
        log.trace(() -> new StringBuilder().append("@>setErroneousFile(filePath=").append(filePath)
                .append(", sqlState=").append(sqlState).append(", errorMessage=").append(errorMessage).append(')'));
        String sql = "insert into " + schema + ".failed_file (status, file, error_message) values (?, ?, ?) "
                + "on conflict (file) do update set status = excluded.status, error_message = excluded.error_message";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, sqlState);
            ps.setString(2, filePath);
            ps.setString(3, errorMessage);
            ps.executeUpdate();
        }
        log.trace(() -> new StringBuilder().append("@<setErroneousFile(filePath=").append(filePath)
                .append(", sqlState=").append(sqlState).append(", errorMessage=").append(errorMessage).append(')'));
    }

    /**
     * Returns the pathnames of all files registered in the failed_file table
     *
     * @return List&lt;String&gt; with pathnames
     * @throws SQLException
     *             When the database feels a need for that
     */
    public List<String> getErroneousFiles() throws SQLException {
        log.trace("@>getErroneousFiles()");
        List<String> result = new ArrayList<>();
        String sql = "select file from " + schema + ".failed_file";

        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            while (rs.next()) {
                result.add(rs.getString(1));
            }
        }
        if (log.isTraceEnabled()) {
            log.trace("@<getErroneousFiles() = " + result);
        } else {
            log.debug(() -> new StringBuilder().append("@<getErroneousFiles() = ").append(result.size())
                    .append(result.size() == 1 ? " file" : " files"));
        }
        return result;
    }

    /**
     * Removes the specified filePath from the failed_file table
     *
     * @param filePath
     *            The identification of the file to remove
     * @throws SQLException
     *             When the database feels a need for that
     */
    public void removeErroneousFile(String filePath) throws SQLException {
        log.trace(() -> new StringBuilder().append("@>removeErroneousFile(filePath=").append(filePath).append(')'));
        String sql = "delete from " + schema + ".failed_file where file = ?";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, filePath);
            ps.executeUpdate();
        }
        log.debug(() -> new StringBuilder().append("@<removeErroneousFile(filePath=").append(filePath).append(')'));
    }

    /**
     * inserts or updates a file in the failed_file table
     *
     * @param filePath
     *            The identification of the file
     * @param sqlState
     *            The database error code
     * @param errorMessage
     *            The error message from the database
     * @throws SQLException
     *             When the database feels a need for that
     */
    public void logFileExecution(String filePath, String sqlState, String errorMessage) throws SQLException {
        log.trace(() -> new StringBuilder().append("@>logFileExecution(filePath=").append(filePath)
                .append(", sqlState=").append(sqlState).append(", errorMessage=").append(errorMessage).append(')'));
        String sql = "insert into " + schema + ".file_execution_log (status, file, error_message) values (?, ?, ?)";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            if (Util.isEmpty(sqlState)) {
                ps.setString(1, "ok");
            } else {
                ps.setString(1, sqlState);
            }
            ps.setString(2, filePath);
            if (Util.isEmpty(errorMessage)) {
                ps.setNull(3, Types.VARCHAR);
            } else {
                ps.setString(3, errorMessage);
            }
            ps.executeUpdate();
        }
        log.trace(() -> new StringBuilder().append("@<logFileExecution(filePath=").append(filePath)
                .append(", sqlState=").append(sqlState).append(", errorMessage=").append(errorMessage).append(')'));
    }

    /**
     * Creates the schema that is needed for synchronisation
     *
     * @throws SQLException
     *             if something went wrong
     */
    private void createSyncherSchema() throws SQLException {
        log.trace("@>createSyncherSchema()");
        conn.setAutoCommit(false);
        String sql = "begin";
        try (Statement st = conn.createStatement()) {
            st.execute(sql);

            sql = "create schema " + schema;
            st.execute(sql);

            sql = "create function " + schema + ".tr_dont() returns trigger language plpgsql as $$"
                    + "begin return null; end $$";
            st.execute(sql);
            sql = "create function " + schema + ".tr_last_updated() returns trigger language plpgsql as $$"
                    + "begin new.last_updated = current_timestamp; return new; end $$";
            st.execute(sql);

            //@formatter:off
            sql = "create table " + schema + ".last_commit "
                    + "( pk boolean primary key check(pk = true)"
                    + ", last_updated timestamp not null"
                    + ", commit_id varchar(40)"
                    + ")";
            //@formatter:on
            st.execute(sql);
            sql = "create trigger tr_bd_last_commit before delete on " + schema + ". last_commit for each row "
                    + "execute procedure " + schema + ".tr_dont()";
            sql = "create trigger tr_bu_last_commit before update on " + schema + ". last_commit for each row "
                    + "execute procedure " + schema + ".tr_last_updated()";
            st.execute(sql);

            sql = "insert into " + schema + ".last_commit (pk, last_updated) values (true, current_timestamp)";
            st.execute(sql);

            //@formatter:off
            sql = "create table " + schema + ".file_execution_log "
                    + "( pk serial primary key"
                    + ", created timestamp not null default current_timestamp"
                    + ", status varchar(5) not null"
                    + ", file text not null"
                    + ", error_message text"
                    + ")";
            //@formatter:on
            st.execute(sql);
            sql = "create trigger tr_bud_file_execution_log before update or delete on " + schema
                    + ".file_execution_log for each row execute procedure " + schema + ".tr_dont()";
            st.execute(sql);

            //@formatter:off
            sql = "create table " + schema + ".failed_file "
                    + "( pk serial primary key"
                    + ", created timestamp not null default current_timestamp"
                    + ", last_updated timestamp not null default current_timestamp"
                    + ", status varchar(5) not null"
                    + ", file text not null unique"
                    + ", error_message text"
                    + ")";
            //@formatter:on
            st.execute(sql);
            sql = "create trigger tr_bu_failed_file before update on " + schema + ". last_commit for each row "
                    + "execute procedure " + schema + ".tr_last_updated()";
            st.execute(sql);

            conn.commit();
        } catch (SQLException e) {
            log.error(sql, e);
            conn.rollback();
            throw e;
        }

        conn.setAutoCommit(true);
        log.trace("@<createSyncherSchema()");
    }

}
