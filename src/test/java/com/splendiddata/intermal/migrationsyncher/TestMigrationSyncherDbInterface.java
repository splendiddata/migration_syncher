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

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.eclipse.jgit.lib.ObjectId;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.postgresql.Driver;
import org.postgresql.PGProperty;

import com.splendiddata.internal.migrationsyncher.MigrationSyncherDbInterface;
import com.splendiddata.internal.migrationsyncher.MigrationSyncherProperties;

/**
 * Tests the MigrationSyncherDbInterface
 *
 * @author Splendid Data Product Development B.V.
 * @since 0.0.1
 */
public class TestMigrationSyncherDbInterface {

    static MigrationSyncherProperties properties;

    /**
     * Drops the test database if it exists and creates it.
     *
     * @throws IOException
     *             from the properties
     * @throws ClassNotFoundException
     *             if the Postgres driver is missing
     */
    @BeforeAll
    static void init() throws IOException, ClassNotFoundException {
        properties = new MigrationSyncherProperties(PropertiesPath.PROPERTIES_PATH);

        Class.forName(Driver.class.getName());
        Properties connectionProperties = new Properties();
        PGProperty.APPLICATION_NAME.set(connectionProperties, "migration_syncher");
        PGProperty.PG_HOST.set(connectionProperties, properties.getDbHost());
        PGProperty.PG_PORT.set(connectionProperties, properties.getDbPort());
        PGProperty.PG_DBNAME.set(connectionProperties, "postgres"); // Use the master database so the test database can be dropped and created
        PGProperty.USER.set(connectionProperties, properties.getDbUser());
        if (properties.getDbPassword() != null) {
            PGProperty.PASSWORD.set(connectionProperties, properties.getDbPassword());
        }

        String url = new StringBuilder().append("jdbc:postgresql://").append(properties.getDbHost()).append(':')
                .append(properties.getDbPort()).append('/').append("postgres").toString();
        String sql = new StringBuilder().append("DriverManager.getConnection(url=").append(url)
                .append(", connectionProperties=").append(connectionProperties).append(")").toString();
        try (Connection conn = DriverManager.getConnection(url, connectionProperties);
                Statement st = conn.createStatement()) {
            sql = "drop database if exists " + properties.getDbName();
            st.execute(sql);
            sql = "create database " + properties.getDbName();
            st.execute(sql);
        } catch (SQLException e) {
            Assertions.fail(sql, e);
        }
    }

    @AfterAll
    static void cleanup() throws IOException, ClassNotFoundException {

        properties = new MigrationSyncherProperties(PropertiesPath.PROPERTIES_PATH);

        Class.forName(Driver.class.getName());
        Properties connectionProperties = new Properties();
        PGProperty.APPLICATION_NAME.set(connectionProperties, "migration_syncher");
        PGProperty.PG_HOST.set(connectionProperties, properties.getDbHost());
        PGProperty.PG_PORT.set(connectionProperties, properties.getDbPort());
        PGProperty.PG_DBNAME.set(connectionProperties, "postgres"); // Use the master database so the test database can be dropped and created
        PGProperty.USER.set(connectionProperties, properties.getDbUser());
        if (properties.getDbPassword() != null) {
            PGProperty.PASSWORD.set(connectionProperties, properties.getDbPassword());
        }

        String url = new StringBuilder().append("jdbc:postgresql://").append(properties.getDbHost()).append(':')
                .append(properties.getDbPort()).append('/').append("postgres").toString();
        String sql = new StringBuilder().append("DriverManager.getConnection(url=").append(url)
                .append(", connectionProperties=").append(connectionProperties).append(")").toString();
        try (Connection conn = DriverManager.getConnection(url, connectionProperties);
                Statement st = conn.createStatement()) {
            sql = "drop database if exists " + properties.getDbName();
            st.execute(sql);
        } catch (SQLException e) {
            Assertions.fail(sql, e);
        }
    }

    /**
     * First creation of a MigrationSyncherDbInterface
     * <p>
     * This should create the syncher database schema as specified in the properties
     *
     * @throws ClassNotFoundException
     *             if the Postgres driver is missing
     * @throws SQLException
     *             from the database
     */
    @Test
    void connect() throws ClassNotFoundException, SQLException {
        try (MigrationSyncherDbInterface db = new MigrationSyncherDbInterface(properties)) {
            ObjectId lastCommit = db.getLastCommitId();
            Assertions.assertNull(lastCommit, "db.getLastCommittedObjectId() on an empty database");
        }
    }
}
