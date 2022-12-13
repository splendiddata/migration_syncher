/*
 * Copyright (c) Splendid Data Product Development B.V. 2022
 *
 * This unpublished material is proprietary to Splendid Data Product Development B.V. All rights reserved. The methods
 * and techniques described herein are considered trade secrets and/or confidential. Reproduction or distribution, in
 * whole or in part, is forbidden except by express written permission of Splendid Data Product Development B.V.
 */

package com.splendiddata.intermal.migrationsyncher;

import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Just provides the pathname to the properties file for testing purposes. The default is
 * "src/test/resources/test.properties", but it may be overridden by system property "PROPERTIES_PATH".
 *
 * @author Splendid Data Product Development B.V.
 * @since 2.1
 */
public class PropertiesPath {

    /**
     * The absolute pathname to the properties path
     */
    public static final Path PROPERTIES_PATH = Paths
            .get(System.getProperty("PROPERTIES_PATH", "src/test/resources/test.properties"));
}
