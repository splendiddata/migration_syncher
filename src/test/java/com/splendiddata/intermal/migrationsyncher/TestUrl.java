/*
 * Copyright (c) Splendid Data Product Development B.V. 2023
 *
 * This unpublished material is proprietary to Splendid Data Product Development B.V. All rights reserved. The methods
 * and techniques described herein are considered trade secrets and/or confidential. Reproduction or distribution, in
 * whole or in part, is forbidden except by express written permission of Splendid Data Product Development B.V.
 */

package com.splendiddata.intermal.migrationsyncher;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * TODO Please insert some explanation here
 *
 * @author Splendid Data Product Development B.V.
 * @since 2.1
 */
public class TestUrl {
    @Test
    public void testUri1() throws URISyntaxException {
        String testStr = "git://bbucket.org:bbdev/myrepo.git";
        URI uri = new URI(testStr);
        Assertions.assertEquals(testStr, uri.toString(), "uri.toString()");
        Assertions.assertEquals("git", uri.getScheme(), "uri.getScheme()");
        Assertions.assertEquals(-1, uri.getPort(), "uri.getPort()");
        Assertions.assertTrue(uri.isAbsolute(), "uri.isAbsolute()");
        Assertions.assertEquals("//bbucket.org:bbdev/myrepo.git", uri.getSchemeSpecificPart(),
                "uri.getSchemeSpecificPart()");
        Assertions.assertEquals("bbucket.org:bbdev", uri.getAuthority(), "uri.getAuthority()");
        Assertions.assertEquals("/myrepo.git", uri.getPath(), "uri.getPath()");
        Assertions.assertNull(uri.getHost(), "uri.getHost()");
        Assertions.assertNull(uri.getUserInfo(), "uri.getUserInfo()");
    }

    @Test
    public void testUri2() throws URISyntaxException {
        String testStr = "https://some.server.com/git/a_repo";
        URI uri = new URI(testStr);
        Assertions.assertEquals(testStr, uri.toString(), "uri.toString()");
        Assertions.assertEquals("https", uri.getScheme(), "uri.getScheme()");
        Assertions.assertEquals(-1, uri.getPort(), "uri.getPort()");
        Assertions.assertTrue(uri.isAbsolute(), "uri.isAbsolute()");
        Assertions.assertEquals("//some.server.com/git/a_repo", uri.getSchemeSpecificPart(),
                "uri.getSchemeSpecificPart()");
        Assertions.assertEquals("some.server.com", uri.getAuthority(), "uri.getAuthority()");
        Assertions.assertEquals("/git/a_repo", uri.getPath(), "uri.getPath()");
        Assertions.assertEquals("some.server.com", uri.getHost(), "uri.getHost()");
    }

    @Test
    public void testUri3() throws URISyntaxException {
        String testStr = "japie@some.server.com:whatever/git/a_repo";
        StringBuilder refactoredStr = new StringBuilder();
        URI uri;
        try {
            uri = new URI(testStr);
        } catch (URISyntaxException e) {
            Matcher m = Pattern.compile("^((\\w+\\@)?[^:/\\s]+):([^/\\s]+)/(.*)$").matcher(testStr);
            if (m.matches()) {
                refactoredStr.append("ssh://").append(m.group(1)).append('/').append(m.group(3)).append('/').append(m.group(4));
                uri = new URI(refactoredStr.toString());
            } else {
                Assertions.fail(testStr + " does not comply:" + e);
                uri = null;
            }
        }
        Assertions.assertEquals(refactoredStr.toString(), uri.toString(), "uri.toString()");
        Assertions.assertEquals("ssh", uri.getScheme(), "uri.getScheme()");
        Assertions.assertEquals(-1, uri.getPort(), "uri.getPort()");
        Assertions.assertTrue(uri.isAbsolute(), "uri.isAbsolute()");
        Assertions.assertEquals("//japie@some.server.com/whatever/git/a_repo", uri.getSchemeSpecificPart(),
                "uri.getSchemeSpecificPart()");
        Assertions.assertEquals("japie@some.server.com", uri.getAuthority(), "uri.getAuthority()");
        Assertions.assertEquals("japie", uri.getUserInfo(), "uri.getUserInfo()");
        Assertions.assertEquals("/whatever/git/a_repo", uri.getPath(), "uri.getPath()");
        Assertions.assertEquals("some.server.com", uri.getHost(), "uri.getHost()");
    }
}
