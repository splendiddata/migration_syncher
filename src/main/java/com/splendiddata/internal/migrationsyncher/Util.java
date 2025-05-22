/*
 * Copyright (c) Splendid Data Product Development B.V. 2020 - 2025
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

/**
 * Some utility methods
 *
 * @author Splendid Data Product Development B.V.
 * @since 1.0
 */
public final class Util {

    /**
     * Utility class - no instances
     */
    private Util() {
        throw new UnsupportedOperationException("Utility class - no instances");
    }


    /**
     * Returns true if str is null, empty, or contains only whitespace characters
     *
     * @param str
     *            the String to be checked, may be null
     * @return boolean true if null or empty
     */
    public static boolean isEmpty(String str) {
        return str == null || str.isBlank();
    }

    /**
     * Checks if str is not null and contains at lease one non-whitespace character
     *
     * @param str
     *            The String to be checked, may be null
     * @return boolean false if null or empty
     */
    public static boolean isNotEmpty(String str) {
        return !isEmpty(str);
    }
}
