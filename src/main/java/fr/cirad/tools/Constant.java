/*******************************************************************************
 * metaXplorDB - Copyright (C) 2020 <CIRAD>
 * This program is free software: you can redistribute it and/or modify it under
 * the terms of the GNU Affero General Public License, version 3 as published by
 * the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License for more
 * details.
 *
 *
 * See <http://www.gnu.org/licenses/agpl.html> for details about GNU General
 * Public License V3.
 *******************************************************************************/
package fr.cirad.tools;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 *
 * @author petel, sempere
 */
public class Constant {
    public static final Map<String, Integer> SEQUENCE_TYPES = new LinkedHashMap<>();
    public static final String DATE_FORMAT_YYYYMMDD = "yyyy-MM-dd";
    public static final String DATE_FORMAT_HHMMSS = "hh:mm:ss";
	public static final String TAXO_TREE_CACHE_COLLNAME = "taxTreeCache";

    static {
        SEQUENCE_TYPES.put("Contig", 2);
        SEQUENCE_TYPES.put("Singleton", 3);
    }
}
