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
package fr.cirad.tools.mongo;

/**
 * 
 * @author petel, sempere
 */
public class DBConstant {

    public static final String FIELDNAME_MIN = "min";
    public static final String FIELDNAME_MAX = "max";
    public static final int UPPER_BOUND = -1;
    public static final int LOWER_BOUND = 1;

    // fields fetched from NCBI 
    public static final String FIELDNAME_HIT_DEFINITION = "hit_definition";
    public static final String FIELDNAME_TAXON = "taxonomy_id";
    public static final String FIELDNAME_SEQ_LENGTH = "sequence length";

    // fields common to sequence, sample and assigment
    public static final String FIELDNAME_PROJECT = "pj";

    // possible field types
    public static final String STRING_TYPE = "S";
    public static final String STRING_ARRAY_TYPE = "SA";
    public static final String DOUBLE_TYPE = "D";
    public static final String DATE_TYPE = "T";
    public static final String GPS_TYPE = "G";

    // prefix for cached collections
    public static final String CACHE_PREFIX = "cache_";
}
