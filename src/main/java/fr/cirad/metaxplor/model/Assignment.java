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
package fr.cirad.metaxplor.model;

import java.util.HashMap;
import java.util.Map;

import org.springframework.data.annotation.TypeAlias;
import org.springframework.data.mongodb.core.mapping.Field;

import fr.cirad.tools.mongo.DBConstant;

/**
 *
 * @author petel, sempere
 */
@TypeAlias(AssignedSequence.FIELDNAME_ASSIGNMENT)
public class Assignment {

    /**
     * list of accessions for this assignment. It's an array of String as a
     * sequence can have multiple 'Best hits'
     */
    public static final String FIELDNAME_SSEQID = "sseqid";
    /**
     * method use for taxonomic assignment 
     */
    public static final String FIELDNAME_ASSIGN_METHOD = "assignment_method";
    /**
     * method use for likelihood
     */
    public static final String FIELDNAME_LIKELIHOOD = "likelihood";
    
    @Field(DBConstant.STRING_TYPE)
    private Map<Integer, String> stringFields;

    @Field(DBConstant.DOUBLE_TYPE)
    private Map<Integer, Double> doubleFields;
    
    @Field(DBConstant.STRING_ARRAY_TYPE)
    private Map<Integer, String[]> stringArrayFields;

    public Map<Integer, String> getStringFields() {
        return stringFields;
    }
    
    public void addStringField(int key, String value) {
    	if (stringFields == null)
    		stringFields = new HashMap<>();
    	stringFields.put(key,  value);	
    }
    
    public void putDoubleField(int key, Double value) {
    	if (doubleFields == null)
    		doubleFields = new HashMap<>();
    	doubleFields.put(key,  value);	
    }
    
    public void addStringArrayField(int key, String[] value) {
    	if (stringArrayFields == null)
    		stringArrayFields = new HashMap<>();
    	stringArrayFields.put(key,  value);	
    }

    public Map<Integer, Double> getDoubleFields() {
        return doubleFields;
    }

	public Map<Integer, String[]> getStringArrayFields() {
		return stringArrayFields;
	}
}
