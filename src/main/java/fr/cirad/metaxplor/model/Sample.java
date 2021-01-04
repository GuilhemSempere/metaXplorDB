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

import fr.cirad.tools.mongo.DBConstant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.TypeAlias;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

/**
 *
 * @author petel, sempere
 */
@Document(collection = "samples")
@TypeAlias(Sample.TYPE_ALIAS)
public class Sample {

    public static final String TYPE_ALIAS = "SP";
    public static final String FIELDNAME_SAMPLE_CODE = "sample_name";
    /**
     * date where sample from this individual were collected
     */
    public static final String FIELDNAME_COLLECT_DATE = "collection_date";
    /**
     * position of the individual (As we focus on plants, we can safely assume
     * that an individual has a single position)
     */
    public static final String FIELDNAME_COLLECT_GPS = "lat_lon";

    @Id
    private String id;

    @Field(DBConstant.FIELDNAME_PROJECT)
    private HashSet<Integer> projects = new HashSet<>();

    @Field(DBConstant.STRING_TYPE)
    private Map<Integer, String> stringFields;

    @Field(DBConstant.DOUBLE_TYPE)
    private Map<Integer, Double> numberFields;    

    @Field(DBConstant.DATE_TYPE)
    private Map<Integer, String> dateFields = new HashMap<>();

    @Field(DBConstant.GPS_TYPE)
    private Map<Integer, Double[]> gpsFields = new HashMap<>();

    public Sample(String id) {
    	this.id = id;
    }

    public String getId() {
        return id;
    }

    public HashSet<Integer> getProjects() {
		return projects;
	}

	public void setProjects(HashSet<Integer> projects) {
		this.projects = projects;
	}

    public Map<Integer, String> getStringFields() {
        return stringFields;
    }

    public void setStringFields(Map<Integer, String> stringFields) {
        this.stringFields = stringFields;
    }

	public Map<Integer, Double> getNumberFields() {
        return numberFields;
    }

    public void setNumberFields(Map<Integer, Double> numberFields) {
        this.numberFields = numberFields;
    }

    public Map<Integer, String> getDateFields() {
		return dateFields;
	}

	public void setDateFields(Map<Integer, String> dateFields) {
		this.dateFields = dateFields;
	}

	public Map<Integer, Double[]> getGpsFields() {
		return gpsFields;
	}

	public void setGpsFields(Map<Integer, Double[]> gpsFields) {
		this.gpsFields = gpsFields;
	}

}
