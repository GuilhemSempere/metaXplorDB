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

import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.TypeAlias;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.format.annotation.DateTimeFormat;

/**
 *
 * @author petel, sempere
 */
@Document(collection = "phyloAssigns")
@TypeAlias(PhylogeneticAssignment.TYPE_ALIAS)
public class PhylogeneticAssignment {

    public static final String TYPE_ALIAS = "CP";
    /**
     * date when job was run
     */
    public static final String FIELDNAME_CREATED_TIME = "cr";
    
    public static final String FIELDNAME_JOB_IDS = "ji";

    public static final String REF_PKG_COLL_NAME = "refPackages";
    
    /**
     * url of the resulting pplacer file
     */
    public static final String FIELDNAME_PPLACER_OUTPUT_URL = "ru";
    
    /**
     * number of assignments found thru this job, that were saved to DB
     */
    public static final String FIELDNAME_ASSIGNMENTS_PERSISTED = "ap";

    // These are not actual fields of this POJO, they are used in records of the refPackages collection of the commons DB, which has no corresponding model class
    public static final String REF_PKG_DESC_FIELD_NAME = "desc";
	public static final String REF_PKG_KRONA_FIELD_NAME = "krona";

    /**
     * checksum of the parameters concatenated
     */
    @Id
    private String id;

    /**
     * creation date
     */
    @Field(FIELDNAME_CREATED_TIME)
    @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME)
    private Date createdDate;

	@Indexed
    @Field(FIELDNAME_JOB_IDS)
    private Set<String> jobIDs = new HashSet<>();

    /**
     * where result files are stored on the cluster
     */
    @Field(FIELDNAME_PPLACER_OUTPUT_URL)
    private String outputUrl;

    /**
     * number of assignments found thru this job, that were saved to DB
     */
    @Field(FIELDNAME_ASSIGNMENTS_PERSISTED)
    private int persistedAssignmentCount = 0;
    
    public int getPersistedAssignmentCount() {
		return persistedAssignmentCount;
	}

	public void setPersistedAssignmentCount(int persistedAssignmentCount) {
		this.persistedAssignmentCount = persistedAssignmentCount;
	}

	public PhylogeneticAssignment(String id, Date createdDate, /*String sequence, String packageName, */String outputUrl) {
        this.id = id;
        this.createdDate = createdDate;
        this.outputUrl = outputUrl;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Date getCreatedDate() {
        return createdDate;
    }

    public void setCreatedDate(Date createdDate) {
        this.createdDate = createdDate;
    }

    public Set<String> getJobIDs() {
		return jobIDs;
	}

	public void setJobIDs(Set<String> jobIDs) {
		this.jobIDs = jobIDs;
	}

    public String getOutputUrl() {
        return outputUrl;
    }

	public void setOutputUrl(String outputUrl) {
        this.outputUrl = outputUrl;
    }
}
