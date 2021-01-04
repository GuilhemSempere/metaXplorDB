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
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.TypeAlias;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.format.annotation.DateTimeFormat.ISO;

/**
 * blast result run on cluster and saved in db
 * @author petel, sempere
 */
@Document(collection = "blasts")
@TypeAlias(Blast.TYPE_ALIAS)
public class Blast {

    /**
     * alias
     */
    public static final String TYPE_ALIAS = "CB";
    /**
     *
     */
    public static final String FIELDNAME_RESULTS = "rs";
    /**
     * date when blast was performed
     */
    public static final String FIELDNAME_CREATED_TIME = "cr";
    /**
     * expect parameter (e-value)
     */
    public static final String FIELDNAME_EXPECT = "ex";
    /**
     * alignement parameter (max number of results return from blast)
     */
    public static final String FIELDNAME_ALIGN = "al";
    /**
     * sequence used for the blast
     */
    public static final String FIELDNAME_SEQUENCE = "sq";
    
    public static final String FIELDNAME_JOB_IDS = "ji";

    /**
     * checksum of the parameters concatenated
     */
    @Id
    private String id;

    /**
     * blast result. So=hould be nested json documents?
     */
    @Field(FIELDNAME_RESULTS)
    private String resultUrl;

    /**
     * document creation date
     */
    @Field(FIELDNAME_CREATED_TIME)
    @DateTimeFormat(iso = ISO.DATE_TIME)
    private Date createdDate;

    @Field(FIELDNAME_EXPECT)
    private double exp;

    @Field(FIELDNAME_ALIGN)
    private double alignment;

    @Field(FIELDNAME_SEQUENCE)
    private String seq;

    /**
     * type : [blastx, blastp, blastn, tblastx, tblastn]
     */
    @Field(DBField.FIELDNAME_TYPE)
    private String blastType;

    @Field(DBConstant.FIELDNAME_PROJECT)
    private int project;
    
	@Indexed
    @Field(FIELDNAME_JOB_IDS)
    private Set<String> jobIDs = new HashSet<>();


    public Blast(String id, String resultUrl, Date createdDate, double exp, double alignment, String seq, String blastType, int project) {
        this.id = id;
        this.resultUrl = resultUrl;
        this.createdDate = createdDate;
        this.exp = exp;
        this.alignment = alignment;
        this.seq = seq;
        this.blastType = blastType;
        this.project = project;
    }

    public double getExpect() {
        return exp;
    }

    public void setExpect(double expect) {
        this.exp = expect;
    }

    public double getAlign() {
        return alignment;
    }

    public void setAlign(double align) {
        this.alignment = align;
    }

    public String getSequence() {
        return seq;
    }

    public void setSequence(String sequence) {
        this.seq = sequence;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getResultUrl() {
        return resultUrl;
    }

    public void setResultUrl(String resultUrl) {
        this.resultUrl = resultUrl;
    }

    public Date getCreatedDate() {
        return createdDate;
    }

    public void setCreatedDate(Date createdDate) {
        this.createdDate = createdDate;
    }

    public String getType() {
        return blastType;
    }

    public void setType(String type) {
        this.blastType = type;
    }

    public int getProject() {
        return project;
    }

    public void setProject(int project) {
        this.project = project;
    }

    public Set<String> getJobIDs() {
		return jobIDs;
	}

	public void setJobIDs(Set<String> jobIDs) {
		this.jobIDs = jobIDs;
	}

}
