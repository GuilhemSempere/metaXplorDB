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

import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.TypeAlias;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

import java.util.Date;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.format.annotation.DateTimeFormat.ISO;

/**
 * store projects document, with meta-information but also list of distinct
 * element of Sequence/Individual of the project to fill filters widgets
 *
 * @author petel, sempere
 */
@Document(collection = "projects")
@TypeAlias(MetagenomicsProject.TYPE_ALIAS)
public class MetagenomicsProject {

    public static final String TYPE_ALIAS = "MP";

    /**
     * project acronym
     */
    public static final String FIELDNAME_ACRONYM = "ac";
    /**
     * project full name
     */
    public static final String FIELDNAME_NAME = "nm";
    /**
     * description of the project
     */
    public static final String FIELDNAME_DESCRIPTION = "de";
    /**
     * list of data authors
     */
    public static final String FIELDNAME_AUTHORS = "au";
    /**
     * how to reach data authors. Can be emailAdress, UMR/lab adress ect
     */
    public static final String FIELDNAME_CONTACT_INFO = "ci";
    /**
     * sequencing technologie used (454 | illumina | pacBio )
     */
    public static final String FIELDNAME_SEQUENCING_TECHNOLOGY = "sqt";
    /**
     * date when samples were sequenced
     */
    public static final String FIELDNAME_SEQUENCING_DATE = "sqd";
    /**
     * how were data assembled
     */
    public static final String FIELDNAME_ASSEMBLY_METHOD = "asm";
    /**
     * link to a publication using thoses data
     */
    public static final String FIELDNAME_PUBLICATION = "pb";
    /**
     * are the original samples still available ?
     */
    public static final String FIELDNAME_DATA_AVAIL = "av";
    /**
     * project visibility. If true, everybody can access it
     */
    public static final String FIELDNAME_PUBLIC = "p";

    /**
     *
     */
    public MetagenomicsProject() {
    }
    /**
     * list of runs in this project
     */
    public static final String FIELDNAME_RUNS = "rn";

    public static final String FIELDNAME_META_INFO = "mi";

    @Id
    private int id;

    @Field(FIELDNAME_ACRONYM)
    private String acronym;

    @Field(FIELDNAME_NAME)
    private String name;

    @Field(FIELDNAME_DESCRIPTION)
    private String description;

    @Field(FIELDNAME_META_INFO)
    private String metaInfo;

    @Field(FIELDNAME_AUTHORS)
    private String authors;

    @Field(FIELDNAME_CONTACT_INFO)
    private String contactInfo;

    @Field(FIELDNAME_SEQUENCING_TECHNOLOGY)
    private String sequencingTechnology;

    @Field(FIELDNAME_SEQUENCING_DATE)
    @DateTimeFormat(iso = ISO.DATE_TIME)
    private Date sequencingDate;

    @Field(FIELDNAME_ASSEMBLY_METHOD)
    private String assemblyMethod;

    @Field(FIELDNAME_PUBLICATION)
    private String publication;

    @Field(FIELDNAME_DATA_AVAIL)
    private boolean isAvail;

    @Field(FIELDNAME_PUBLIC)
    private boolean publicProject;


    public MetagenomicsProject(int id) {
        super();
        this.id = id;
    }

    public int getId() {
        return id;
    }

    public String getAcronym() {
        return acronym;
    }

    public void setAcronym(String acronym) {
        this.acronym = acronym;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getMetaInfo() {
        return metaInfo;
    }

    public void setMetaInfo(String metaInfo) {
        this.metaInfo = metaInfo;
    }

    public String getAuthors() {
        return authors;
    }

    public void setAuthors(String authors) {
        this.authors = authors;
    }

    public String getContactInfo() {
        return contactInfo;
    }

    public void setContactInfo(String contactInfo) {
        this.contactInfo = contactInfo;
    }

    public String getSequencingTechnology() {
        return sequencingTechnology;
    }

    public void setSequencingTechnology(String sequencingTechnology) {
        this.sequencingTechnology = sequencingTechnology;
    }

    public String getAssemblyMethod() {
        return assemblyMethod;
    }

    public void setAssemblyMethod(String assemblyMethod) {
        this.assemblyMethod = assemblyMethod;
    }

    public String getPublication() {
        return publication;
    }

    public void setPublication(String publication) {
        this.publication = publication;
    }

    public Date getSequencingDate() {
        return sequencingDate;
    }

    public void setSequencingDate(Date sequencingDate) {
        this.sequencingDate = sequencingDate;
    }

    public boolean isIsAvail() {
        return isAvail;
    }

    public void setIsAvail(boolean isAvail) {
        this.isAvail = isAvail;
    }

    public boolean isPublicProject() {
        return publicProject;
    }

    public void setPublicProject(boolean publicProject) {
        this.publicProject = publicProject;
    }
}
