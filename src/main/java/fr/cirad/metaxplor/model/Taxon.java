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

import java.util.List;

import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.TypeAlias;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

@Document(collection = "taxon")
@TypeAlias(Taxon.TYPE_ALIAS)
public class Taxon {

    public static final String TYPE_ALIAS = "TA";
    
    public static final String FIELDNAME_NAMES = "na";
    public static final String FIELDNAME_RANK = "ra";
    public static final String FIELDNAME_PARENT_ID = "pa";

	public static final int UNIDENTIFIED_ORGANISM_TAXID = 32644;

    @Id
    private final Integer id;

    @Field(FIELDNAME_RANK)
    private String rank;

    @Field(FIELDNAME_NAMES)
    private List<String> names;

    @Field(FIELDNAME_PARENT_ID)
    private Integer parentId;

    public Taxon(Integer id) {
        this.id = id;
    }

    public Integer getId() {
        return id;
    }

    public String getRank() {
        return rank;
    }

    public void setRank(String rank) {
        this.rank = rank;
    }

    public List<String> getNames() {
        return names;
    }

    public void setNames(List<String> names) {
        this.names = names;
    }

    public Integer getParentId() {
        return parentId;
    }

    public void setParentId(Integer parentId) {
        this.parentId = parentId;
    }
}
