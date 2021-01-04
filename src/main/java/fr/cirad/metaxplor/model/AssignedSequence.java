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

import java.util.ArrayList;
import java.util.List;

import org.springframework.data.annotation.TypeAlias;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

/**
 *
 * @author petel, sempere
 */
@Document(collection = "assignedSequences")
@TypeAlias(AssignedSequence.TYPE_ALIAS)
public class AssignedSequence extends Sequence {

    public static final String TYPE_ALIAS = "SQ";
    
    public static final String FIELDNAME_ASSIGNMENT = "AS";
    
    @Field(FIELDNAME_ASSIGNMENT)
    private List<Assignment> assignments = new ArrayList<>();
  
    public AssignedSequence() {
    }
    
    public AssignedSequence(SequenceId id) {
        super(id);
    }

    public AssignedSequence(int projectId, String qseqid) {
        super(new SequenceId(projectId, qseqid));
    }

	public List<Assignment> getAssignments() {
		return assignments;
	}

	public void setAssignments(List<Assignment> assignments) {
		this.assignments = assignments;
	}
	
} 