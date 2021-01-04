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
import htsjdk.samtools.reference.ReferenceSequenceFileFactory;

import java.util.List;
import java.util.Map;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.TypeAlias;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

/**
 *
 * @author petel, sempere
 */
@Document(collection = "sequences")
@TypeAlias(Sequence.TYPE_ALIAS)
public class Sequence {

    /**
     * extension name for nucleic fasta
     */
    public static final String NUCL_FASTA_EXT = ".fna";
    /**
     * extension name for nucleic fai index
     */
    public static final String NUCL_FAI_EXT = ReferenceSequenceFileFactory.FASTA_INDEX_EXTENSION;
    /**
     * extension name for fasta query / export.
     */
    public static final String FULL_FASTA_EXT = ".fasta";

    public static final String TYPE_ALIAS = "US";

    public static final String FIELDNAME_QSEQID = "qseqid";
    
    /**
     * contribution of each samples of the project to this sequence.
     */
    public static final String FIELDNAME_SAMPLE_COMPOSITION = "sc";
    
    /**
     * length of the nucleotidic sequence
     */
    public static final String FIELDNAME_SEQUENCE_LENGTH = "sequence_length";

    static public class SequenceId
	{		
		/** The project id. */
		@Indexed
		@Field(DBConstant.FIELDNAME_PROJECT)
		private int projectId;

		/** The query sequence id (from the fasta file). */
		@Indexed
		@Field(FIELDNAME_QSEQID)
		private String qseqid;
		
		public SequenceId(int projectId, String qseqid) {
			this.projectId = projectId;
			this.qseqid = qseqid;
		}

		public int getProjectId() {
			return projectId;
		}

		public String getQseqid() {
			return qseqid;
		}
		
		public boolean equals(Object o) {
			if (o == null || !(o instanceof SequenceId))
				return false;
			
			return projectId == ((SequenceId) o).getProjectId() && qseqid.equals(((SequenceId) o).getQseqid());
		}
		
		public String toString() {
			return projectId + "§" + qseqid;
		}
		
		public int hashCode() {
			return toString().hashCode();
		}
	}

    @Id
    private SequenceId id;

    @Field(DBConstant.DOUBLE_TYPE)
    private Map<Integer, Double> doubleFields;

    @Field(FIELDNAME_SAMPLE_COMPOSITION)
    private List<SampleReadCount> sampleComposition;
    
    public Sequence() {
    }
    
    public Sequence(SequenceId id) {
        this.id = id;
    }

    public Sequence(int projectId, String qseqid) {
        id = new SequenceId(projectId, qseqid);
    }

    public SequenceId getId() {
		return id;
	}

    public Map<Integer, Double> getDoubleFields() {
        return doubleFields;
    }

    public void setDoubleFields(Map<Integer, Double> doubleFields) {
        this.doubleFields = doubleFields;
    }

	public List<SampleReadCount> getSampleComposition() {
        return sampleComposition;
    }

    public void setSampleComposition(List<SampleReadCount> sampleComposition) {
        this.sampleComposition = sampleComposition;
    }

	public boolean equals(Object o) {
		if (o == null || !(o instanceof Sequence))
			return false;
		
		return id.equals(((Sequence) o).getId());
	}
	
	public int hashCode() {
		return getId().hashCode();
	}
} 