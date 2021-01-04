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
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.log4j.Logger;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.TypeAlias;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

import fr.cirad.metaxplor.model.Sequence.SequenceId;

/**
 * NCBI Accession model
 *
 * @author petel, sempere
 */
/**
 * @author sempere
 *
 */
@Document(collection = "accessions")
@TypeAlias(Accession.TYPE_ALIAS)
public class Accession {
	private static final Logger LOG = Logger.getLogger(Accession.class);
	
	public static final String TYPE_ALIAS = "AC";

    public static final String FIELDNAME_NCBI_TAXID = "tx";
    public static final String FIELDNAME_HIT_DEFINITION = "hd";

    static public class AccessionId
	{
        public static final char NUCLEOTIDE_TYPE = 'n';	// this is the default and may be omitted
        public static final char PROTEIN_TYPE = 'p';
        
        private static final List<Character> allowedTypes = Arrays.asList(NUCLEOTIDE_TYPE, PROTEIN_TYPE);

		/** The project id. */
		@Indexed
		@Field(DBField.FIELDNAME_TYPE)
		private char type;

		/** The query sequence id (from the fasta file). */
		@Indexed
		@Field(Assignment.FIELDNAME_SSEQID)
		private String sseqid;
		
		public AccessionId() {
		}
		
		public AccessionId(char type, String sseqid) {
			if (!allowedTypes.contains(type))
				LOG.error("Type " + type + " is not allowed for Accession");
			this.type = type;
			this.sseqid = sseqid;
		}

		public void setType(char type) {
			this.type = type;
		}

		public void setSseqid(String sseqid) {
			this.sseqid = sseqid;
		}

		public char getType() {
			return type;
		}

		public String getSseqid() {
			return sseqid;
		}
		
		public boolean equals(Object o) {
			if (o == null || !(o instanceof SequenceId))
				return false;
			
			return type == ((AccessionId) o).getType() && sseqid.equals(((AccessionId) o).getSseqid());
		}
		
		public String toString() {
			return type + ":" + sseqid;
		}
	}
    
    public static final String ID_NUCLEOTIDE_PREFIX = AccessionId.NUCLEOTIDE_TYPE + ":";	// this is the default and may be omitted
    public static final String ID_PROTEIN_PREFIX = AccessionId.PROTEIN_TYPE + ":";
    
	@Id
    private AccessionId id;
	
	@Indexed
	@Field(FIELDNAME_NCBI_TAXID)
    private Integer tx;
	
	@Field(FIELDNAME_HIT_DEFINITION)
    private String hd;

	public Accession(AccessionId id, Integer tx, String hd) {
        this.id = id;
        this.tx = tx;
        this.hd = hd;
    }

    public AccessionId getId() {
        return id;
    }
    
    public Integer getTx() {
        return tx;
    }
    
    public String getHd() {
        return hd;
    }

	public void setTx(int tx) {
		this.tx = tx;
	}

	public void setHd(String hd) {
		this.hd = hd;
	}

    public Boolean isProtein() {
		return id.getType() == AccessionId.PROTEIN_TYPE;
	}

	/**
	 * @param accIDs
	 * @param fSpecifyPrefix
	 * @return an array of List<String> where the first item contains nucleotide accessions and the second protein accessions
	 */
	public static List<String>[] separateNuclFromProtIDs(Collection<String> accIDs, boolean fSpecifyPrefix) {
		List<String>[] result = new List[] { new ArrayList(), new ArrayList() };
		for (String accId : accIDs) {
        	boolean fProt = accId.startsWith(ID_PROTEIN_PREFIX);
        	Collection<String> accColl = fProt ? result[1] : result[0];
        	String prefix = fProt ? ID_PROTEIN_PREFIX : ID_NUCLEOTIDE_PREFIX;
        	if (!fSpecifyPrefix && (fProt || accId.startsWith(prefix)))
        		accId = accId.replaceFirst("^" + prefix, "");
        	else if (fSpecifyPrefix && !fProt && !accId.startsWith(prefix))
        		accId = prefix + accId;
        	accColl.add(accId);
        }
		return result;
	}
	
	/**
	 * @param accIDs
	 * @return a List<AccessionId>
	 */
	public static List<AccessionId> buildAccessionIDsFromPrefixedString(Collection<String> accIDs) {
		List<AccessionId> result = new ArrayList<>();
		for (String accId : accIDs) {
        	boolean fProt = accId.startsWith(ID_PROTEIN_PREFIX);
        	String prefix = fProt ? ID_PROTEIN_PREFIX : ID_NUCLEOTIDE_PREFIX;
        	result.add(new AccessionId(fProt ? AccessionId.PROTEIN_TYPE : AccessionId.NUCLEOTIDE_TYPE, accId.replaceFirst("^" + prefix, "")));
        }
		return result;
	}
}