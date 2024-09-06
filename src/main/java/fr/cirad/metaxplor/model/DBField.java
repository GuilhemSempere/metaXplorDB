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
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.collections.bidimap.DualHashBidiMap;
import org.apache.log4j.Logger;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.TypeAlias;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.util.AnnotatedTypeScanner;

import fr.cirad.tools.mongo.DBConstant;

/**
 * This class modelizes the type of entity that represents fields that may be contained in the following objects: Sample, Sequence, Assignment
 * @author sempere
 */
@Document(collection = "dbFields")
@TypeAlias(DBField.TYPE_ALIAS)
public class DBField {

	private static final Logger LOG = Logger.getLogger(DBField.class);
	
	private static final DualHashBidiMap supportedTypes = new DualHashBidiMap();

	private static final HashMap<String, List<String>> requiredFields = new HashMap<>();
	private static final ArrayList<DBField> fieldsWishStaticId = new ArrayList<>();
	
	public static final int sampleFieldId = 0 /*not a real DBField*/, sseqIdFieldId = 1, gpsPosFieldId = 2, collDateFieldId = 3, seqLengthFieldId = 4, taxonFieldId = 5, hitDefFieldId = 6, qseqIdFieldId = 7;
	
	public static final String bestHitFieldName = "best_hit";
	
	static {
		supportedTypes.put(String.class, DBConstant.STRING_TYPE);
		supportedTypes.put(String[].class, DBConstant.STRING_ARRAY_TYPE);
		supportedTypes.put(Double.class, DBConstant.DOUBLE_TYPE);
		supportedTypes.put(Date.class, DBConstant.DATE_TYPE);
		supportedTypes.put(Double[].class, DBConstant.GPS_TYPE);
		
		try
		{
			DBField dbField = new DBField(gpsPosFieldId, Sample.TYPE_ALIAS, Sample.FIELDNAME_COLLECT_GPS, DBConstant.GPS_TYPE);
			fieldsWishStaticId.add(dbField);
			dbField = new DBField(collDateFieldId, Sample.TYPE_ALIAS, Sample.FIELDNAME_COLLECT_DATE, DBConstant.DATE_TYPE);
			fieldsWishStaticId.add(dbField);

			dbField = new DBField(seqLengthFieldId, AssignedSequence.TYPE_ALIAS, DBConstant.FIELDNAME_SEQ_LENGTH, DBConstant.DOUBLE_TYPE);
			fieldsWishStaticId.add(dbField);
			dbField = new DBField(qseqIdFieldId, AssignedSequence.TYPE_ALIAS, AssignedSequence.FIELDNAME_QSEQID, DBConstant.STRING_TYPE);
			fieldsWishStaticId.add(dbField);
			dbField = new DBField(DBField.sampleFieldId, AssignedSequence.TYPE_ALIAS, Sample.FIELDNAME_SAMPLE_CODE, DBConstant.STRING_TYPE);
			fieldsWishStaticId.add(dbField);

			dbField = new DBField(sseqIdFieldId, AssignedSequence.FIELDNAME_ASSIGNMENT, Assignment.FIELDNAME_SSEQID, DBConstant.STRING_ARRAY_TYPE);
			fieldsWishStaticId.add(dbField);
			dbField = new DBField(taxonFieldId, AssignedSequence.FIELDNAME_ASSIGNMENT, DBConstant.FIELDNAME_TAXON, DBConstant.DOUBLE_TYPE);
			fieldsWishStaticId.add(dbField);
			dbField = new DBField(hitDefFieldId, AssignedSequence.FIELDNAME_ASSIGNMENT, DBConstant.FIELDNAME_HIT_DEFINITION, DBConstant.STRING_ARRAY_TYPE);
			fieldsWishStaticId.add(dbField);

			requiredFields.put(Sample.TYPE_ALIAS, Arrays.asList(Sample.FIELDNAME_SAMPLE_CODE /*linked to sample type because expected to be found in sample file*/, Sample.FIELDNAME_COLLECT_GPS, Sample.FIELDNAME_COLLECT_DATE));
			requiredFields.put(AssignedSequence.FIELDNAME_ASSIGNMENT, Arrays.asList(Sequence.FIELDNAME_QSEQID, Assignment.FIELDNAME_ASSIGN_METHOD));
		}
		catch (UnsupportedOperationException e)
		{
			LOG.error(e);
		}
	}
	
    public static final String TYPE_ALIAS = "DBF";
    
    public static final String FIELDNAME_TYPE = "ty";
    
    public static final String FIELDNAME_ENTITY_TYPEALIAS = "et";
    
    public static final String FIELDNAME_NAME = "nm";
        
    @Id
    private int id;
    
	/** Name of the entityTypeAlias holding the field. */
	@Field(FIELDNAME_ENTITY_TYPEALIAS)
	private String entityTypeAlias;

	/** The field name. */
	@Field(FIELDNAME_NAME)
	private String fieldName;

	public String getEntityTypeAlias() {
		return entityTypeAlias;
	}

	public String getFieldName() {
		return fieldName;
	}
    
	public DBField(int id, String entityTypeAlias, String fieldName, String type) throws UnsupportedOperationException {
		if (!supportedTypes.containsValue(type))
    		throw new UnsupportedOperationException("supported types are: " + DBConstant.STRING_TYPE + ", " + DBConstant.STRING_ARRAY_TYPE + ", " + DBConstant.DOUBLE_TYPE + ", " + DBConstant.DATE_TYPE + ", " + DBConstant.GPS_TYPE);
    
    	this.id = id;
		this.entityTypeAlias = entityTypeAlias;
		this.fieldName = fieldName;
		this.type = type;
	}
	
    public int getId() {
		return id;
	}
		
    @Field(DBConstant.FIELDNAME_PROJECT)
    private HashSet<Integer> projects = new HashSet<>();
    
    @Field(FIELDNAME_TYPE)
    private String type;
    
    public void addProject(int projId) {
    	if (!fieldsWishStaticId.stream().filter(dbf -> dbf.getId() != sseqIdFieldId).map(dbf -> dbf.getId()).collect(Collectors.toList()).contains(id))
    		projects.add(projId);
    	else
    		LOG.debug("Not adding project " + projId + " to DBField " + fieldName + " because it has a static id (and shall exist in all projects)");
    }

	public Class getTypeClass() throws ClassNotFoundException {
		return (Class) supportedTypes.getKey(type);
	}

	public String getType() {
		return type;
	}	

	public void getType(String type) {
		this.type = type;
	}

	public static Class getModelClassFromTypeAlias(String typeAlias)
	{
    	AnnotatedTypeScanner scanner = new AnnotatedTypeScanner(TypeAlias.class); 
    	for (Class clazz : scanner.findTypes(Assignment.class.getPackage().getName()))
    		if (typeAlias.equals(((TypeAlias) clazz.getAnnotation(TypeAlias.class)).value()))
    			return clazz;

    	return null;
	}

	public static HashMap<String, List<String>> getRequiredFields() {
		return requiredFields;
	}

	public static List<DBField> getFieldsNotNeedingProjectReference() {
		return fieldsWishStaticId.stream().filter(dbf -> dbf.getId() != sseqIdFieldId).collect(Collectors.toList());
	}

	public static ArrayList<DBField> getFieldsWishStaticId() {
		return fieldsWishStaticId;
	}

	public String toString() {
		return getEntityTypeAlias() + "§" + getFieldName();
	}
	
	public boolean equals(Object o) {
		if (o == null || !(o instanceof DBField))
			return false;
		
		return ((DBField) o).getId() == getId();
	}
}
