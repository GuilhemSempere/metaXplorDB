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
package fr.cirad.metaxplor.importing;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Component;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoCommandException;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;

import fr.cirad.metaxplor.jobs.base.IOpalServiceInvoker;
import fr.cirad.metaxplor.model.Accession;
import fr.cirad.metaxplor.model.Accession.AccessionId;
import fr.cirad.metaxplor.model.AssignedSequence;
import fr.cirad.metaxplor.model.Assignment;
import fr.cirad.metaxplor.model.AutoIncrementCounter;
import fr.cirad.metaxplor.model.DBField;
import fr.cirad.metaxplor.model.MetagenomicsProject;
import fr.cirad.metaxplor.model.Sample;
import fr.cirad.metaxplor.model.SampleReadCount;
import fr.cirad.metaxplor.model.Sequence;
import fr.cirad.metaxplor.model.Sequence.SequenceId;
import fr.cirad.metaxplor.model.TaxonomyNode;
import fr.cirad.tools.AppConfig;
import fr.cirad.tools.Constant;
import fr.cirad.tools.Helper;
import fr.cirad.tools.ProgressIndicator;
import fr.cirad.tools.mongo.DBConstant;
import fr.cirad.tools.mongo.MongoTemplateManager;
import htsjdk.samtools.SAMException;
import htsjdk.samtools.reference.FastaSequenceIndex;
import htsjdk.samtools.reference.FastaSequenceIndexCreator;
import htsjdk.samtools.reference.IndexedFastaSequenceFile;

/**
 *
 * @author sempere, petel, abbe
 */
@Component
public class MtxImport {

    /**
     * logger
     */
    private static final Logger LOG = Logger.getLogger(MtxImport.class);
    
    @Autowired private AppConfig appConfig;
    
    private static Map<String /*module*/, Map<String /*field name*/, Comparable[]>> sampleFieldsToPersist = new HashMap<>();		// static to support multiple concurrent imports
    private static Map<String /*module*/, Map<String /*field name*/, Comparable[]>> assignmentFieldsToPersist = new HashMap<>();	// static to support multiple concurrent imports
    private static Map<String /*module*/, Collection<Integer>> currentlyImportedProjects = new HashMap<>();	// static to support multiple concurrent imports
    
	private @Autowired AccessionImport accessionImport;

    /**
     * import data from importFiles
     *
     * @param opalServiceInvoker
     * @param module
     * @param projectId
     * @param projectCode acronym
     * @param projectName project full name
     * @param projectDescription
     * @param authors
     * @param adress
     * @param seqDate
     * @param seqTech
     * @param assemblTech
     * @param isAvail
     * @param maxAccessionCountPerAssignment
     * @param pub
     * @param extraInfo
     * @param importZipURL
     * @param progress
     * @param access
     * @return A map with number of sequences and samples imported
     * @throws Exception 
     */
    public Map<String, String> doImport(IOpalServiceInvoker opalServiceInvoker, String module, int projectId, String projectCode, String projectName, String projectDescription, String authors, String adress, String seqDate, String seqTech, String assemblTech, boolean isAvail, int maxAccessionCountPerAssignment, String pub, String extraInfo, URL importZipURL, ProgressIndicator progress, boolean access) throws Exception {
    	long before = System.currentTimeMillis();

    	Collection<Integer> currentlyImportedProjectsForModule = currentlyImportedProjects.get(module);
    	if (currentlyImportedProjectsForModule == null) {
    		currentlyImportedProjectsForModule = new HashSet<>();
    		currentlyImportedProjects.put(module, currentlyImportedProjectsForModule);
    	}
    	currentlyImportedProjectsForModule.add(projectId);

        MongoTemplate mongoTemplate = MongoTemplateManager.get(module);
        AutoIncrementCounter.ensureCounterIsAbove(mongoTemplate, MongoTemplateManager.getMongoCollectionName(DBField.class), DBField.getFieldsWishStaticId().size());

        // this map holds reference of available fields 
        LinkedHashMap<Integer, DBField> fields = new LinkedHashMap<>();

        String importedFastaFileName = null;
        Map<String, Comparable[]> fieldNameToIdAndTypeMap;

    	ZipEntry ze;
    	ZipInputStream zis = new ZipInputStream(importZipURL.openStream());

    	List<String> sampleFieldsWithStaticId = (List<String>) DBField.getFieldsWishStaticId().stream().filter(dbf -> Sample.TYPE_ALIAS.equals(dbf.getEntityTypeAlias())).map(dbf -> dbf.getFieldName()).collect(Collectors.toList());

    	while ((ze = zis.getNextEntry()) != null)
           if (ze.getName().endsWith("samples.tsv")) {
               	progress.addStep("Importing sample file contents");
            	progress.moveToNextStep();
            	fieldNameToIdAndTypeMap = importSamples(module, projectId, zis);
                for (String fieldName : fieldNameToIdAndTypeMap.keySet()) {
                	Comparable[] idAndType = fieldNameToIdAndTypeMap.get(fieldName);

                	if (idAndType[1] != null || sampleFieldsWithStaticId.contains(fieldName)) {	// otherwise there was no data in that column
    	            	DBField dbField = mongoTemplate.findById(idAndType[0], DBField.class);
    	            	if (dbField == null) {
    	            		dbField = new DBField((int) idAndType[0], Sample.TYPE_ALIAS, fieldName, (String) idAndType[1]);
       						mongoTemplate.save(dbField);
       					}
    	            	fields.put((int) idAndType[0], dbField);
                	}
                }
                break;
           }

    	zis = new ZipInputStream(importZipURL.openStream());
    	while ((ze = zis.getNextEntry()) != null)
           if (ze.getName().endsWith(Sequence.FULL_FASTA_EXT)) {
               	progress.addStep("Importing and indexing fasta file contents");
            	progress.moveToNextStep();
            	try {
            		importedFastaFileName = importFasta(module, projectId, zis);
            	}
            	catch (StringIndexOutOfBoundsException siobe) {
            		if ("String index out of range: 0".equals(siobe.getMessage())) {
                    	throw new Exception("Error occured while indexing fasta file: make sure fasta contains no empty lines!");
            		}
            	}
                if (importedFastaFileName == null)
                	throw new Exception("Error occured while importing fasta file");
                break;
           }

    	zis = new ZipInputStream(importZipURL.openStream());
    	while ((ze = zis.getNextEntry()) != null)
           if (ze.getName().endsWith("assignments.tsv")) {
        	   updateAccessionCache(zis, maxAccessionCountPerAssignment, progress);
               break;
           }

    	File importedFasta = new File(appConfig.sequenceLocation() + File.separator + module + File.separator + importedFastaFileName);

    	String makeblastdbJobID;
        try {	// we don't do this before this stage because import process gets aborted when NCBI service is not available
        	makeblastdbJobID = opalServiceInvoker.makeBlastDb(module, projectId, importedFasta);	// job that runs asynchronously
        } catch (Exception ex) {
        	LOG.error("makeBlastDb failed", ex);
        	throw new Exception("Error occured on HPC while creating blast DB: " + ex.getMessage());
        }

    	progress.addStep("Loading fasta index");
    	progress.moveToNextStep();
    	IndexedFastaSequenceFile indexedFasta = new IndexedFastaSequenceFile(importedFasta);
    	List<String> assignmentFieldsWithStaticId = (List<String>) DBField.getFieldsWishStaticId().stream().filter(dbf -> AssignedSequence.FIELDNAME_ASSIGNMENT.equals(dbf.getEntityTypeAlias())).map(dbf -> dbf.getFieldName()).collect(Collectors.toList());

    	zis = new ZipInputStream(importZipURL.openStream());
    	while ((ze = zis.getNextEntry()) != null)
           if (ze.getName().endsWith("assignments.tsv")) {
	           	progress.addStep("Processing lines in assignment file");
	        	progress.moveToNextStep();
	        	fieldNameToIdAndTypeMap = importAssignments(module, projectId, zis, maxAccessionCountPerAssignment, indexedFasta, progress);
                for (String fieldName : fieldNameToIdAndTypeMap.keySet()) {
                   	Comparable[] idAndType = fieldNameToIdAndTypeMap.get(fieldName);

                   	if (idAndType[1] != null || assignmentFieldsWithStaticId.contains(fieldName)) {	// otherwise there was no data in that column
                   		DBField dbField = mongoTemplate.findById(idAndType[0], DBField.class);
       					if (dbField == null) {
       						dbField = new DBField((int) idAndType[0], AssignedSequence.FIELDNAME_ASSIGNMENT, fieldName, (String) idAndType[1]);
       						mongoTemplate.save(dbField);
       					}
       					fields.put((int) idAndType[0], dbField);
               		}
               }
               break;
           }

    	HashSet<String> assignedSeqIDs = new HashSet<>((Collection<String>) mongoTemplate.findDistinct(new Query(Criteria.where("_id." + DBConstant.FIELDNAME_PROJECT).is(projectId)), "_id." + Sequence.FIELDNAME_QSEQID, AssignedSequence.class, String.class));
    	createPartialFai(new File(importedFasta.getParent() + "/" + importedFasta.getName() + Sequence.NUCL_FAI_EXT), new File(importedFasta.getParent() + "/_" + importedFasta.getName() + Sequence.NUCL_FAI_EXT), assignedSeqIDs);

    	zis = new ZipInputStream(importZipURL.openStream());
    	while ((ze = zis.getNextEntry()) != null)
           if (ze.getName().endsWith("sequences.tsv")) {
               importSequences(module, projectId, zis, maxAccessionCountPerAssignment, indexedFasta, progress);
               break;
           }

    	indexedFasta.close();
    	zis.close();

    	progress.addStep("Updating database indexes");
    	progress.moveToNextStep();
    	Helper.removeObsoleteIndexes(module);	// just in case
        ensureIndexes(mongoTemplate, fields);

        Map<String, String> result = saveProject(
                mongoTemplate,
                projectId,
                projectCode,
                projectName,
                projectDescription,
                authors,
                adress,
                seqDate,
                seqTech,
                assemblTech,
                isAvail,
                pub,
                extraInfo,
                access
        );
        
        try {
        	opalServiceInvoker.getMakeBlastDbStatus(makeblastdbJobID);	// if it failed before we get here then this import will be cancelled
        }
        catch (Exception e) {
        	throw new Exception("Import cancelled because system was unable to create BLAST / DIAMOND bank on HPC: " + e.getMessage());
        }

    	progress.addStep("Generating cache for search widgets");
    	progress.moveToNextStep();
        updateDBFieldsAndComputeCache(module, fields.values(), projectId, progress);
        currentlyImportedProjectsForModule.remove(projectId);

        progress.markAsComplete();
        
        LOG.info("doImport took " + (System.currentTimeMillis() - before)/1000 + "s");
        return result;
    }

	/**
     * For each field in 'fields' set project id AND generate the corresponding cache collection.
     * Cache collection can store two type of values, String or double. They are shared by all projects within the module
     *
     * @param module
     * @param fields
     * @param projectId
     * @param progress 
     * @throws ClassNotFoundException 
     * @throws UnsupportedOperationException 
     */
    private synchronized static void updateDBFieldsAndComputeCache(String module, Collection<DBField> fields, int projectId, ProgressIndicator progress) throws ClassNotFoundException, UnsupportedOperationException {        
    	if (fields.isEmpty())
    		return;
    	
    	MongoTemplate mongoTemplate = MongoTemplateManager.get(module);
    	
    	Collection<DBField> fieldsToUpdateAndComputeCacheFor = new ArrayList<>(fields);
    	DBField sampleField = new DBField(DBField.sampleFieldId, AssignedSequence.TYPE_ALIAS, Sample.FIELDNAME_SAMPLE_CODE, DBConstant.STRING_TYPE);
    	fieldsToUpdateAndComputeCacheFor.add(sampleField);
    	DBField seqLengthField = new DBField(DBField.seqLengthFieldId, AssignedSequence.TYPE_ALIAS, AssignedSequence.FIELDNAME_SEQUENCE_LENGTH, DBConstant.DOUBLE_TYPE);
    	fieldsToUpdateAndComputeCacheFor.add(seqLengthField);
    	if (mongoTemplate.count(new Query(Criteria.where("_id").is(DBField.sampleFieldId)), DBField.class) == 0)
    		mongoTemplate.save(sampleField);
    	if (mongoTemplate.count(new Query(Criteria.where("_id").is(DBField.seqLengthFieldId)), DBField.class) == 0)
    		mongoTemplate.save(seqLengthField);

        Map<String, Comparable[]> sampleFieldsToPersistForModule = sampleFieldsToPersist.get(module), assignmentFieldsToPersistForModule = assignmentFieldsToPersist.get(module);

        int nEntryIndex = 0;
        for (DBField dbField : fieldsToUpdateAndComputeCacheFor) {
            boolean fIsAssignmentField = AssignedSequence.FIELDNAME_ASSIGNMENT.equals(dbField.getEntityTypeAlias());
    		if (fIsAssignmentField) {
    			if (assignmentFieldsToPersistForModule != null)
    				assignmentFieldsToPersistForModule.remove(dbField.getFieldName());
    		}
    		else if (sampleFieldsToPersistForModule != null)
    			sampleFieldsToPersistForModule.remove(dbField.getFieldName());

    		if (dbField.getId() != DBField.taxonFieldId)
    			computeFieldCache(mongoTemplate, projectId, dbField);
    		
            progress.setCurrentStepProgress(++nEntryIndex * 100 / fieldsToUpdateAndComputeCacheFor.size());
        }
        progress.setCurrentStepProgress(100);

    	List<String> sampleFieldsWithStaticId = (List<String>) DBField.getFieldsWishStaticId().stream().filter(dbf -> Sample.TYPE_ALIAS.equals(dbf.getEntityTypeAlias())).map(dbf -> dbf.getFieldName()).collect(Collectors.toList());
    	List<String> assignmentFieldsWithStaticId = (List<String>) DBField.getFieldsWishStaticId().stream().filter(dbf -> AssignedSequence.FIELDNAME_ASSIGNMENT.equals(dbf.getEntityTypeAlias())).map(dbf -> dbf.getFieldName()).collect(Collectors.toList());
    	List<DBField> fieldsNotRequiringProjectReference = DBField.getFieldsNotNeedingProjectReference();
    	mongoTemplate.updateMulti(new Query(Criteria.where("_id").in(fieldsToUpdateAndComputeCacheFor.stream()
    		.filter(dbf -> !fieldsNotRequiringProjectReference.contains(dbf))
			.map(dbf -> dbf.getId()).collect(Collectors.toList()))), new Update().push(DBConstant.FIELDNAME_PROJECT, projectId), DBField.class);
        
        List<String> emptyFields = new ArrayList<String>();
        if (sampleFieldsToPersistForModule != null)
	        for (String key : sampleFieldsToPersistForModule.keySet()) {
	        	Comparable[] unsavedFieldInfo = sampleFieldsToPersistForModule.get(key);
	        	if (unsavedFieldInfo[1] == null) {
	        		if (!sampleFieldsWithStaticId.contains(key))
	        			LOG.info("No data found in sample column " + key);
	        		emptyFields.add(key);
	        	}
	        }
        if (!emptyFields.isEmpty())
        	for (String key : emptyFields)
        		sampleFieldsToPersistForModule.remove(key);
        
        emptyFields = new ArrayList<String>();
        if (assignmentFieldsToPersistForModule != null)
	        for (String key : assignmentFieldsToPersistForModule.keySet()) {
	        	Comparable[] unsavedFieldInfo = assignmentFieldsToPersistForModule.get(key);
	        	if (unsavedFieldInfo[1] == null) {
	        		if (!assignmentFieldsWithStaticId.contains(key) && !Sequence.FIELDNAME_QSEQID.equals(key))
	        			LOG.info("No data found in assignment column " + key);
	        		emptyFields.add(key);
	        	}
	        }
        if (!emptyFields.isEmpty())
        	for (String key : emptyFields)
        		assignmentFieldsToPersistForModule.remove(key);
    }
    
    public static void computeFieldCache(MongoTemplate mongoTemplate, int projectId, DBField dbField) throws UnsupportedOperationException, ClassNotFoundException {
        boolean fIsAssignmentField = AssignedSequence.FIELDNAME_ASSIGNMENT.equals(dbField.getEntityTypeAlias());
    	Class type = dbField.getTypeClass();
    	String pathPrefix = fIsAssignmentField ? AssignedSequence.FIELDNAME_ASSIGNMENT + "." : ""; 
        List<Integer> stringArrayFieldIDs = Arrays.asList(DBField.sseqIdFieldId, DBField.hitDefFieldId);
        String cacheCollectionName = DBConstant.CACHE_PREFIX + dbField.getId();
        Class<?> entityClass = DBField.getModelClassFromTypeAlias(dbField.getEntityTypeAlias());
        MongoCollection<Document> baseCollection = mongoTemplate.getCollection(MongoTemplateManager.getMongoCollectionName(Assignment.class.equals(entityClass) ? AssignedSequence.class : entityClass));
        String projectFieldPath = (entityClass.equals(Sample.class) ? "" : "_id.") + DBConstant.FIELDNAME_PROJECT;

    	if (String.class.equals(type) || String[].class.equals(type)) {
            // store docs as { "_id": "value", "pj": [ 1, 3, 4]} where "pj" is the id of project containing this value
        	List<BasicDBObject> pipeline = new ArrayList<>();
            if (dbField.getId() == DBField.sampleFieldId || !pathPrefix.isEmpty()) {
            	pipeline.add(new BasicDBObject("$unwind", "$" + (dbField.getId() == DBField.sampleFieldId ? Sequence.FIELDNAME_SAMPLE_COMPOSITION : AssignedSequence.FIELDNAME_ASSIGNMENT)));
            	
            	if (stringArrayFieldIDs.contains(dbField.getId())) {
                    // in this case, the field is an array so unwind it before applying other stages of the pipeline
                    pipeline.add(new BasicDBObject("$unwind", "$" + pathPrefix + dbField.getType() + "." + dbField.getId()));
            	}
            }
            
            if (entityClass.equals(Sample.class)) {
                // in this case, project is an array so unwind it before applying other stages of the pipeline
                pipeline.add(new BasicDBObject("$unwind", "$" + projectFieldPath));
            }  
        	
            pipeline.add(new BasicDBObject("$group", new BasicDBObject("_id", "$" + (dbField.getId() == DBField.sampleFieldId ? (Sequence.FIELDNAME_SAMPLE_COMPOSITION + "." + SampleReadCount.FIELDNAME_SAMPLE_CODE) : (pathPrefix + dbField.getType() + "." + dbField.getId())))
            		.append(DBConstant.FIELDNAME_PROJECT, new BasicDBObject("$addToSet", "$" + projectFieldPath))));
            pipeline.add(new BasicDBObject("$match", new BasicDBObject("_id", new BasicDBObject("$ne", null))));
            pipeline.add(new BasicDBObject("$out", cacheCollectionName));

            try {
            	baseCollection.aggregate(pipeline).allowDiskUse(true).toCollection();	/* invoking toCollection() is necessary for $out to take effect */
               	mongoTemplate.getCollection(cacheCollectionName).createIndex(new BasicDBObject(projectFieldPath, 1));	// create an index on 'pj' field, as it's the one that will be used for creating filters
            }
            catch (MongoCommandException mce) {
            	if (!mce.getMessage().toLowerCase().contains("changed during processing"))
            		throw mce;	// otherwise we can ignore it, it was only being executed twice at once
            }
    }
    else if (Double.class.equals(type) || Date.class.equals(type)) {
            // store docs as { "_id": 1, "min": 0, "max": 256 }
            // where "_id" is the projectId
            int[] projectIds = new int[]{projectId};
            Comparable min = Helper.getBound(baseCollection, pathPrefix + dbField.getType() + "." + dbField.getId(), DBConstant.LOWER_BOUND, projectFieldPath, projectIds);
            Comparable max = Helper.getBound(baseCollection, pathPrefix + dbField.getType() + "." + dbField.getId(), DBConstant.UPPER_BOUND, projectFieldPath, projectIds);
            mongoTemplate.save(new HashMap() {{ put("_id", projectId); put(DBConstant.FIELDNAME_MIN, min); put(DBConstant.FIELDNAME_MAX, max); }}, cacheCollectionName);
    }
    else if (!Double[].class.equals(type))
    	throw new UnsupportedOperationException("Unsupported field type: " + type);
    }

    /**
     * ensure index on all fields
     *
     * TODO (optimisation): make sure "sc.sampleCode" is indexed in Sequence
     *
     * @param mongoTemplate
     * @param fields
     */
    private static void ensureIndexes(MongoTemplate mongoTemplate, Map<Integer, DBField> fields) {
    	try {
	        fields.entrySet().stream().forEach(entry -> {
	            DBField dbField = entry.getValue();
	
	            Class entityClass = DBField.getModelClassFromTypeAlias(dbField.getEntityTypeAlias());
	            MongoCollection<Document> collection = mongoTemplate.getCollection(MongoTemplateManager.getMongoCollectionName(entityClass.equals(Assignment.class) ? AssignedSequence.class : entityClass));
	            BasicDBObject index = new BasicDBObject((entityClass.equals(Assignment.class) ? AssignedSequence.FIELDNAME_ASSIGNMENT + "." : "") + dbField.getType() + "." + dbField.getId(), 1);
	            collection.createIndex(index);
	        });
        }
        catch (MongoCommandException mce) {
        	if (!mce.getMessage().contains("add index fails, too many indexes for"))
        		throw mce;
        	LOG.warn("Unable to create additional index(es) in database " + mongoTemplate.getDb().getName() + " (too many indexes already exist)");
        }
    }

    /**
     * create the metagenomic project, and compute number of
     * sequence/samples/assigments imported
     *
     * @param mongoTemplate
     * @param projectId
     * @param project
     * @param projectName
     * @param projectDescription
     * @param authors
     * @param adress
     * @param seqDate
     * @param seqTech
     * @param assemblTech
     * @param isAvail
     * @param pub
     * @param extraInfo
     * @param access
     * @return
     * @throws Exception
     */
    private static Map<String, String> saveProject(MongoTemplate mongoTemplate, int projectId, String project, String projectName, String projectDescription, String authors, String adress, String seqDate, String seqTech, String assemblTech, boolean isAvail, String pub, String extraInfo, boolean access) throws Exception {
        MetagenomicsProject p = new MetagenomicsProject(projectId);
        p.setAcronym(project);
        p.setName(projectName);
        p.setDescription(projectDescription);
        p.setAuthors(authors);
        p.setContactInfo(adress);
        p.setSequencingTechnology(seqTech);
        if (!seqDate.isEmpty())
	        try {
	        	p.setSequencingDate(new SimpleDateFormat(Constant.DATE_FORMAT_YYYYMMDD).parse(seqDate));
	        }
	        catch (ParseException pe) {
	        	LOG.warn("Unable to parse " + project + " project's sequencing date: " + seqDate);
	        }
        p.setAssemblyMethod(assemblTech);
        p.setIsAvail(isAvail);
        p.setPublication(pub);
        p.setMetaInfo(extraInfo);
        p.setPublicProject(access);

        Map<String, String> result = new LinkedHashMap<>();

        Query query = new Query().addCriteria(Criteria.where(DBConstant.FIELDNAME_PROJECT).is(projectId));
        long count = mongoTemplate.count(query, Sample.class);
        result.put("samples", Long.toString(count));        
        
        query = new Query().addCriteria(Criteria.where("_id." + DBConstant.FIELDNAME_PROJECT).is(projectId));
        count = mongoTemplate.count(query, AssignedSequence.class);
        mongoTemplate.save(p);
        result.put("assigned sequences", Long.toString(count));
        
        List<BasicDBObject> pipeline = new ArrayList<>();
        pipeline.add(new BasicDBObject("$match", new BasicDBObject("_id." + DBConstant.FIELDNAME_PROJECT, projectId)));
        pipeline.add(new BasicDBObject("$unwind", "$" + AssignedSequence.FIELDNAME_ASSIGNMENT));
        pipeline.add(new BasicDBObject("$count", "count"));
        MongoCollection<Document> coll = mongoTemplate.getCollection(MongoTemplateManager.getMongoCollectionName(AssignedSequence.class));
        MongoCursor<Document> cursor = coll.aggregate(pipeline).allowDiskUse(true).iterator();
        result.put("assignments", Integer.toString(!cursor.hasNext() ? 0 : (int) cursor.next().get("count")));

        result.put("unassigned sequences", Long.toString(mongoTemplate.count(query, Sequence.class)));

        return result;
    }

    /**
     * copy fasta file on server, and create the .fai index file
     *
     * @param module
     * @param projectId
     * @param fis
     * @return 
     * @throws IOException
     */
    private String importFasta(String module, int projectId, InputStream fis) throws IOException {
        File importLocation = new File(appConfig.sequenceLocation() + File.separator + module);
        File outputFastaFile = importLocation.exists() || importLocation.mkdirs() ? new File(importLocation.getAbsolutePath() + File.separator + projectId + Sequence.NUCL_FASTA_EXT) : null;
        if (outputFastaFile != null) {
            Files.copy(fis, outputFastaFile.toPath(), StandardCopyOption.REPLACE_EXISTING);

            FastaSequenceIndex index = FastaSequenceIndexCreator.buildFromFasta(outputFastaFile.toPath());
            index.write(new File(importLocation + "/" + outputFastaFile.getName() + Sequence.NUCL_FAI_EXT).toPath());

            return outputFastaFile.getName();
        } else {
            LOG.error("Could not find nor create folder '" + importLocation.getAbsolutePath() + "'");
            return null;
        }
    }

    public static Double parseToDouble(String field) {
        try 
        {
        	return Double.valueOf(field.replaceAll(",", "."));
        }
        catch (NumberFormatException e)
        {
            return null;
        }
    }

    /**
	 * import all the samples from the sample file
	 *
	 * sample id is parsed as string position is a []Double collectDate is a
	 * Date all other fields are either String or Double
	 *
	 * @param mongoTemplate
	 * @param projectId
	 * @param fis
	 * @return headers of the file
	 * @throws Exception
	 */
	private static Map<String /* field name */, Comparable[] /* field id + type */> importSamples(String module, int projectId, InputStream fis) throws Exception {
		long before = System.currentTimeMillis();
	    Map<String, Comparable[]> fieldNameToIdAndTypeMap = new LinkedHashMap<>(); 
	    
	    int nImportedSampleCount = 0;

        BufferedReader br = new BufferedReader(new InputStreamReader(fis, Charset.forName("UTF-8")));
        // get the headers 
        String line = br.readLine();
        List<String> headerFields = ImportArchiveChecker.standardizeHeaders(Arrays.asList(line.split("\t")));
        
        // get the position of fields with a special type
        int idColumn = headerFields.indexOf(Sample.FIELDNAME_SAMPLE_CODE);
        int posColumn = headerFields.indexOf(Sample.FIELDNAME_COLLECT_GPS);
        int dateColumn = headerFields.indexOf(Sample.FIELDNAME_COLLECT_DATE);

        MongoTemplate mongoTemplate = MongoTemplateManager.get(module);
        Map<String, Comparable[]> staticIdFieldNameToIdAndTypeMap = new LinkedHashMap<>();
        DBField.getFieldsWishStaticId().stream().filter(dbf -> Sample.TYPE_ALIAS.equals(dbf.getEntityTypeAlias())).forEach(dbf -> staticIdFieldNameToIdAndTypeMap.put(dbf.getFieldName(), new Comparable[] {dbf.getId(), dbf.getType()}));

        for (String fieldName : headerFields)
        	if (!Sample.FIELDNAME_SAMPLE_CODE.equals(fieldName)) {
            	DBField dbField = mongoTemplate.findOne(new Query(new Criteria().andOperator(Criteria.where(DBField.FIELDNAME_ENTITY_TYPEALIAS).is(Sample.TYPE_ALIAS), Criteria.where(DBField.FIELDNAME_NAME).is(fieldName))), DBField.class);
            	if (dbField != null)
            		fieldNameToIdAndTypeMap.put(fieldName, new Comparable[] {dbField.getId(), dbField.getType()});
            	else if (sampleFieldsToPersist.containsKey(module) && sampleFieldsToPersist.get(module).containsKey(fieldName)) {	// another import seems to be running and is already dealing with that new field
            		fieldNameToIdAndTypeMap.put(fieldName, sampleFieldsToPersist.get(module).get(fieldName));
            		LOG.info(fieldName + " had been registered for creation");
            	}
            	else {
            		Comparable[] fieldInfo = staticIdFieldNameToIdAndTypeMap.get(fieldName);
            		if (fieldInfo == null) { // it's a free field: let's create an ID for it
		        		int nFieldId = AutoIncrementCounter.getNextSequence(mongoTemplate, MongoTemplateManager.getMongoCollectionName(DBField.class));
			        	fieldInfo = new Comparable[] {nFieldId, null};
	            	}
	        		fieldNameToIdAndTypeMap.put(fieldName, fieldInfo);
            		Map<String, Comparable[]> moduleSampleFieldsToPersist = sampleFieldsToPersist.get(module);
            		if (moduleSampleFieldsToPersist == null) {
            			moduleSampleFieldsToPersist = new HashMap<>();
            			sampleFieldsToPersist.put(module, moduleSampleFieldsToPersist);
            		}
            		moduleSampleFieldsToPersist.put(fieldName, fieldInfo);
            	}
        	}

        while ((line = br.readLine()) != null) {
            List<String> fields = Helper.split(line, '\t');
            
            String sampleId = fields.get(idColumn);
            Sample sample = mongoTemplate.findById(sampleId, Sample.class);
            if (sample == null)
            	sample = new Sample(sampleId); // it's a new one
            sample.getProjects().add(projectId);

            String positionField = fields.get(posColumn);
            if (!ImportArchiveChecker.EMPTY_FIELD_CODES.contains(positionField)) {
                sample.getGpsFields().put(DBField.gpsPosFieldId, new Double[]{
                    Double.parseDouble(positionField.split(",")[0]),
                    Double.parseDouble(positionField.split(",")[1])
                });
            }

            String dateField = fields.get(dateColumn);
            if (!ImportArchiveChecker.EMPTY_FIELD_CODES.contains(dateField))
            	sample.getDateFields().put(DBField.collDateFieldId, dateField);

            // Parse all other fields and dispatch them according to their types
            Map<Integer, String> stringFields = new HashMap<>();
            Map<Integer, Double> numberFields = new HashMap<>();

            for (int i=0; i<fields.size(); i++) {
                if (i == idColumn)
                    continue;

                String fieldName = headerFields.get(i);
            	Comparable[] idAndType = staticIdFieldNameToIdAndTypeMap.get(fieldName);
            	if (idAndType == null)
            		idAndType = fieldNameToIdAndTypeMap.get(fieldName);

            	if (i == posColumn && idAndType[1] == null)
            		idAndType[1] = DBConstant.GPS_TYPE;
            	else if (i == dateColumn && idAndType[1] == null)
            		idAndType[1] = DBConstant.DATE_TYPE;
            	else {
                    if (i == posColumn || i == dateColumn)
                        continue;
            		String field = fields.get(i).trim();
	                if (".".equals(field) || field.isEmpty())
	                    continue;
            	
	                // either parse as double or store as string
	                Double d;
	                if (!DBConstant.STRING_TYPE.equals(idAndType[1]) && (d = parseToDouble(field)) != null) {
	                	numberFields.put((int) idAndType[0], d);
	
	                    if (idAndType[1] == null)
	                    	idAndType[1] = DBConstant.DOUBLE_TYPE;
	                } else {
	                	stringFields.put((int) idAndType[0], field);
	
	                    if (idAndType[1] == null || idAndType[1] == DBConstant.DOUBLE_TYPE) {
	                    	if (idAndType[1] != null) {
	                    		List<Sample> samplesWithWrongFieldType = mongoTemplate.find(new Query(Criteria.where(DBConstant.DOUBLE_TYPE + "." + idAndType[0]).exists(true)), Sample.class);
		                    	LOG.info("Changing " + fieldName + " field type from double to string for " + samplesWithWrongFieldType.size() + " samples");
		                    	for (Sample s : samplesWithWrongFieldType) {
		                    		s.getStringFields().put((int) idAndType[0], Helper.formatDouble(s.getNumberFields().get(idAndType[0])));
		                    		s.getNumberFields().remove(idAndType[0]);
		                    		mongoTemplate.save(s);
		                    	}
	                    	}
	                    	idAndType[1] = DBConstant.STRING_TYPE;
	                    }
	                }
            	}
            }

            if (sample.getStringFields() == null)
            	sample.setStringFields(stringFields);
            else
            	sample.getStringFields().putAll(stringFields);
            if (sample.getNumberFields() == null)
            	sample.setNumberFields(numberFields);
            else
            	sample.getNumberFields().putAll(numberFields);
            

            // possible optimization: batch sample saving
            mongoTemplate.save(sample);
            nImportedSampleCount++;
        }

	    LOG.debug("importSamples took " + (System.currentTimeMillis() - before) + "ms for " + nImportedSampleCount + " samples");

	    return fieldNameToIdAndTypeMap;
	}

	/**
     * save assignments from assignment file.
     *
     * Currently, there is no limit to number of assignment for a single
     * sequence
     *
     * @param module
     * @param projectId
     * @param is
     * @param maxAccessionCountPerAssignment
     * @param indexedFasta
     * @param ProgressIndicator 
     * @throws Exception
     */
    private Map<String, Comparable[]> importAssignments(String module, int projectId, ZipInputStream is, int maxAccessionCountPerAssignment, IndexedFastaSequenceFile indexedFasta, ProgressIndicator progress) throws Exception {
    	long before = System.currentTimeMillis();
    	Map<String, Comparable[]> fieldNameToIdAndTypeMap = new LinkedHashMap<>();
    
        progress.setPercentageEnabled(false);
    	
    	int nProcessedRowCount = 0, nNumberOfRowsToSaveAtOnce = 1000;
    	
        BufferedReader br = new BufferedReader(new InputStreamReader(is, Charset.forName("UTF-8")), 65536);
        // get the headers 
        String line = br.readLine();
        List<String> headerFields = ImportArchiveChecker.standardizeHeaders(Arrays.asList(line.split("\t")));

        int qseqidColumn = headerFields.indexOf(Sequence.FIELDNAME_QSEQID);
        int sseqidColumn = headerFields.indexOf(Assignment.FIELDNAME_SSEQID);
        int taxidColumn = headerFields.indexOf(DBConstant.FIELDNAME_TAXON);
        
    	MongoTemplate mongoTemplate = MongoTemplateManager.get(module);
		Map<String, Comparable[]> moduleAssignmentFieldsToPersist = assignmentFieldsToPersist.get(module);
		if (moduleAssignmentFieldsToPersist == null) {
			moduleAssignmentFieldsToPersist = new HashMap<>();
			assignmentFieldsToPersist.put(module, moduleAssignmentFieldsToPersist);
		}

    	Map<String, Comparable[]> staticIdFieldNameToIdAndTypeMap = new LinkedHashMap<>();
        DBField.getFieldsWishStaticId().stream().filter(dbf -> AssignedSequence.FIELDNAME_ASSIGNMENT.equals(dbf.getEntityTypeAlias())).forEach(dbf -> staticIdFieldNameToIdAndTypeMap.put(dbf.getFieldName(), new Comparable[] {dbf.getId(), dbf.getType()}));
        
        for (Object aStaticIdRequiredField : CollectionUtils.intersection(fieldNameToIdAndTypeMap.keySet(), DBField.getRequiredFields().get(AssignedSequence.FIELDNAME_ASSIGNMENT)))
        	staticIdFieldNameToIdAndTypeMap.remove(aStaticIdRequiredField);	// don't add it manually because it's a required field so we will find it in the data anyway

        for (String fieldName : headerFields) {
        	DBField dbField = mongoTemplate.findOne(new Query(new Criteria().andOperator(Criteria.where(DBField.FIELDNAME_ENTITY_TYPEALIAS).is(AssignedSequence.FIELDNAME_ASSIGNMENT), Criteria.where(DBField.FIELDNAME_NAME).is(fieldName))), DBField.class);
        	if (dbField != null) {
        		fieldNameToIdAndTypeMap.put(fieldName, new Comparable[] {dbField.getId(), dbField.getType()});
        	}
        	else if (assignmentFieldsToPersist.containsKey(module) && assignmentFieldsToPersist.get(module).containsKey(fieldName)) {	// another import seems to be running and is already dealing with that new field
        		fieldNameToIdAndTypeMap.put(fieldName, assignmentFieldsToPersist.get(module).get(fieldName));
        		LOG.info(fieldName + " had been registered for creation");
        	}
        	else {
        		Comparable[] fieldInfo = staticIdFieldNameToIdAndTypeMap.get(fieldName);
        		if (fieldInfo == null) { // it's a free field: let's create an ID for it
	        		int nFieldId = AutoIncrementCounter.getNextSequence(mongoTemplate, MongoTemplateManager.getMongoCollectionName(DBField.class));
		        	fieldInfo = new Comparable[] {nFieldId, null};
            	}
        		fieldNameToIdAndTypeMap.put(fieldName, fieldInfo);
        		moduleAssignmentFieldsToPersist.put(fieldName, fieldInfo);
        	}

        	if (Assignment.FIELDNAME_SSEQID.equals(fieldName))
        		for (String key : staticIdFieldNameToIdAndTypeMap.keySet()) {
        			Comparable[] fieldInfo = staticIdFieldNameToIdAndTypeMap.get(key);
        			fieldNameToIdAndTypeMap.put(key, fieldInfo);
        			moduleAssignmentFieldsToPersist.put(key, fieldInfo);
        		}
    	}

        HashMap<String /*qseqid*/, AssignedSequence> seqsToUpdate = new HashMap<>(), seqsToInsert = new HashMap<>();

        AssignedSequence currentSeq = null;
        HashSet<String> encounteredSequences = new HashSet<>();
        Collection<String> accsMissingFromCache = new TreeSet<String>();
        while ((line = br.readLine()) != null) {
            nProcessedRowCount++;

            List<String> fields = Helper.split(line, '\t');
        	
            String qseqid = fields.get(qseqidColumn);

            if (currentSeq == null || !qseqid.equals(currentSeq.getId().getQseqid())) {
            	currentSeq = seqsToUpdate.get(qseqid);	// see whether we've got it in the current list of those to update
            	if (currentSeq == null)
            		currentSeq = seqsToInsert.get(qseqid);	// see whether we've got it in the current list of those to insert (happens when all assignments for a given sequence are not consecutive)
            	if (currentSeq == null)
            	{	// we need to read or create a Sequence instance because we don't have a reference to the correct object
	            	Sequence.SequenceId seqId = new Sequence.SequenceId(projectId, qseqid);
            		currentSeq = mongoTemplate.findById(seqId, AssignedSequence.class);
	
	            	if (currentSeq == null) {	// create it	
	                    currentSeq = new AssignedSequence(seqId);

	                    Map<Integer, Double> doubleFields = currentSeq.getDoubleFields();
	                    if (doubleFields == null) {
	                    	doubleFields = new HashMap<>();
	                    	currentSeq.setDoubleFields(doubleFields);
	                    }
	                    
	                    try {
	                    	doubleFields.put(DBField.seqLengthFieldId, (double) indexedFasta.getSequence(qseqid).length());
	                    }
	                    catch (SAMException se) {
	                    	throw new Exception("Error reading sequence length from fasta index: " + se.getMessage());
	                    }
	
	                    if (!seqsToInsert.containsKey(currentSeq))
	                    	seqsToInsert.put(qseqid, currentSeq);
	            	}
	            	else
	            		seqsToUpdate.put(qseqid, currentSeq);
            	}
            }

            Assignment assignment = new Assignment();
            for (int i=0; i<fields.size(); i++) {
                if (i == qseqidColumn)
                    continue;

                String fieldName = headerFields.get(i);
            	Comparable[] idAndType = staticIdFieldNameToIdAndTypeMap.get(fieldName);	// assignments contain field that are not provided but created at import time (taxon, hit_def)
            	if (idAndType == null)
            		idAndType = fieldNameToIdAndTypeMap.get(fieldName);

            	if (i == sseqidColumn && idAndType[1] == null)
            		idAndType[1] = DBConstant.STRING_ARRAY_TYPE;
            	else {
                    if (i == sseqidColumn)
                        continue;
            		String field = fields.get(i).trim();
	                if (".".equals(field) || field.isEmpty())
	                    continue;
	
                    // either parse as double or store as string
	                Double d;
	                if (!DBField.bestHitFieldName.equals(fieldName) /* force best-hit to be a String because we expect it as such */ && !DBConstant.STRING_TYPE.equals(idAndType[1]) && (d = parseToDouble(field)) != null) {
	                	assignment.putDoubleField((int) idAndType[0], d);
	                    if (idAndType[1] == null)
	                    	idAndType[1] = DBConstant.DOUBLE_TYPE;
	                } else {
	                	assignment.addStringField((int) idAndType[0], field);
	                    if (idAndType[1] == null)
	                    	idAndType[1] = DBConstant.STRING_TYPE;
	                }
            	}
            }

            if (sseqidColumn != -1)
	            accsMissingFromCache.addAll(addAccessionInfoToAssignment(Helper.split(fields.get(sseqidColumn), ',', maxAccessionCountPerAssignment), assignment)); // accessions should be found csv formatted
            else
            	assignment.putDoubleField(DBField.taxonFieldId, Double.parseDouble(fields.get(taxidColumn)));	// we should have a taxid if no sseqids were provided

            ((AssignedSequence) currentSeq).getAssignments().add(assignment);

            if (nProcessedRowCount % nNumberOfRowsToSaveAtOnce == 0) {
                if (!accsMissingFromCache.isEmpty()) {
                	LOG.warn("No accession cache found for " + StringUtils.join(accsMissingFromCache, ", "));
                	accsMissingFromCache.clear();
                }
                
            	final Collection<AssignedSequence> sequencesToUpdate = seqsToUpdate.values();
                Thread updateThread = sequencesToUpdate.size() == 0 ? null : new Thread() {
                    @Override
                    public void run() {
//                    	LOG.debug("updating " + sequencesToUpdate.stream().map(seq -> seq.getId().getQseqid()).collect(Collectors.toList()));
                    	for (Sequence seq : sequencesToUpdate)
                    		mongoTemplate.save(seq);
                    }
                };
                if (updateThread != null)
                	updateThread.start();
                
            	if (seqsToInsert.size() > 0) {
//                	LOG.debug("inserting " + seqsToInsert.values().stream().map(seq -> seq.getId().getQseqid()).collect(Collectors.toList()));
	            	mongoTemplate.insertAll(seqsToInsert.values());
	        	}
            	
                if (updateThread != null)
                	updateThread.join();

                seqsToInsert = new HashMap<>();
                seqsToUpdate = new HashMap<>();
                currentSeq = null;
            }
            encounteredSequences.add(qseqid);
            if (encounteredSequences.size() % 1000 == 0)
            	progress.setCurrentStepProgress(nProcessedRowCount);
        }
        
    	if (seqsToInsert.size() > 0)
        	mongoTemplate.insertAll(seqsToInsert.values());

    	if (seqsToUpdate.size() > 0)
        	for (Sequence seq : seqsToUpdate.values())
	        	mongoTemplate.save(seq);

        // cleanup sequences that may have been provided as both assigned and unassigned
        HashSet<String> subSet = new HashSet<>();
        Iterator<String> it = encounteredSequences.iterator();
        int nRemovedSeqCount = 0;
    	if (it.hasNext())
	        do {
	        	subSet.add(it.next());
	        	if (subSet.size() % 2000 == 0 || !it.hasNext()) {
	        		nRemovedSeqCount += mongoTemplate.remove(new Query(Criteria.where("id." + Sequence.FIELDNAME_QSEQID).in(subSet)), Sequence.class).getDeletedCount();
	        		subSet.clear();
	        	}
	        } while (it.hasNext());
    	if (nRemovedSeqCount > 0)
    		LOG.info("Removed " + nRemovedSeqCount + " sequences from assigned collection because they were actually assigned");
    	
        LOG.debug("importAssignments took " + (System.currentTimeMillis() - before)/1000 + "s for " + nProcessedRowCount + " records");
        return fieldNameToIdAndTypeMap;
    }

    private void importSequences(String module, int projectId, ZipInputStream is, int maxAccessionCountPerAssignment, IndexedFastaSequenceFile indexedFasta, ProgressIndicator progress) throws IOException, InterruptedException {
        progress.setPercentageEnabled(true);
    	progress.addStep("Processing lines in sequence composition file");
    	progress.moveToNextStep();

    	MongoTemplate mongoTemplate = MongoTemplateManager.get(module);
    	int nNumberOfRowsToSaveAtOnce = 1000;
    	BufferedReader br = new BufferedReader(new InputStreamReader(is, Charset.forName("UTF-8")), 65536);
    	
        // get the headers 
        String line = br.readLine();
        List<String> headerFields = Arrays.asList(line.split("\t"));
        int qseqidColumn = headerFields.indexOf(Sequence.FIELDNAME_QSEQID);

        int nSeqCount = 0;
    	while ((indexedFasta.nextSequence()) != null)
    		nSeqCount++;

    	AtomicInteger nEncounteredSeqCount = new AtomicInteger(0);
        Collection<Sequence> seqsToInsert = new HashSet<>();
    	BulkOperations bulkOperations = mongoTemplate.bulkOps(BulkOperations.BulkMode.UNORDERED, AssignedSequence.class);
    	int nBulkOpCount = 0;
    	long nUnassignedCount = 0;
        
        while ((line = br.readLine()) != null) {
            List<String> fields = Helper.split(line, '\t');
	
            // generate sample composition object
            List<SampleReadCount> sampleComposition = new ArrayList<>();

	        for (int index=0; index<headerFields.size(); index++)
	        	if (index != qseqidColumn) {
		            String field = fields.get(index);
		            // only store sample code contributing to the sequence, i.e. where field value > 0
		            if (!field.isEmpty() && !"0".equals(field)) {
		                int count = Integer.parseInt(field);
		                String sampleCode = headerFields.get(index);
		                sampleComposition.add(new SampleReadCount(sampleCode, count));
		            }
	        	}

	        SequenceId seqId = new Sequence.SequenceId(projectId, fields.get(qseqidColumn));
	        if (mongoTemplate.exists(new Query(Criteria.where("_id").is(seqId)), AssignedSequence.class)) {	// it's an assigned sequence
	        	bulkOperations.updateOne(new Query(Criteria.where("_id").is(seqId)), new Update().set(Sequence.FIELDNAME_SAMPLE_COMPOSITION, sampleComposition));
	        	nBulkOpCount++;
	        }
	        else {	// it's an unassigned sequence
	        	Sequence currentSeq = new Sequence(seqId);
	        	currentSeq.setSampleComposition(sampleComposition);
	        	seqsToInsert.add(currentSeq);
	        }

            if (seqsToInsert.size() == nNumberOfRowsToSaveAtOnce || nBulkOpCount == nNumberOfRowsToSaveAtOnce) {
            	final BulkOperations finalBulkOperations = bulkOperations; 
                Thread updateThread = nBulkOpCount == 0 ? null : new Thread() {
                    @Override
                    public void run() {
//                    	LOG.debug("updating " + sequencesToUpdate.stream().map(seq -> seq.getId().getQseqid()).collect(Collectors.toList()));
                    	BulkWriteResult bwr = finalBulkOperations.execute();
                    	nEncounteredSeqCount.addAndGet(bwr.getModifiedCount());
                    	LOG.debug(bwr.getModifiedCount() + " seqs updated with composition");
                    }
                };
                if (updateThread != null)
                	updateThread.start();
                
            	if (seqsToInsert.size() > 0)
	            	try {
//            			LOG.debug("inserting " + seqsToInsert.stream().map(seq -> seq.getId().getQseqid()).collect(Collectors.toList()));
	            		nEncounteredSeqCount.addAndGet(mongoTemplate.insertAll(seqsToInsert).size());
	            		nUnassignedCount += seqsToInsert.size();
	            		LOG.debug(seqsToInsert.size() + " unassigned seqs inserted");
	            	}
            		catch (DuplicateKeyException dke) {	// at least one was already in there
	            		Collection<String> existingSeqIDs = mongoTemplate.findDistinct(new Query(Criteria.where("_id").in(seqsToInsert.stream().map(seq -> seq.getId()).collect(Collectors.toList()))), "_id." + Sequence.FIELDNAME_QSEQID, Sequence.class, String.class);
	            		String[] splitMsg = dke.getMessage().split(" _id: ");
            			LOG.warn("Unassigned sequence provided several times in sequence composition file: " + splitMsg[splitMsg.length - 1].substring(0, 1 + splitMsg[splitMsg.length - 1].indexOf("}")));
            			seqsToInsert = seqsToInsert.stream().filter(seq -> !existingSeqIDs.contains(seq.getId().getQseqid())).collect(Collectors.toList());
            			
            			long nUnassignedCountInDB = mongoTemplate.count(new Query(Criteria.where("_id." + DBConstant.FIELDNAME_PROJECT).is(projectId)), Sequence.class);	// find out how many were inserted before it failed
            			int nUpdatedInsertCount = mongoTemplate.insertAll(seqsToInsert).size() + (int) (nUnassignedCountInDB - nUnassignedCount);
	            		nEncounteredSeqCount.addAndGet(nUpdatedInsertCount);
	            		nUnassignedCount += nUpdatedInsertCount;
	            		LOG.debug(nUpdatedInsertCount + " unassigned seqs inserted");
            		}
            	
                if (updateThread != null)
                	updateThread.join();

                seqsToInsert.clear();
                bulkOperations = mongoTemplate.bulkOps(BulkOperations.BulkMode.UNORDERED, AssignedSequence.class);
                nBulkOpCount = 0;
                
            	progress.setCurrentStepProgress(nEncounteredSeqCount.get() * 100 / nSeqCount);
            }
        }
        
    	if (seqsToInsert.size() > 0)
        	try {
//    			LOG.debug("inserting " + seqsToInsert.stream().map(seq -> seq.getId().getQseqid()).collect(Collectors.toList()));
        		nEncounteredSeqCount.addAndGet(mongoTemplate.insertAll(seqsToInsert).size());
        		nUnassignedCount += seqsToInsert.size();
        		LOG.debug(seqsToInsert.size() + " unassigned seqs inserted");
        	}
    		catch (DuplicateKeyException dke) {	// at least one was already in there
        		Collection<String> existingSeqIDs = mongoTemplate.findDistinct(new Query(Criteria.where("_id").in(seqsToInsert.stream().map(seq -> seq.getId()).collect(Collectors.toList()))), "_id." + Sequence.FIELDNAME_QSEQID, Sequence.class, String.class);
        		String[] splitMsg = dke.getMessage().split(" _id: ");
    			LOG.warn("Unassigned sequence provided several times in sequence composition file: " + splitMsg[splitMsg.length - 1].substring(0, 1 + splitMsg[splitMsg.length - 1].indexOf("}")));
    			seqsToInsert = seqsToInsert.stream().filter(seq -> !existingSeqIDs.contains(seq.getId().getQseqid())).collect(Collectors.toList());
    			
    			long nUnassignedCountInDB = mongoTemplate.count(new Query(Criteria.where("_id." + DBConstant.FIELDNAME_PROJECT).is(projectId)), Sequence.class);	// find out how many were inserted before it failed
    			int nUpdatedInsertCount = mongoTemplate.insertAll(seqsToInsert).size() + (int) (nUnassignedCountInDB - nUnassignedCount);
        		nEncounteredSeqCount.addAndGet(nUpdatedInsertCount);
        		nUnassignedCount += nUpdatedInsertCount;
        		LOG.debug(nUpdatedInsertCount + " unassigned seqs inserted");
    		}

    	if (nBulkOpCount > 0) {
//        	LOG.debug("updating " + seqsToUpdate.values().stream().map(seq -> seq.getId().getQseqid()).collect(Collectors.toList()));
    		BulkWriteResult bwr = bulkOperations.execute();
    		nEncounteredSeqCount.addAndGet(bwr.getModifiedCount());
        	LOG.debug(bwr.getModifiedCount() + " seqs updated with composition");
    	}
    	progress.setCurrentStepProgress(nEncounteredSeqCount.get() * 100 / nSeqCount);
	}

    private void updateAccessionCache(InputStream fis, int nMaxAccessionsPerAssignment, ProgressIndicator progress) throws Exception {
    	progress.addStep("Collecting accession IDs for each line in assignment file");
    	progress.moveToNextStep();
    	progress.setPercentageEnabled(false);

        Collection<String> accColl = new HashSet<>();

        BufferedReader br = new BufferedReader(new InputStreamReader(fis, Charset.forName("UTF-8")), 16384);

        String line = br.readLine();
        int nLineCount = 0;
        List<String> headers = Helper.split(line, '\t', -1), stdHeaders = ImportArchiveChecker.standardizeHeaders(headers);

        int sseqidColumnIndex = stdHeaders.indexOf(Assignment.FIELDNAME_SSEQID);
        if (sseqidColumnIndex == -1) {
        	LOG.info("Currently imported project contains no accession information");
        	return;
        }

        while ((line = br.readLine()) != null) {
        	String sseqid = Helper.getNthColumn(line, '\t', sseqidColumnIndex);
            if (!ImportArchiveChecker.EMPTY_FIELD_CODES.contains(sseqid)) // an assignment can have several comma separated accessions
            	accColl.addAll(Helper.split(sseqid, ',', nMaxAccessionsPerAssignment));
            progress.setCurrentStepProgress(++nLineCount);
        }
        
        List<String>[] accsByType = Accession.separateNuclFromProtIDs(accColl.stream().map(accId -> -1 != accId.indexOf(".") ? accId.substring(0, accId.indexOf(".")) : accId).collect(Collectors.toCollection(TreeSet::new /*removes duplicates that may appear when removing the version number*/)), false);

        if (!accsByType[0].isEmpty() || !accsByType[1].isEmpty())
            accessionImport.fetchRemoteAccessionInfo(accsByType[0], accsByType[1], progress);
	}

    /**
     * Create a partial fai index from an exhaustive one
     * 
     * @param inputFile
     * @param outputFile
     * @param sequencesToKeep (HashSet preferred for best performance)
     */
    public static void createPartialFai(File inputFaiFile, File outputFaiFile, Collection<String> sequencesToKeep) throws Exception 
    { 
      BufferedReader in = new BufferedReader(new FileReader(inputFaiFile));
      BufferedWriter out = new BufferedWriter(new FileWriter(outputFaiFile));
      String line, id;
      while ((line = in.readLine()) != null){
    	  id = line.split("\\t")[0];
    	  if (sequencesToKeep.contains(id)) {
    		  sequencesToKeep.remove(id);
    		  out.write(line+"\n");}
    	  }
      in.close();
      out.close();
    }

	/**
     * Fetch info from accessions in the database. If several accessions, assign taxonomy to last common ancestor
     * @return list of accession IDs that were not found in the cache
     */
    public List<String> addAccessionInfoToAssignment(List<String> accessions, Assignment assignment) {
        List<String> notInCache = new ArrayList<>();
    	List<String> nonEmptyAccessions = accessions.stream().filter(acc -> !acc.isEmpty()).collect(Collectors.toList());
    	if (!nonEmptyAccessions.isEmpty()) {
	    	List<AccessionId> accIdList = Accession.buildAccessionIDsFromPrefixedString(nonEmptyAccessions.stream().map(id -> id.replaceAll("\\..*", "")).collect(Collectors.toList()));
	        String[] hitDefs = new String[nonEmptyAccessions.size()];
	        Map<Integer, Integer> taxIdCounts = new HashMap<>();
	        boolean fHitDefArrayEmpty = true;
	        for (int i=0; i<accIdList.size(); i++) {
		        AccessionId accID = accIdList.get(i);
		        Accession accCache = MongoTemplateManager.getCommonsTemplate().findById(accID, Accession.class);
		        if (accCache != null) {
		        	if (accCache.getHd() != null) {
		        		fHitDefArrayEmpty = false;
		        		hitDefs[i] = accCache.getHd();
		        	}
		        	if (accCache.getTx() != null) {
			        	Integer count = taxIdCounts.get(accCache.getTx());
			        	taxIdCounts.put(accCache.getTx(), count == null ? 1 : ++count);
		        	}
		        }
		        else
		        	notInCache.add(accID.toString());
		    }
	
	       	assignment.addStringArrayField(DBField.sseqIdFieldId, nonEmptyAccessions.toArray(new String[nonEmptyAccessions.size()]));
	        if (!fHitDefArrayEmpty)
	        	assignment.addStringArrayField(DBField.hitDefFieldId, hitDefs);   
	        if (taxIdCounts.size() == 1) {
	        	assignment.putDoubleField(DBField.taxonFieldId, (double) taxIdCounts.keySet().iterator().next());
	        	return notInCache;
	        }
	
	        // we have several accessions so we need to find their FCA
	    	String[] csvTaxaAncestry = TaxonomyNode.getTaxaAncestry(taxIdCounts.keySet(), false, false, ",").values().toArray(new String[0]);
	    	List<Integer[]> taxaAncestry = new ArrayList<>();	// we use a list rather than an int[][] because some accessions have no related tax id so sizes may differ
	    	for (String csvTaxonomy : csvTaxaAncestry) {
	    		Integer[] taxoAsIntArray = Helper.csvToIntegerArray(csvTaxonomy);
	    		for (int j=0; j<taxIdCounts.get(taxoAsIntArray[taxoAsIntArray.length - 1]); j++)
	    			taxaAncestry.add(taxoAsIntArray);	// repeat as many times as it's present in the original list
	    	}
	
	    	int nFirstCommonAncestor = TaxonomyNode.calculateFirstCommonAncestor(taxaAncestry.toArray(new Integer[taxaAncestry.size()][]));
	    	if (nFirstCommonAncestor > 0) {
	    		assignment.putDoubleField(DBField.taxonFieldId, (double) nFirstCommonAncestor);
	        	return notInCache;
	    	}
    	}
    	return notInCache;
    }

    static public Collection<Integer> getCurrentlyImportedProjectsForModule(String sModule) {
    	return currentlyImportedProjects.get(sModule);
    }
}