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
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipInputStream;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.bson.Document;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.factory.BeanDefinitionStoreException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.support.GenericXmlApplicationContext;
import org.springframework.core.io.Resource;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.mongodb.BulkOperationException;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationOptions;
import org.springframework.data.mongodb.core.aggregation.Fields;
import org.springframework.data.mongodb.core.aggregation.LimitOperation;
import org.springframework.data.mongodb.core.aggregation.MatchOperation;
import org.springframework.data.mongodb.core.aggregation.ProjectionOperation;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Component;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.client.DistinctIterable;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.result.UpdateResult;

import fr.cirad.metaxplor.model.Accession;
import fr.cirad.metaxplor.model.Accession.AccessionId;
import fr.cirad.metaxplor.model.AssignedSequence;
import fr.cirad.metaxplor.model.Assignment;
import fr.cirad.metaxplor.model.DBField;
import fr.cirad.metaxplor.model.Taxon;
import fr.cirad.tools.AppConfig;
import fr.cirad.tools.Helper;
import fr.cirad.tools.ProgressIndicator;
import fr.cirad.tools.mongo.DBConstant;
import fr.cirad.tools.mongo.MongoTemplateManager;

/**
 * @author petel, sempere, abbe
 */
@Component
public class AccessionImport {

    private static final Logger LOG = Logger.getLogger(AccessionImport.class);
    
    @Autowired private AppConfig appConfig;
    
    private static final int ACCESSION_QUERY_BATCH_SIZE = 50;
    private static final int MAX_RETRIES = 3;
    
    static private long previousCallTime = 0;
    
    /**
     * The main method is used for creating the initial bunch of cached accessions, taken from SILVA.
     * At the time of writing, it is advised to invoke it twice with the following arguments
     * (i) https://www.arb-silva.de/fileadmin/silva_databases/release_132/Exports/taxonomy/taxmap_embl_ssu_ref_nr99_132.txt.gz
     * (ii) https://www.arb-silva.de/fileadmin/silva_databases/release_132/Exports/taxonomy/taxmap_embl_lsu_ref_132.txt.gz
     */
    public static void main(String[] args) throws IOException {
        if (args.length < 1)
            throw new IOException("You must pass 1 parameter as argument: URI to SILVA taxmap gz file");	// example argument: https://www.arb-silva.de/fileadmin/silva_databases/current/Exports/taxonomy/taxmap_embl_lsu_ref_132.txt.gz

        GenericXmlApplicationContext ctx = null;
        Scanner scanner = null;
        try {
            MongoTemplate mongoTemplate = MongoTemplateManager.getCommonsTemplate();
            if (mongoTemplate == null) {	// we are probably being invoked offline
                try {
                    ctx = new GenericXmlApplicationContext("applicationContext-data.xml");
                } catch (BeanDefinitionStoreException fnfe) {
                    LOG.warn("Unable to find applicationContext-data.xml. Now looking for applicationContext.xml", fnfe);
                    ctx = new GenericXmlApplicationContext("applicationContext.xml");
                }

                MongoTemplateManager.initialize(ctx);
                mongoTemplate = MongoTemplateManager.getCommonsTemplate();
                if (mongoTemplate == null) {
                    throw new IOException("DATASOURCE 'metaxplor_commons' is not supported!");
                }
            }

            InputStream is = Helper.openStreamFromUrl(args[0]);
            if (args[0].toLowerCase().endsWith(".gz"))
            	is = new GZIPInputStream(is);
            
            scanner = new Scanner(is);
            List<Accession> newAccessions = new ArrayList<>();
            List<Accession> existingAccessions = new ArrayList<>();
            int nUpdated = 0;
            HashSet<String> addedAccessions = new HashSet<>();
            do {
            	String[] sLine = scanner.nextLine().split("\t");
            	
            	if (addedAccessions.contains(sLine[0]))
            		continue;
            	
            	AccessionId accId = new AccessionId(AccessionId.NUCLEOTIDE_TYPE, sLine[0]);
            	int taxId = Integer.parseInt(sLine[5].trim());
            	Accession acc = mongoTemplate.findById(accId, Accession.class);
            	if (acc == null) {
            		acc = new Accession(accId, taxId, null);
            		newAccessions.add(acc);
            		addedAccessions.add(sLine[0]);
            	}
            	else if (acc.getTx() == null || acc.getTx() != taxId) {
            		acc.setTx(taxId);
            		existingAccessions.add(acc);
            	}

            	if (newAccessions.size() + existingAccessions.size() == 1000 || !scanner.hasNextLine()) {	// persist to DB
            		mongoTemplate.insertAll(newAccessions);            		
            		newAccessions.clear();
            		
            		for (Accession accession : existingAccessions)
            			mongoTemplate.save(accession);
            		nUpdated += existingAccessions.size();
            		existingAccessions.clear();
            	}
            	int nCompleted = addedAccessions.size() + nUpdated;
            	if (nCompleted > 0 && nCompleted % 10000 == 0)
            		LOG.info(nCompleted + " records processed");
            }
            while (scanner.hasNextLine());

            LOG.info(addedAccessions.size() + " inserts ; " + nUpdated + " updates");
        	
          } finally {
	          if (ctx != null)
	              ctx.close();
	          if (scanner != null)
	        	  scanner.close();
	      }
	}

    private synchronized void waitUntilAllowedToSendRequest(boolean fWithApiKey) throws InterruptedException {

    	while (System.currentTimeMillis() - previousCallTime < (!fWithApiKey ? 400 : 120))	// we're not allowed to send more than 3 queries per second without an API key
    		Thread.sleep(50);
	
    	previousCallTime = System.currentTimeMillis();
    }

    private HttpResponse<String> doGetRequest(String url, boolean fWithApiKey) throws InterruptedException, UnirestException {
    	waitUntilAllowedToSendRequest(fWithApiKey);
    	return Unirest.get(url).asString();
    }

    /**
     * fetch info from NCBI according to accession number
     *
     * @param accIdColl
     * @param fProtein
     * @param apiKey 
     * @param fTryingAgain means we're not being called during a data import but in the scope of an attempt to fetch accessions that could not be obtained at import time
     * @return successfully fetched accessions, with prefix
     * @throws InterruptedException 
     */
    private Collection<String> getSeqInfoFromNCBI(Collection<String> accIdColl, boolean fProtein, String apiKey, boolean fTryingAgain) throws Exception {
    	BulkOperations bulkOperations = MongoTemplateManager.getCommonsTemplate().bulkOps(BulkOperations.BulkMode.UNORDERED, Accession.class);
    	boolean fBulkOpsEmpty = true;
    	HashMap<Integer, ArrayList<Accession>> accessionsToAddByTaxId = new HashMap<>(), accessionsWithUnknownTaxa = new HashMap<>();
    	
    	Collection<String> updatedAccs = new ArrayList<>();
        String url = appConfig.getEUtilsBaseUrl();

        HttpResponse<String> resp = doGetRequest(url + "esummary.fcgi?db=" + (fProtein ? "protein" : "nucleotide") + "&retmode=json" + (apiKey != null && !apiKey.isEmpty() ? "&api_key=" + apiKey : "") + "&id=" + StringUtils.join(accIdColl, ","), apiKey != null && !apiKey.isEmpty());
        if (resp.getStatus() != 200)
        	throw new Exception(resp.getStatusText());

        BasicDBObject jsonObject = (BasicDBObject) BasicDBObject.parse(resp.getBody());	// parse using MongoDB API because it is less strict and goes around the fact that response may contain multiple "error" fields

        JSONObject jsonBody = new JSONObject(jsonObject.toString());
        JSONObject results = jsonBody.getJSONObject("result");
        JSONArray resultList = results.getJSONArray("uids");

        for (int i=0; i<resultList.length(); i++) {
            JSONObject object = results.getJSONObject(resultList.getString(i));

            if (!object.has("taxid")) {
            	LOG.warn("No taxid found for accession " + object.getString("caption") + ": " + object.getString("comment"));
            	continue;
            }

            try {
            	int taxId = -1;
                if (!"".equals(object.get("taxid")))
                	taxId = object.getInt("taxid");

                String hitDefinition = null;
                String accession = object.getString("caption");

                if (object.getString("title") != null) {
                    hitDefinition = object.getString("title");
                    if (hitDefinition.length() > 1024) {
                    	hitDefinition = hitDefinition.substring(0, 1000) + "...";
                    	LOG.info("Truncated hit definition for accession " + accession + " because it exceeded 1024 chars");
                    }
                }

                Accession acc = new Accession(new AccessionId(fProtein ? AccessionId.PROTEIN_TYPE : AccessionId.NUCLEOTIDE_TYPE, accession), taxId == -1 ? null : taxId, hitDefinition);
                ArrayList<Accession> accsForTaxId = accessionsToAddByTaxId.get(taxId);
                if (accsForTaxId == null) {
                	accsForTaxId = new ArrayList<>();
                	accessionsToAddByTaxId.put(taxId, accsForTaxId);
                }

                accsForTaxId.add(acc);
                
                if (taxId != -1) {
	                if (!MongoTemplateManager.getCommonsTemplate().exists(new Query(Criteria.where("_id").is(taxId)), Taxon.class)) {	// keep aside accessions with an unknown taxid
		                accsForTaxId = accessionsWithUnknownTaxa.get(taxId);
	                    if (accsForTaxId == null) {
	                    	accsForTaxId = new ArrayList<>();
	                    	accessionsWithUnknownTaxa.put(taxId, accsForTaxId);
	                    }

	                    accsForTaxId.add(acc);
	                }
	                else {
	                	updatedAccs.add((fProtein ? Accession.ID_PROTEIN_PREFIX : Accession.ID_NUCLEOTIDE_PREFIX) + accession);
	                	if (!accIdColl.remove(object.getString("caption")))
		                    LOG.debug("couldn't remove " + object.getString("caption") + " from " + accIdColl + " (database " + (fProtein ? "protein" : "nucleotide") + ")");	                	
	                }
                }
            }
            catch (JSONException je) {
                LOG.error(je);
            }
        }

        if (accessionsWithUnknownTaxa.size() > 0) {
        	// check whether unknown taxa were merged into others
    		HttpResponse<String> bodyAsText = doGetRequest(url + "esummary.fcgi?db=taxonomy&retmode=json" + (apiKey != null && !apiKey.isEmpty() ? "&api_key=" + apiKey : "") + "&id=" + StringUtils.join(accessionsWithUnknownTaxa.keySet().toArray(new Integer[accessionsWithUnknownTaxa.size()]), ","), apiKey != null && !apiKey.isEmpty());
    		JSONObject taxResp = new JSONObject(((BasicDBObject) BasicDBObject.parse(bodyAsText.getBody())).toString());	// parse using MongoDB API because it is less strict and goes around the fact that response may contain multiple "error" fields
            if (!taxResp.has("result"))
            	throw new Exception(taxResp.has("error") ? taxResp.getString("error") : bodyAsText.getBody());
            else {
	            JSONObject resultObj = taxResp.getJSONObject("result");
	            int nMergedTaxaCount = 0;
	            for (Integer anUnknownTaxId : accessionsWithUnknownTaxa.keySet())
		            try
		            {
		            	int taxId = Integer.parseInt(resultObj.getJSONObject("" + anUnknownTaxId).get("akataxid").toString());
		            	nMergedTaxaCount++;
		            	for (Accession acc : accessionsWithUnknownTaxa.get(anUnknownTaxId))
		            		acc.setTx(taxId);
		            }
		            catch (Exception ignored)
		            {}
	            LOG.info(nMergedTaxaCount + " taxa with merged taxid, " + (accessionsWithUnknownTaxa.size() - nMergedTaxaCount) + " could not be merged");
            }
        }

        // persist the precious information we found
        for (int taxId : accessionsToAddByTaxId.keySet()) {
        	List<Accession> accsToAdd = accessionsToAddByTaxId.get(taxId);
        	List<AccessionId> accIDs = accsToAdd.stream().map(acc -> acc.getId()).collect(Collectors.toList());
        	if (taxId == -1) {
        		LOG.info("No taxon id found for " + (fProtein ? "protein" : "nucleotide") + " accessions " + StringUtils.join(accIDs, ", ") + ": unclassified?");
        		accsToAdd.stream().forEach(acc -> { acc.setTx(Taxon.UNIDENTIFIED_ORGANISM_TAXID); });	// mark as unidentified because we won't be able to grab more info than this
        	}
            if (!fTryingAgain)
            	bulkOperations.insert(accsToAdd);
            else
            	for (Accession acc : accsToAdd) {
            		Query query = new Query(new Criteria().andOperator(Criteria.where("_id." + DBField.FIELDNAME_TYPE).is(String.valueOf(fProtein ? Accession.AccessionId.PROTEIN_TYPE : Accession.AccessionId.NUCLEOTIDE_TYPE)), Criteria.where("_id." + Assignment.FIELDNAME_SSEQID).is(acc.getId().getSseqid())));
	            	bulkOperations.updateMulti(query, new Update().set(Accession.FIELDNAME_NCBI_TAXID, taxId).set(Accession.FIELDNAME_HIT_DEFINITION, acc.getHd()));
            	}
        	fBulkOpsEmpty = false;
        }
        if (!fBulkOpsEmpty)
        	try {
        		com.mongodb.bulk.BulkWriteResult wr = bulkOperations.execute();
        		if (wr.getInsertedCount() > 0 || wr.getModifiedCount() > 0)
        			LOG.debug(wr.getInsertedCount() + " accessions inserted, " + wr.getModifiedCount() + " accessions updated");
        	}
        	catch (BulkOperationException boe) {
        		if (!boe.getMessage().toLowerCase().contains("duplicate key"))	// this may happen when several imports are running at the same time, should not be a problem as long as BulkMode is set to UNORDERED
        			throw boe;
        	}
		
        return updatedAccs;
    }

    /**
     * Fetch info on accessions from "nucleotide" and "protein" databases
     *
     * @param nuclAccessions
     * @param protAccessions 
     * @param progress if null we consider we're not being called during a data import but in the scope of an attempt to fetch accessions that could not be obtained at import time
     * @return fetched accession IDs
     * @throws Exception 
     */
    public List<String> fetchRemoteAccessionInfo(List<String> nuclAccessions, List<String> protAccessions, ProgressIndicator progress) throws Exception {
        Collection<String>[] accLists = new Collection[] {nuclAccessions, protAccessions};
    	LOG.debug("fetchRemoteAccessionInfo called for " + (nuclAccessions.size() + protAccessions.size()) + " accessions"/* + " -> NUCL:" + nuclAccessions + ", PROT:" + protAccessions*/);

    	List<String> result = new ArrayList<>();
        if (progress != null)
        	 // it's an import so we won't try and get info for accessions we already know
        	for (int j=0; j<accLists.length; j++) {
            	final boolean fProtein = accLists[j] == protAccessions;
		        List<String> existingAccessions = new ArrayList<>();
		    	HashSet<String> subSet = new HashSet<>();
		    	Iterator<String> accIt = accLists[j].iterator();
		    	if (accIt.hasNext())
			        do {
			        	subSet.add(accIt.next());
			        	if (subSet.size() % 5000 == 0 || !accIt.hasNext()) {
			        		existingAccessions.addAll(MongoTemplateManager.getCommonsTemplate().findDistinct(new Query(new Criteria().andOperator(Criteria.where("_id." + DBField.FIELDNAME_TYPE).is(String.valueOf(fProtein ? AccessionId.PROTEIN_TYPE : AccessionId.NUCLEOTIDE_TYPE)), Criteria.where("_id." + Assignment.FIELDNAME_SSEQID).in(subSet))), "_id." + Assignment.FIELDNAME_SSEQID, MongoTemplateManager.getCommonsTemplate().getCollectionName(Accession.class), String.class));
			        		subSet.clear();
			        	}
			        } while (accIt.hasNext());
		    	if (!existingAccessions.isEmpty())
		    		accLists[j].removeAll(existingAccessions);
	    	}
        int nTotalAccCount = nuclAccessions.size() + protAccessions.size();
        if (nTotalAccCount == 0)
        	return result;

        LOG.info(nTotalAccCount + " new accessions actually need to be fetched (" + nuclAccessions.size() + " nucl & " + protAccessions.size() + " prot)");
        
        if (appConfig.getEUtilsBaseUrl() == null)
        	throw new Exception("eutils_base_url not defined in config.properties!");

        String apiKey = appConfig.getNcbiApiKey();
        boolean fGotApiKey = apiKey != null && !apiKey.isEmpty();
        
        if (progress != null) {
        	String sProgress = "Retrieving accession information from NCBI with" + (fGotApiKey ? "" : "out") + " an API key";
        	LOG.info(sProgress);
	    	progress.addStep(sProgress);
	    	progress.moveToNextStep();
	    	progress.setPercentageEnabled(true);
        }
        
    	Collection<String> accIdList = new HashSet<>();

        // fetch accessions by batch
        final ArrayList<Thread> threadsToWaitFor = new ArrayList<>();
        int nNConcurrentThreads = !fGotApiKey ? 3 : 10;
        AtomicInteger nTotalFailureCount = new AtomicInteger(0), nOverallProcessedAccCount = new AtomicInteger(0);
        for (int j=0; j<accLists.length; j++) {
        	if (accLists[j].isEmpty())
        		continue;

        	final boolean fProt = accLists[j] == protAccessions;
            int accIndex = 0, chunkIndex = -1;
            LOG.debug("Starting to deal with " + (fProt ? "protein" : "nucleotide") + " accessions");
	        for (String accession : accLists[j]) {
	            accIdList.add(accession);
	            nOverallProcessedAccCount.incrementAndGet();
	            accIndex++;
	            if (accIndex % ACCESSION_QUERY_BATCH_SIZE == 0 || accIndex == accLists[j].size())
	            {
	            	final Collection<String> finalAccList = new HashSet<>(accIdList);
	            	++chunkIndex;
	                Thread queryThread = new Thread() {
	                    @Override
	                    public void run() {
	            			int nFailureCount = 0;
	                    	while (nFailureCount < MAX_RETRIES)
		                    	try {
		                    		if (nFailureCount > 0)
		                    			LOG.debug((fProt ? "protein" : "nucleotide") + " retry " + finalAccList);
		                    		result.addAll(getSeqInfoFromNCBI(finalAccList /* will shrink when some are obtained */, fProt, apiKey, progress == null));
									break;
								}
		                    	catch (Exception e) {
									LOG.debug("NCBI service call failed: " + e.getMessage());
									nFailureCount++;
								}
	                    	if (!finalAccList.isEmpty()) { // some accessions could not be obtained from the WS
		                    	if (nFailureCount == MAX_RETRIES) {	// unable to call WS at all: tax id shall be set to null so we know can we retrieve it later on
		                    		LOG.warn("After " + nFailureCount + " attempts, unable to get " + (fProt ? "protein" : "nucleotide") + " info for accessions " + finalAccList);
		                        	if (progress != null) { // otherwise we already have those empty Accession objects
		                        		List<Accession> accObjects = finalAccList.stream().map(unobtainedAccId -> new Accession(new AccessionId(fProt ? AccessionId.PROTEIN_TYPE : AccessionId.NUCLEOTIDE_TYPE, unobtainedAccId), null /* setting a null taxon is a way of marking the accession for retries */, null)).collect(Collectors.toList());
		                        		try {
		                        			MongoTemplateManager.getCommonsTemplate().insert(accObjects, Accession.class);
		                        		}
			                        	catch (DuplicateKeyException dke) { // may happen in the case of concurrent imports
			                        		for (Accession accObj : accObjects)
			                        			MongoTemplateManager.getCommonsTemplate().save(accObj);
			                        	}
		                        	}
		                    		nTotalFailureCount.incrementAndGet();
		                    	}
		                    	else { // WS could be reached but some accessions do not seem to exist: mark their taxon as unidentified
	                	            if (progress != null) {
	                        			List<Accession> accObjects = finalAccList.stream().map(accId -> new Accession(new AccessionId(fProt ? AccessionId.PROTEIN_TYPE : AccessionId.NUCLEOTIDE_TYPE, accId), Taxon.UNIDENTIFIED_ORGANISM_TAXID, null)).collect(Collectors.toList());
		                        		try {
		                        			MongoTemplateManager.getCommonsTemplate().insert(accObjects, Accession.class);
		                        		}
			                        	catch (DuplicateKeyException dke) { // may happen in the case of concurrent imports
			                        		for (Accession accObj : accObjects)
			                        			MongoTemplateManager.getCommonsTemplate().save(accObj);
			                        	}
	                	            }
	                	            else { // we're not being invoked by an import, the accessions already exist so we can't insert
	                	            	Query query = new Query(new Criteria().andOperator(Criteria.where("_id." + DBField.FIELDNAME_TYPE).is(String.valueOf(fProt ? AccessionId.PROTEIN_TYPE : AccessionId.NUCLEOTIDE_TYPE)), Criteria.where("_id." + Assignment.FIELDNAME_SSEQID).in(finalAccList)));
	                	            	MongoTemplateManager.getCommonsTemplate().updateMulti(query, new Update().set(Accession.FIELDNAME_NCBI_TAXID, Taxon.UNIDENTIFIED_ORGANISM_TAXID), Accession.class);
	                	            }
	                	            LOG.info("Unable to find valid taxid for " + finalAccList.size() + " accessions in " + (fProt ? "protein" : "nucleotide") + " database: " + StringUtils.join(finalAccList, ","));
		                	    }
	                    	}
	            		}
	                };

	                if (chunkIndex % nNConcurrentThreads == (nNConcurrentThreads - 1) || accIndex == accLists[j].size()) {
	                    threadsToWaitFor.add(queryThread); // only needed to have an accurate count
	                    queryThread.run();	// run synchronously
	                    
	                    if (progress != null)
	                    	progress.setCurrentStepProgress(accIndex * 100 / nTotalAccCount);
	
	                    for (Thread ttwf : threadsToWaitFor)	// wait for all threads before moving to next phase
	            			ttwf.join();
	                    
	                    if ((chunkIndex + 1) >= (5 * nNConcurrentThreads) && nTotalFailureCount.get() == chunkIndex + 1)
	                    	throw new InterruptedException("E-utilities web-service seems to be down: aborting!");	// no data could be obtained for any of the 5 first chunks
	
	            		threadsToWaitFor.clear();
	                }
	                else {
	                    threadsToWaitFor.add(queryThread);
	                    queryThread.start();	// run asynchronously for better speed
	                }
	
	                accIdList.clear();
	            }
	        }
        }
        if (progress != null)
        	progress.setCurrentStepProgress(100);

       	LOG.info("accessions fetched from NCBI: " + result.size());
       	return result;
    }

    public static void importAccessionsFromDump(Resource resource) throws IOException {
    	long startTime = System.currentTimeMillis();
    	ZipInputStream zis = new ZipInputStream(resource.getInputStream());
		zis.getNextEntry();
		Reader reader = new InputStreamReader(zis);
		BufferedReader br = new BufferedReader(reader);
		String line;
		List<DBObject> accessionTmplist = new ArrayList<>(5000);
		MongoTemplate mongoTemplate = MongoTemplateManager.getCommonsTemplate();
		while ((line = br.readLine()) != null) {
			DBObject jsonobject = ( DBObject ) BasicDBObject.parse( line );
			accessionTmplist.add(jsonobject);
			if (accessionTmplist.size()>=5000) {
				mongoTemplate.insert(accessionTmplist, Accession.class);
				accessionTmplist.clear();
			}
		}
		if (accessionTmplist.size() > 0) {
			mongoTemplate.insert(accessionTmplist, Accession.class);
			accessionTmplist.clear();
		}
		long endTime = System.currentTimeMillis();
        LOG.info("Finished importing accessions from dump into database in " + ((endTime - startTime) / 1000) + "s");
    }
    
    public void retryFailedAccessionRequests(boolean fAsync, Integer updateLimit) {
    	MongoTemplate commonsTemplate = MongoTemplateManager.getCommonsTemplate();
    	if (commonsTemplate.findOne(new Query(Criteria.where(Accession.FIELDNAME_NCBI_TAXID).is(null)), Accession.class) != null) {
     		Thread t = new Thread() {
    			public void run() { // some assignments are not tied to a taxon: let's try and invoke NCBI service again
    				try {
    					List<Document> pipeline = Arrays.asList(new Document("$match", new Document(Accession.FIELDNAME_NCBI_TAXID, null)), new Document("$project", new Document("_id", 1)));
    					MongoCursor<Document> accIDiterator = commonsTemplate.getCollection(commonsTemplate.getCollectionName(Accession.class)).aggregate(pipeline).allowDiskUse(true).iterator();
    					List<Document> accIDs = new ArrayList<>();
    					while (accIDiterator.hasNext())
    						accIDs.add((Document) accIDiterator.next().get("_id"));

    					boolean fApplyLimit = updateLimit != null && updateLimit < accIDs.size();
    		        	LOG.info(accIDs.size() + " accessions still need to be fetched: invoking NCBI service again" + (updateLimit != null && fApplyLimit ? " for " + updateLimit + " of them" : ""));
    					if (fApplyLimit)
    						accIDs = accIDs.subList(0, updateLimit);
      					List<String> prefixedAccIDs = new ArrayList<>();
    					for (Document accID : accIDs)
    						prefixedAccIDs.add((accID.getString(DBField.FIELDNAME_TYPE).charAt(0) == Accession.AccessionId.PROTEIN_TYPE ? Accession.ID_PROTEIN_PREFIX : Accession.ID_NUCLEOTIDE_PREFIX) + accID.getString(Assignment.FIELDNAME_SSEQID));
    		            List<String>[] accessionsByType = Accession.separateNuclFromProtIDs(prefixedAccIDs, false);
	            		Collection<String> fetchedAccIDs = fetchRemoteAccessionInfo(accessionsByType[0], accessionsByType[1], null);
	            		if (fetchedAccIDs.isEmpty())
	            			return;

    					long before = System.currentTimeMillis();
    					List<Accession> fetchedAccessions = commonsTemplate.find(new Query(Criteria.where("_id").in(fetchedAccIDs.stream().map(accId -> new Accession.AccessionId(accId.startsWith(Accession.ID_PROTEIN_PREFIX) ? Accession.AccessionId.PROTEIN_TYPE : Accession.AccessionId.NUCLEOTIDE_TYPE, accId.substring(2))).collect(Collectors.toList()))), Accession.class);
            			List<Thread> moduleUpdateThreads = new ArrayList<>();
            			for (String module : MongoTemplateManager.getAvailableModules()) { // update assignments linked to fetchedAccessions
            				final MongoTemplate mongoTemplate = MongoTemplateManager.get(module);
            				Thread t = new Thread() {
                				public void run() {
    			            		for (Accession fetchedAcc : fetchedAccessions) {
	    	            				Query q = new Query(new Criteria().andOperator(
	                						Criteria.where(AssignedSequence.FIELDNAME_ASSIGNMENT + "." + DBConstant.DOUBLE_TYPE + "." + DBField.taxonFieldId).is(null),
	                						Criteria.where(AssignedSequence.FIELDNAME_ASSIGNMENT + "." + DBConstant.STRING_ARRAY_TYPE + "." + DBField.sseqIdFieldId).regex("^" + (fetchedAcc.isProtein() ? Accession.ID_PROTEIN_PREFIX : Accession.ID_NUCLEOTIDE_PREFIX) + fetchedAcc.getId().getSseqid() + "(\\..+|$)")
	                					));
	    	            				Update u = new Update().set(AssignedSequence.FIELDNAME_ASSIGNMENT + ".$." + DBConstant.DOUBLE_TYPE + "." + DBField.taxonFieldId, fetchedAcc.getTx()).set(AssignedSequence.FIELDNAME_ASSIGNMENT + ".$." + DBConstant.STRING_TYPE + "." + DBField.hitDefFieldId, fetchedAcc.getHd());
	    	            				UpdateResult wr = mongoTemplate.updateMulti(q, u, AssignedSequence.class);
	    	            				if (wr.getModifiedCount() > 0)
	    	            					LOG.info("In database " + module + ", " + wr.getModifiedCount() + " sequences were updated with newly obtained tax id " + fetchedAcc.getTx());
	                				}
			            		}
            				};
            				moduleUpdateThreads.add(t);
            				t.start();
	            		}
            			for (Thread t : moduleUpdateThreads)
            				t.join();
	            		LOG.debug("Updating assignments with newly obtained tax IDs in " + MongoTemplateManager.getAvailableModules().size() + " databases took " + (System.currentTimeMillis() - before)/1000 + "s");
    				}
					catch(Exception e) {
						LOG.warn("Unable to fetch accession records from NCBI service", e);
					}
				}
    		};
    		if (fAsync)
    			t.start();
    		else
    			t.run();
    	}
    }
}
