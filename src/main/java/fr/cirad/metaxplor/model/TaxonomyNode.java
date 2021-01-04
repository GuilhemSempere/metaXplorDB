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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.log4j.Logger;
import org.bson.Document;

import com.mongodb.BasicDBObject;
import com.mongodb.client.MongoCursor;

import fr.cirad.tools.mongo.MongoTemplateManager;

public class TaxonomyNode implements Comparable {

	private static final Logger LOG = Logger.getLogger(TaxonomyNode.class);
	
	public static LinkedHashMap<String, String> rankPrefixes = new LinkedHashMap<String, String>() {{
		put("kingdom", "k__");
		put("phylum", "p__");
		put("class", "c__");
		put("order", "o__");
		put("family", "f__");
		put("genus", "g__");
		put("species", "s__");
	}};

    final Integer id;
    String text;
    int assignedSeqCounts;
    Set<TaxonomyNode> children = new TreeSet<>();

    public TaxonomyNode(int taxonId, String text, int assignedSeqCounts) {
        this.id = taxonId;
        this.assignedSeqCounts = assignedSeqCounts;
        this.text = text;
    }

    public void addChildren(TaxonomyNode node) {
        this.children.add(node);
//        updateNode();
    }

//    public void updateNode() {
//        for (TaxonomyNode childNode : this.children) {
//            this.assignedSeqCounts += childNode.assignedSeqCounts;
//        }
//    }
    
    public int countAssignedSegs() {
    	int count = assignedSeqCounts;
        for (TaxonomyNode childNode : this.children) {
        	count += childNode.countAssignedSegs();
        }
        return count;
    }

    @Override
    public int compareTo(Object o) {
        return id.compareTo(((TaxonomyNode) o).id);
    }

    public Integer getId(){
        return id; 
    }

    public String getText() {
        return text + " {" + countAssignedSegs() + "}";
    }

    public Set<TaxonomyNode> getChildren() {
        return children;
    }

    static public HashMap<Integer, String> getTaxaAncestry(Collection<Integer> taxa, boolean fUseNamesRatherThanIDs, boolean fIncludeRankPrefix, String delimiter) {
//    	long before = System.currentTimeMillis();
    	String taxCollName = MongoTemplateManager.getCommonsTemplate().getCollectionName(Taxon.class);
    	List<BasicDBObject> pipeline = new ArrayList<>();
    	pipeline.add(new BasicDBObject("$match", new BasicDBObject("_id", new BasicDBObject("$in", taxa))));
    	pipeline.add(new BasicDBObject("$graphLookup", new BasicDBObject("from", taxCollName).append("startWith", "$_id").append("connectFromField", "pa").append("connectToField", "_id").append("as", "tx").append("depthField", "dp")));
    	pipeline.add(new BasicDBObject("$unwind", "$tx"));
    	pipeline.add(new BasicDBObject("$sort", new BasicDBObject("tx.dp", -1)));
    	pipeline.add(new BasicDBObject("$group", new BasicDBObject("_id", "$_id").append("tx", new BasicDBObject("$push", "$tx"))));
    	MongoCursor<Document> cursor = MongoTemplateManager.getCommonsTemplate().getCollection(taxCollName).aggregate(pipeline).iterator();

    	HashMap<Integer, String> result = new HashMap<>();
    	while (cursor.hasNext()) {
    		Document taxonWithAncestry = cursor.next();
    		StringBuffer taxonomy = new StringBuffer();
    		for (Object tx : (List) taxonWithAncestry.get("tx")) {
    			Document taxon = (Document) tx;
    			if ((int) taxon.get("_id") != 1 && (!fUseNamesRatherThanIDs || !"no rank".equals(taxon.get(Taxon.FIELDNAME_RANK)))) {
    				String prefix = fIncludeRankPrefix ? rankPrefixes.get(taxon.get(Taxon.FIELDNAME_RANK)) : null;
    				taxonomy.append((taxonomy.length() == 0 ? "" : delimiter) + (prefix != null ? prefix : "") + (fUseNamesRatherThanIDs ? ((List) taxon.get(Taxon.FIELDNAME_NAMES)).get(0) : taxon.get("_id")));
    			}
    		}
    		result.put((int) taxonWithAncestry.get("_id"), taxonomy.toString()); 
    	}
//    	LOG.debug("getTaxaAncestry took " + (System.currentTimeMillis() - before) + "ms for " + taxa.size() + " taxa");
    	return result;
    }

    static public int calculateFirstCommonAncestor(Integer[][] taxaAncestry) {
    	int nTaxLevel = -1, nPreviousLevelMajorTaxCount = taxaAncestry.length;
    	Map<Integer, Integer> taxCounts = null;
    	List<Map<Float, List<Integer>>> taxFreqToIdListByLevel = new ArrayList<>();
    	while (taxCounts == null || !taxCounts.isEmpty()) {
        	taxCounts = new HashMap<>();
    		
    		nTaxLevel++;
    		for (int i=0; i<taxaAncestry.length; i++) {
    			if (taxaAncestry[i].length < nTaxLevel + 1)
    				continue;	// no more items here
    			
    			int taxon = taxaAncestry[i][nTaxLevel];
    			if (nTaxLevel > 0 && !taxFreqToIdListByLevel.get(nTaxLevel - 1).values().iterator().next().contains(taxaAncestry[i][nTaxLevel - 1]))
    				continue;	// does not descend from the previous major taxon
    			
    			Integer freq = taxCounts.get(taxon);
    			taxCounts.put(taxon, freq == null ? 1 : (freq + 1));    			
    		}
    		
    		if (taxCounts.isEmpty())
    			break;
//    		System.out.println(taxCounts);
    		
			Map<Float, List<Integer>> taxFreqToIdList = new TreeMap<>(Collections.reverseOrder());
			taxFreqToIdListByLevel.add(taxFreqToIdList);

			int nMajorTaxCount = 0;
    		for (int taxon : taxCounts.keySet()) {
    			int nTaxCount = taxCounts.get(taxon);
    			if (nTaxCount > nMajorTaxCount)
    				nMajorTaxCount = nTaxCount;

    			float freq = (float) nTaxCount / nPreviousLevelMajorTaxCount;
    			List<Integer> taxaWithThisFreq = taxFreqToIdList.get(freq);
    			if (taxaWithThisFreq == null) {
    				taxaWithThisFreq = new ArrayList<Integer>();
    				taxFreqToIdList.put(freq, taxaWithThisFreq);
    			}
    			taxaWithThisFreq.add(taxon);
    			if (freq > 0.5 || (freq == 0.5 && taxFreqToIdList.size() > 1))
    				break;	// none can be more frequent than this one
    		}
    		
    		boolean fSeveralExAequoMajor = taxFreqToIdListByLevel.get(nTaxLevel).values().iterator().next().size() > 1;	// if we have several ex-aequo major taxa we will stick to their parent because we cannot choose between them
    		boolean fOneCountOutOfMoreThanTwo = nMajorTaxCount == 1 && nPreviousLevelMajorTaxCount > 2;	// if the parent occurred more than twice and this taxon occurs only once (which means all others are undefined), we don't want trust it either
			if (fSeveralExAequoMajor || fOneCountOutOfMoreThanTwo) {	
				taxFreqToIdListByLevel.remove(taxFreqToIdListByLevel.size() - 1);
				break;
			}

			nPreviousLevelMajorTaxCount = nMajorTaxCount;
    	}
    	
//		System.out.println(taxFreqToIdListByLevel);    	
    	return nTaxLevel <= 0 ? -1 : taxFreqToIdListByLevel.get(nTaxLevel - 1).values().iterator().next().get(0);
	}

}
