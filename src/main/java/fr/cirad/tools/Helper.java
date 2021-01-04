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
package fr.cirad.tools;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.net.HttpURLConnection;
import java.net.URL;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.bson.Document;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import com.mongodb.BasicDBObject;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;

import fr.cirad.metaxplor.model.AssignedSequence;
import fr.cirad.metaxplor.model.Sample;
import fr.cirad.tools.mongo.MongoTemplateManager;

/**
 * Custom implementation of common methods
 *
 * @author petel, sempere
 */
public class Helper {

    private Helper() {

    }

    private static final Logger LOG = Logger.getLogger(Helper.class);

    static MessageDigest md = null;

    static {
        try {
            md = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            LOG.error("Unable to find MD5 algorithm", e);
        }
    }

    /**
     * custom implementation of split() to split a string on a delimiter and
     * store results in a List
     *
     * @param stringToSplit
     * @param delimiter
     * @return
     */
    public static List<String> split(String stringToSplit, char delimiter) {
        return split(stringToSplit, delimiter, -1);
    }
    
    /**
     * custom implementation of split() to split a string on a delimiter and
     * store results in a List
     *
     * @param stringToSplit
     * @param delimiter
     * @param nMaxColumnCount
     * @return
     */
    public static List<String> split(String stringToSplit, char delimiter, int nMaxColumnCount) {
        List<String> splittedString = new ArrayList<>();
        if (stringToSplit != null) {
            int pos = 0;
            int end;
            while ((end = stringToSplit.indexOf(delimiter, pos)) >= 0) {
                splittedString.add(stringToSplit.substring(pos, end));
                if (nMaxColumnCount > 0 && splittedString.size() == nMaxColumnCount)
                	return splittedString;
                pos = end + 1;
            }
            if (nMaxColumnCount <= 0 || splittedString.size() < nMaxColumnCount)
            	splittedString.add(stringToSplit.substring(pos));
        }
        return splittedString;
    }
    
    /**
     * custom implementation of split() to split a string on a delimiter and
     * store results in a List
     *
     * @param csvString
     * @param delimiter
     * @param n
     * @return
     */
    public static String getNthColumn(String csvString, char delimiter, int n) {
        int nColCount = 0;
        if (csvString != null) {
            int pos = 0;
            int end;
            while ((end = csvString.indexOf(delimiter, pos)) >= 0) {
                if (nColCount == n)
                	return csvString.substring(pos, end);
            	nColCount++;
                pos = end + 1;
            }
        }
        throw new IndexOutOfBoundsException("CSV string contains " + ++nColCount +  " columns, index " + n + " doesn't exist");
    }

    /**
     * get lower or upper bound for a field of type float
     *
     *
     * @param baseCollection
     * @param key
     * @param direction 1 for lower bound, -1 for upper bound
     * @param projectFieldPath
     * @param projectIds
     * @return
     */
    public static Comparable getBound(MongoCollection<Document> baseCollection, String key, int direction, String projectFieldPath, int[] projectIds) {

    	BasicDBObject match = new BasicDBObject(key, new BasicDBObject("$exists", true));
        match.put(projectFieldPath, new BasicDBObject("$in", projectIds));
        BasicDBObject sort = new BasicDBObject("$sort", new BasicDBObject(key, direction));
        BasicDBObject limit = new BasicDBObject("$limit", 1);
        List<BasicDBObject> pipeline = new ArrayList<>();
        if (key.startsWith(AssignedSequence.FIELDNAME_ASSIGNMENT + ".")) {
        	pipeline.add(new BasicDBObject("$unwind", "$" + AssignedSequence.FIELDNAME_ASSIGNMENT));
        	pipeline.add(new BasicDBObject("$project", new BasicDBObject(key, 1)));
    	}
        pipeline.add(new BasicDBObject("$match", match));
        pipeline.add(sort);
        pipeline.add(limit);
        MongoCursor<Document> cursor = baseCollection.aggregate(pipeline).allowDiskUse(true).iterator();

        if (cursor != null && cursor.hasNext()) {
            Document doc = cursor.next();
            return (Comparable) readPossiblyNestedField(doc, key, "; ");
        }
        return null;
    }

    /**
     * Read possibly nested field.
     *
     * @param doc the record
     * @param fieldPath the field path
     * @param listFieldSeparator separator to use for list fields
     * @return the object
     */
    public static Object readPossiblyNestedField(Document doc, String fieldPath, String listFieldSeparator) {
    	Document slidingRecord = doc;
        String[] splitFieldName = fieldPath.split("\\.");
        Object o = null, result;
        for (String s : splitFieldName) {
            o = slidingRecord.get(s);
            if (o != null && Document.class.isAssignableFrom(o.getClass())) {
                slidingRecord = ((Document) o);
            }
        }
        if (o != null && List.class.isAssignableFrom(o.getClass())) {
            result = new ArrayList<>();
            for (Object o2 : ((List) o)) {
                if (o2 != null && List.class.isAssignableFrom(o2.getClass())) {
                    ((ArrayList<Object>) result).addAll(((List) o2));
                } else {
                    ((ArrayList<Object>) result).add(o2);
                }
            }
            result = StringUtils.join(((ArrayList<Object>) result), listFieldSeparator);
        } else {
            result = o;
        }

        if (result == null) {
            result = "";
        }

        return result;
    }
    
    /**
     * get the MD5 hash of a String
     *
     * @param string
     * @return
     */
    public static String convertToMD5(String string) {
        if (md == null) {
            return string;
        }
        byte[] messageDigest = md.digest(string.getBytes());
        BigInteger number = new BigInteger(1, messageDigest);
        String md5String = number.toString(16);
        // Now we need to zero pad it if you actually want the full 32 chars.
        while (md5String.length() < 32) {
            md5String = "0" + md5String;
        }
        return md5String;
    }

    /**
     * Csv to int array.
     *
     * @param csvString the csv string
     * @return the int[]
     */
    public static Integer[] csvToIntegerArray(String csvString) {
        if (csvString == null) {
            return new Integer[0];
        }

        String[] splittedString = csvString.split(",");
        Integer[] result = new Integer[splittedString.length];
        for (int i = 0; i < result.length; i++) {
            result[i] = Integer.parseInt(splittedString[i]);
        }
        return result;
    }
    
    public static String formatDouble(Double d) {
    	if (d == null)
    		return "";
    	String s = d.toString();
    	return s.endsWith(".0") ? s.substring(0, s.length() - 2) : s;
    }

    public static int removeObsoleteIndexes(String sModule) {
    	MongoTemplate mongoTemplate = MongoTemplateManager.get(sModule);
    	List<MongoCollection<Document>> indexedColls = Arrays.asList(mongoTemplate.getCollection(MongoTemplateManager.getMongoCollectionName(AssignedSequence.class)), mongoTemplate.getCollection(MongoTemplateManager.getMongoCollectionName(Sample.class)));
    	int nTotal = 0;
    	for (MongoCollection<Document> collection : indexedColls) {
        	long before = System.currentTimeMillis();
	    	int n = 0;
	    	for (Document doc : collection.listIndexes()) {
	        	String fieldPath = ((Document) doc.get("key")).keySet().iterator().next();
	        	if (!fieldPath.startsWith("_id")) {
	        		Query q = new Query(Criteria.where(fieldPath).exists(true)).limit(1);
	        		q.fields().include("_id");
	        		if (mongoTemplate.findOne(q, Object.class, collection.getNamespace().getCollectionName()) == null) {
	        			collection.dropIndex((String) doc.get("name"));
	                	n++;
	        		}
	        	}
	        }
	        if (n > 0)
	        	LOG.debug("removeObsoleteIndexes dropped " + n + " " + collection.getNamespace().getCollectionName() + " indexes from db " + sModule + " in " + (System.currentTimeMillis() - before) + "ms");
	        nTotal += n;
        }
        
        return nTotal;
    }
    
    public static InputStream openStreamFromUrl(String args) throws IOException {
		String lcURL = args.toLowerCase();
		boolean fIsFtp = lcURL.startsWith("ftp://");
		InputStream is;
		
		if (lcURL.startsWith("http://") || lcURL.startsWith("https://") || fIsFtp)
		{
			URL url = new URL(args);
			if (!fIsFtp)
			{
				HttpURLConnection httpConn = ((HttpURLConnection) url.openConnection());
				httpConn.setInstanceFollowRedirects(true);
				boolean fValidURL = Arrays.asList(HttpURLConnection.HTTP_OK, HttpURLConnection.HTTP_MOVED_PERM, HttpURLConnection.HTTP_MOVED_TEMP).contains(httpConn.getResponseCode());
				if (fValidURL && HttpURLConnection.HTTP_OK != httpConn.getResponseCode())
				{	// there's a redirection: try and handle it
					String sNewUrl = httpConn.getHeaderField("Location");
					if (sNewUrl != null && sNewUrl.toLowerCase().startsWith("http"))
						url = new URL(sNewUrl);
				}
			}
			is = url.openStream();
		}
		else {
			is = new FileInputStream(args);
		}
		return is;	
    }
}
