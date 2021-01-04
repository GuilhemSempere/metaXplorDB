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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Scanner;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.log4j.Logger;
import org.biojavax.RichObjectFactory;
import org.springframework.beans.factory.BeanDefinitionStoreException;
import org.springframework.context.support.GenericXmlApplicationContext;
import org.springframework.data.mongodb.core.MongoTemplate;

import com.mongodb.BasicDBObject;

import fr.cirad.metaxplor.model.Taxon;
import fr.cirad.tools.Helper;
import fr.cirad.tools.mongo.MongoTemplateManager;

public class NCBITaxonomyImport {

    private static final Logger LOG = Logger.getLogger(NCBITaxonomyImport.class);
        

    public static void importTaxonomy(String url) throws IOException {

        InputStream is = Helper.openStreamFromUrl(url);

    	File nodeFile = File.createTempFile("ncbi_taxo_nodes_", ".dmp"), nameFile = File.createTempFile("ncbi_taxo_names_", ".dmp");
        try
        {
        	ZipInputStream zis = new ZipInputStream(is);
            ZipEntry ze;
            byte[] buffer = new byte[4 * 1024];
            while ((ze = zis.getNextEntry()) != null) {
            	if ("nodes.dmp".equals(ze.getName()) || "names.dmp".equals(ze.getName())) {
                    try (FileOutputStream fos = new FileOutputStream("nodes.dmp".equals(ze.getName()) ? nodeFile : nameFile)) {
                        int len;
                        while ((len = zis.read(buffer)) > 0) {
                            fos.write(buffer, 0, len);
                        }
                    }
            	}
            }
            
            if (nodeFile == null)
            	throw new Exception("Node file not found in archive!");
            if (nameFile == null)
            	throw new Exception("Name file not found in archive!");

            new NCBITaxonomyImport().loadTaxonomy(nodeFile, nameFile);
        }
        catch (Exception e)
        {
        	e.printStackTrace();
        	return;
        }
        finally
        {
        	if (nodeFile.exists())
        		nodeFile.delete();
        	if (nameFile.exists())
        		nameFile.delete();
        }
    }

    public static void main(String[] args) throws IOException {
        if (args.length < 1)
            throw new IOException("You must pass 1 parameter as argument: URI to taxdump zip file");

        GenericXmlApplicationContext ctx = null;
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
            
            mongoTemplate.dropCollection(Taxon.class);        
            importTaxonomy(args[0]);
          } finally {
	          if (ctx != null) {
	              ctx.close();
	          }
	      }
	}

    public void loadTaxonomy(File nodeFile, File nameFile) throws IOException {
    	GenericXmlApplicationContext ctx = null;
    	try {
            MongoTemplate mongoTemplate = MongoTemplateManager.getCommonsTemplate();
            
            
            HashMap<Integer, Comparable[]> taxonMap = new HashMap<>(2000000);

            LOG.info("Loading NCBI taxonomy nodes");
            
            try (Scanner sc = new Scanner(nodeFile);){
            	String line,pti;
            	while (sc.hasNextLine()) {
            		line=sc.nextLine();
            		String[] parts = line.split("\\|");
                    pti = parts[1].trim();
                	Comparable[] taxon = new Comparable[] {pti.length()>0?new Integer(pti):null, parts[2].trim()};
                	taxonMap.put(Integer.valueOf(parts[0].trim()), taxon);
                    RichObjectFactory.clearLRUCache(/*SimpleNCBITaxon.class*/);
                }}
                LOG.info("Finished loading NCBI taxonomy nodes");
            LOG.info("Adding taxonomy names");
            try (Scanner sc = new Scanner(nameFile)) {
            	List<Taxon> taxonTmplist = new ArrayList<>(5000);
            	List<String> taxonNames = new ArrayList<>();
            	long startTime = System.nanoTime();
            	
            	String line=sc.nextLine();
            	String[] parts = line.split("\\|");
            	int currentTaxonId=Integer.valueOf(parts[0].trim());
            	Taxon taxon = new Taxon(currentTaxonId);
            	Comparable[] taxonInfo = taxonMap.get(currentTaxonId);
    			taxon.setParentId((Integer) taxonInfo[0]);
                taxon.setRank((String) taxonInfo[1]);
                taxonNames.add(parts[1].trim());
                
            	while(sc.hasNextLine()) {
            		line=sc.nextLine();
            		parts = line.split("\\|");
            		if(currentTaxonId!=Integer.valueOf(parts[0].trim())){
            			taxon.setNames(taxonNames);
            			taxonNames = new ArrayList<>();
            			taxonTmplist.add(taxon);
                        taxonMap.remove(taxon.getId());
						if (taxonTmplist.size() >= 5000) {
                        	mongoTemplate.insert(taxonTmplist, Taxon.class);
                        	taxonTmplist.clear();
                        }
            			currentTaxonId=Integer.valueOf(parts[0].trim());
            			taxon = new Taxon(currentTaxonId);
            			taxonInfo = taxonMap.get(currentTaxonId);
            			taxon.setParentId((Integer) taxonInfo[0]);
                        taxon.setRank((String) taxonInfo[1]);
                        if(parts[3].trim().equals("scientific name")) {
                    		taxonNames.add(0, parts[1].trim());}else {taxonNames.add(parts[1].trim());}
            		}else {
            			if(parts[3].trim().equals("scientific name")) {
                        	taxonNames.add(0, parts[1].trim());}else {taxonNames.add(parts[1].trim());}}}
            	taxon.setNames(taxonNames);
    			taxonTmplist.add(taxon);
    			if (taxonTmplist.size() > 0) {
    				mongoTemplate.insert(taxonTmplist, Taxon.class);
                    taxonTmplist.clear();
    			}
                
            	sc.close();
            	
                LOG.info("Completed saving NCBI taxonomy into database in "+((System.nanoTime() - startTime)/1000000)+"ms");

                // create indexes on collection 
                mongoTemplate.getCollection(mongoTemplate.getCollectionName(Taxon.class)).createIndex(new BasicDBObject(Taxon.FIELDNAME_NAMES, 1));
                mongoTemplate.getCollection(mongoTemplate.getCollectionName(Taxon.class)).createIndex(new BasicDBObject(Taxon.FIELDNAME_PARENT_ID, 1));
            }
        } finally {
            if (ctx != null) {
                ctx.close();
            }
        }
    }
}
