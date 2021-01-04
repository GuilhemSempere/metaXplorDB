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

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.env.Environment;
import org.springframework.core.io.Resource;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;

import fr.cirad.metaxplor.importing.AccessionImport;
import fr.cirad.metaxplor.importing.NCBITaxonomyImport;
import fr.cirad.metaxplor.model.Accession;
import fr.cirad.metaxplor.model.Taxon;
import fr.cirad.tools.mongo.MongoTemplateManager;

@Configuration
@PropertySource("classpath:config.properties")
public class AppConfig {

    private Environment environment;

    @Autowired
    public void setEnvironment(Environment environment) {
        this.environment = environment;
        
//    	Thread taxoLoadThread = null;
//        MongoTemplate commonsTemplate = MongoTemplateManager.getCommonsTemplate();
//		if (commonsTemplate.count(new Query(), Taxon.class) == 0) {
//        	taxoLoadThread = new Thread () {
//	        	public void run() {
//		        	try {
//		        		LOG.warn("No data found in collection Taxonomy: Trying to build it from NCBI dump");
//		        		NCBITaxonomyImport.importTaxonomy(AppConfig.getNcbiTaxdumpZipUrl());
//		        	}
//		        	catch (Exception e) {
//		            	LOG.error("Error while performing Taxonomy Import",e);
//		            	throw new Error(e);
//		            }
//	        	}
//	        };
//        }
//        if (taxoLoadThread != null)
//        	taxoLoadThread.start();
//        if (commonsTemplate.count(new Query(), Accession.class) == 0) {
//        	Resource accessionDumpResource = ac.getResource("data/initial_accession_cache.zip");
//    		if (accessionDumpResource.exists())
//	        	try {
//	        		LOG.info("Accession dump file has been found: importing it");
//	        		AccessionImport.importAccessionsFromDump(accessionDumpResource);
//	        	}
//	        	catch (Exception e) {
//	        		LOG.error("Error while performing accession import from dump file",e);
//	            	throw new Error(e);
//	        	}
//    		else
//            	LOG.warn("No data found in collection " + commonsTemplate.getCollectionName(Accession.class) + " in metaxplor_commons. Every single accession info will need to be fetched from NCBI");
//        }
//
//        if (taxoLoadThread != null)
//			try {
//				taxoLoadThread.join();
//			} catch (InterruptedException e) {
//            	LOG.error("Error while performing Taxonomy Import",e);
//            	throw new Error(e);
//			}
    }

    public String sequenceLocation() {
        return environment.getProperty("sequenceLocation");
    }

    public String blastDbLocation() {
        return environment.getProperty("blastDBLocation");
    }

    public String getAdminEmail() {
        return environment.getProperty("adminEmail");
    }
    
    public String getNcbiApiKey() {
        return environment.getProperty("NCBI_api_key");
    }
    
    public String getNcbiTaxdumpZipUrl() {
    	String url = environment.getProperty("NCBI_taxdump_zip_url");    	
        return url == null ? "ftp://ftp.ncbi.nlm.nih.gov/pub/taxonomy/new_taxdump/new_taxdump.zip" : url;
    }
    
    public String getEUtilsBaseUrl() throws Exception {
    	String url = environment.getProperty("eutils_base_url");
    	if (url == null)
    		return null;

        return url + (url.endsWith("/") ? "" : "/");
    }
    
    public String get(String sPropertyName) {
        return environment.getProperty(sPropertyName);
    }
}