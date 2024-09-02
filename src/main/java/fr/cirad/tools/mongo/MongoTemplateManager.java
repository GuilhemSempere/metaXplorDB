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
package fr.cirad.tools.mongo;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.ResourceBundle;
import java.util.ResourceBundle.Control;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.log4j.Logger;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Component;

import com.mongodb.BasicDBObject;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClient;
import com.mongodb.connection.ClusterDescription;
import com.mongodb.connection.ServerDescription;

import fr.cirad.metaxplor.importing.AccessionImport;
import fr.cirad.metaxplor.importing.NCBITaxonomyImport;
import fr.cirad.metaxplor.model.Accession;
import fr.cirad.metaxplor.model.DatabaseInformation;
import fr.cirad.metaxplor.model.MetagenomicsProject;
import fr.cirad.metaxplor.model.Taxon;
import fr.cirad.tools.AppConfig;

/**
 * Class to handle connection with database instance connection to multiple
 * database are stored in a Map<String, MongoTemplate>
 * where the key is the module name
 *
 * @author petel, sempere
 */
@Component
public class MongoTemplateManager implements ApplicationContextAware {
    
    private static final Logger LOG = Logger.getLogger(MongoTemplateManager.class);
    
    @Autowired private AppConfig appConfig;

    private static MongoTemplate commonsTemplate;

    private static ApplicationContext applicationContext;

    private static Map<String, MongoTemplate> templateMap = new TreeMap<>();
    /**
     * list of database visible by everybody (logged + unlogged users)
     */
    static private Set<String> publicDatabases = new TreeSet<>();
    /**
     * list of database visible by logged users only
     */
    private static List<String> hiddenDatabases = new ArrayList<>();
    /**
     * Map to store the connection instance
     */
    private static Map<String, MongoClient> mongoClients = new HashMap<>();
    private static final String RESOURCE = "datasources";
    private static final String EXPIRY_PREFIX = "_ExpiresOn_";
    
    /**
     * The datasource  (properties filename)
     */
    static private String resource = "datasources";

    /**
     * The datasource properties
     */  
    static private Properties dataSourceProperties = new Properties();

    public static final String TMP_VIEW_PREFIX = "view_";
    public static final String TMP_SAMPLE_SORT_CACHE_COLL = "sampleSortCache_";

    private static final String DOT_REPLACEMENT_STRING = "\\[dot\\]";

    public enum ModuleAction {
        CREATE, UPDATE_STATUS, DELETE;
    }

	/** Map that associates modules to projects currently undergoing a write operation, thus making them unavailable for other write operations
	 *  A null value in the set indicates the whole module is locked (i.e., a dump is being generated or restored)
	 */
	private static HashMap<String /*module*/, Set<String> /*projects*/> currentlyImportedProjects = new HashMap<String, Set<String>>();

    /**
     *
     * @param ac
     */
    @Override
    public void setApplicationContext(ApplicationContext ac) {
        initialize(ac);

    	if (commonsTemplate == null)
        	throw new Error("No entry named 'metaxplor_commons' was found in datasources.properties");
    	Thread taxoLoadThread = null;
        if (commonsTemplate.count(new Query(), Taxon.class) == 0) {
        	taxoLoadThread = new Thread () {
	        	public void run() {
		        	try {
		        		LOG.warn("No data found in collection Taxonomy: Trying to build it from NCBI dump");
		        		NCBITaxonomyImport.importTaxonomy(appConfig.getNcbiTaxdumpZipUrl());
		        	}
		        	catch (Exception e) {
		            	LOG.error("Error while performing Taxonomy Import",e);
		            	throw new Error(e);
		            }
	        	}
	        };
        }
        if (taxoLoadThread != null)
        	taxoLoadThread.start();
        if (commonsTemplate.count(new Query(), Accession.class) == 0) {
        	Resource accessionDumpResource = ac.getResource("data/initial_accession_cache.zip");
    		if (accessionDumpResource.exists())
	        	try {
	        		LOG.info("Accession dump file has been found: importing it");
	        		AccessionImport.importAccessionsFromDump(accessionDumpResource);
	        	}
	        	catch (Exception e) {
	        		LOG.error("Error while performing accession import from dump file",e);
	            	throw new Error(e);
	        	}
    		else
            	LOG.warn("No data found in collection " + commonsTemplate.getCollectionName(Accession.class) + " in metaxplor_commons. Every single accession info will need to be fetched from NCBI");
        }

        if (taxoLoadThread != null)
			try {
				taxoLoadThread.join();
			} catch (InterruptedException e) {
            	LOG.error("Error while performing Taxonomy Import",e);
            	throw new Error(e);
			}

        // we do this cleanup here because it only happens when the webapp is being (re)started
        templateMap.keySet().stream().forEach((module) -> {
        	MongoTemplate mongoTemplate = templateMap.get(module);
            mongoTemplate.getCollectionNames().stream().filter((collName) -> collName.startsWith(TMP_VIEW_PREFIX) || collName.startsWith(TMP_SAMPLE_SORT_CACHE_COLL)).map((collName) -> {
                mongoTemplate.dropCollection(collName);
                return collName;
            }).forEach((collName) -> {
                LOG.debug("Dropped " + collName + " in module " + module);
            });
        });
    }

    /**
     *
     * @param ac
     */
    static public void initialize(ApplicationContext ac) throws BeansException {
    	if (applicationContext != null)
    		return;	// already initialized
    	
        applicationContext = ac;
        while (applicationContext.getParent() != null) /* we want the root application-context */
            applicationContext = applicationContext.getParent();

        loadDataSources();
    }

    /**
     * Instanciate connection with database and store them in the mongoClients
     * Map. Runs at app startup
     */
    private static void loadDataSources() {
        templateMap.clear();
        mongoClients.clear();
        publicDatabases.clear();
        hiddenDatabases.clear();
        try {
            mongoClients = applicationContext.getBeansOfType(MongoClient.class);
            
    	    InputStream input = MongoTemplateManager.class.getClassLoader().getResourceAsStream(resource + ".properties");
    	    dataSourceProperties.load(input);
    	    input.close();
            
            Enumeration<Object> bundleKeys = dataSourceProperties.keys();
            while (bundleKeys.hasMoreElements()) {
                String key = (String) bundleKeys.nextElement();
                String[] datasourceInfo = dataSourceProperties.getProperty(key).split(",");

                if (datasourceInfo.length < 2) {
                    LOG.error("Unable to deal with datasource info for key " + key + ". Datasource definition requires at least 2 comma-separated strings: mongo host bean name (defined in Spring application context) and database name");
                    continue;
                }

                boolean fHidden = key.endsWith("*"), fPublic = key.startsWith("*");
                String cleanKey = key.replaceAll("\\*", "");
                if (cleanKey.length() == 0) {
                	LOG.warn("Skipping unnamed datasource");
                	continue;
                }

                if (templateMap.containsKey(cleanKey)) {
                    LOG.error("Datasource " + cleanKey + " already exists!");
                    continue;
                }

                try {
                    MongoTemplate mongoTemplate = createMongoTemplate(datasourceInfo[0], datasourceInfo[1]);
	                if ("metaxplor_commons".equals(cleanKey))
	                	commonsTemplate = mongoTemplate;
	                else
	                {
	                    templateMap.put(cleanKey, mongoTemplate);
	                    if (fPublic)
	                        publicDatabases.add(cleanKey);
	                    if (fHidden)
	                        hiddenDatabases.add(cleanKey);
	                    LOG.info("Datasource " + cleanKey + " loaded as " + (fPublic ? "public" : "private") + " and " + (fHidden ? "hidden" : "exposed"));
	                }
                }
                catch (UnknownHostException e) {
                    LOG.warn("Unable to create MongoTemplate for module " + cleanKey + " (no such host)");
                }
                catch (Exception e) {
                    LOG.warn("Unable to create MongoTemplate for module " + cleanKey, e);
                }
            }
        } catch (IOException ioe) {
            LOG.error("Unable to load " + resource + ".properties, you may need to adjust your classpath", ioe);
        }
    }

    /**
     *
     * @param sHost
     * @param sDbName
     * @return
     * @throws Exception 
     */
    public static MongoTemplate createMongoTemplate(String sHost, String sDbName) throws Exception {
        MongoClient client = mongoClients.get(sHost);
        if (client == null) {
            throw new IOException("Unknown host: " + sHost);
        }

        MongoTemplate mongoTemplate = new MongoTemplate(client, sDbName);
        ((MappingMongoConverter) mongoTemplate.getConverter()).setMapKeyDotReplacement(DOT_REPLACEMENT_STRING);
		mongoTemplate.getDb().runCommand(new BasicDBObject("profile", 0));

        return mongoTemplate;
    }

    /**
     * Saves or updates a data source.
     *
     * @param action the action to perform on the module
     * @param sModule the module, with a leading * if public and/or a trailing *
     * if hidden
     * @param fPublic flag telling whether or not the module shall be public,
     * ignored for deletion
     * @param fHidden flag telling whether or not the module shall be hidden,
     * ignored for deletion
     * @param sHost the host, only used for creation
     * @param sSpeciesName scientific name of the species, optional, ignored for
     * deletion
     * @param expiryDate the expiry date, only used for creation
     * @return
     * @throws Exception the exception
     */
    synchronized static public boolean saveOrUpdateDataSource(ModuleAction action, String sModule, boolean fPublic, boolean fHidden, String sHost, String sSpeciesName, Long expiryDate) throws Exception {	// as long as we keep all write operations in a single synchronized method, we should be safe
    	if (get(sModule) == null) {
    		if (!action.equals(ModuleAction.CREATE))
    			throw new Exception("Module " + sModule + " does not exist!");
    	}
    	else if (action.equals(ModuleAction.CREATE))
    		throw new Exception("Module " + sModule + " already exists!");
    	
    	FileOutputStream fos = null;
        File f = new ClassPathResource("/" + resource + ".properties").getFile();
    	FileReader fileReader = new FileReader(f);

        dataSourceProperties.load(fileReader);
        
    	try
    	{
    		if (action.equals(ModuleAction.DELETE))
    		{
    	        String sModuleKey = (isModulePublic(sModule) ? "*" : "") + sModule + (isModuleHidden(sModule) ? "*" : "");
                if (!dataSourceProperties.containsKey(sModuleKey))
                {
                	LOG.warn("Module could not be found in datasource.properties: " + sModule);
                	return false;
                }
                dataSourceProperties.remove(sModuleKey);
                fos = new FileOutputStream(f);
                dataSourceProperties.store(fos, null);
                return true;
    		}
	        else if (action.equals(ModuleAction.CREATE))
	        {
	            int nRetries = 0;
		        while (nRetries < 100)
		        {
		            String sIndexForModule = nRetries == 0 ? "" : ("_" + nRetries);
		            String sDbName = "mgdb2_" + sModule + sIndexForModule + (expiryDate == null ? "" : (EXPIRY_PREFIX + expiryDate));
		            MongoTemplate mongoTemplate = createMongoTemplate(sHost, sDbName);
		            if (mongoTemplate.getCollectionNames().size() > 0)
		                nRetries++;	// DB already exists, let's try with a different DB name
		            else
		            {
		                if (dataSourceProperties.containsKey(sModule) || dataSourceProperties.containsKey("*" + sModule) || dataSourceProperties.containsKey(sModule + "*") || dataSourceProperties.containsKey("*" + sModule + "*"))
		                {
		                	LOG.warn("Tried to create a module that already exists in datasource.properties: " + sModule);
		                	return false;
		                }
		                String sModuleKey = (fPublic ? "*" : "") + sModule + (fHidden ? "*" : "");
		                dataSourceProperties.put(sModuleKey, sHost + "," + sDbName + (sSpeciesName == null ? "" : ("," + sSpeciesName)));		                fos = new FileOutputStream(f);
		                dataSourceProperties.store(fos, null);

		                templateMap.put(sModule, mongoTemplate);
		                if (fPublic)
		                    publicDatabases.add(sModule);
		                if (fHidden)
		                    hiddenDatabases.add(sModule);
		                updateDatabaseLastModification(sModule);
		                return true;
		            }
		        }
		        throw new Exception("Unable to create a unique name for datasource " + sModule + " after " + nRetries + " retries");
	        }
	        else if (action.equals(ModuleAction.UPDATE_STATUS))
	        {
	        	String sModuleKey = (isModulePublic(sModule) ? "*" : "") + sModule + (isModuleHidden(sModule) ? "*" : "");
                if (!dataSourceProperties.containsKey(sModuleKey))
                {
                	LOG.warn("Tried to update a module that could not be found in datasource.properties: " + sModule);
                	return false;
                }
                String[] propValues = ((String) dataSourceProperties.get(sModuleKey)).split(",");
                dataSourceProperties.remove(sModuleKey);
                dataSourceProperties.put((fPublic ? "*" : "") + sModule + (fHidden ? "*" : ""), propValues[0] + "," + propValues[1] + (sSpeciesName == null ? "" : ("," + sSpeciesName)));
                fos = new FileOutputStream(f);
                dataSourceProperties.store(fos, null);
                
                if (fPublic)
                    publicDatabases.add(sModule);
                else
                	publicDatabases.remove(sModule);
                if (fHidden)
                    hiddenDatabases.add(sModule);
                else
                	hiddenDatabases.remove(sModule);
	        	return true;
	        }
	        else
	        	throw new Exception("Unknown ModuleAction: " + action);
        }
    	catch (IOException ex)
    	{
            LOG.warn("Failed to update datasource.properties for action " + action + " on " + sModule, ex);
            return false;
        }
    	finally
    	{
            try 
            {
           		fileReader.close();
            	if (fos != null)
            		fos.close();
            } 
            catch (IOException ex)
            {
                LOG.debug("Failed to close FileReader", ex);
            }
        }
    }

    /**
     * Removes the data source.
     *
     * @param sModule the module
     * @param fAlsoDropDatabase whether or not to also drop database
     * @return
     */
    static public boolean removeDataSource(String sModule, boolean fAlsoDropDatabase) {
        try {
            String key = sModule.replaceAll("\\*", "");
            saveOrUpdateDataSource(ModuleAction.DELETE, key, false, false, null, null, null);	// only this unique synchronized method may write to file safely

            if (fAlsoDropDatabase)
                templateMap.get(key).getDb().drop();
            templateMap.remove(key);
            publicDatabases.remove(key);
            hiddenDatabases.remove(key);
            return true;
        } catch (Exception ex) {
            LOG.warn("Failed to remove " + sModule + " datasource.properties", ex);
            return false;
        }
    }

    public static boolean updateVisibility(String module, int projId, boolean visibility) {

        MongoTemplate mongoTemplate = MongoTemplateManager.get(module);
        Query query = new Query(Criteria.where("_id").is(projId));
        Update update = new Update().set(MetagenomicsProject.FIELDNAME_PUBLIC, visibility);
        mongoTemplate.updateFirst(query, update, MetagenomicsProject.class);
        return true;
    }

    public static Set<String> getHostNames() {
        return mongoClients.keySet();
    }

    public static MongoTemplate get(String module) {
        return templateMap.get(module);
    }

    /**
     * Gets the public database names.
     *
     * @return the public database names
     */
    static public Collection<String> getPublicDatabases() {
        return publicDatabases;
    }

    public static Set<String> getAvailableModules() {
        return templateMap.keySet();
    }

    public static boolean isModulePublic(String module) {
        return publicDatabases.contains(module);
    }

    public static boolean isModuleHidden(String module) {
        return hiddenDatabases.contains(module);
    }

    /**
     * get the name of a collection from it's java model class
     *
     * @param clazz
     * @return String collection name
     */
    public static String getMongoCollectionName(Class clazz) {
        Document document = (Document) clazz.getAnnotation(Document.class);
        if (document != null) {
            return document.collection();
        }
        return clazz.getSimpleName();
    }

    public static MongoTemplate getCommonsTemplate() {
		return commonsTemplate;
	}

    public static String getModuleHost(String sModule) {
        Enumeration<Object> bundleKeys = dataSourceProperties.keys();
        while (bundleKeys.hasMoreElements()) {
            String key = (String) bundleKeys.nextElement();
            
            if (sModule.equals(key.replaceAll("\\*", ""))) {
            	String[] datasourceInfo = dataSourceProperties.getProperty(key).split(",");
            	return datasourceInfo[0];
            }
        }
        return null;
    }
    
	public static boolean isModuleAvailableForWriting(String sModule) {
		Set<String> projects = currentlyImportedProjects.get(sModule);
		if (projects != null) {
			return projects.size() == 0;
		} else {
			return true;
		}
	}

	public static void lockProjectForWriting(String sModule, String sProject) {
		Set<String> projects = currentlyImportedProjects.get(sModule);
		if (projects != null) {
			projects.add(sProject);
		} else {
			projects = new HashSet<String>();
			projects.add(sProject);
			currentlyImportedProjects.put(sModule, projects);
		}
	}

	public static void unlockProjectForWriting(String sModule, String sProject) {
		Set<String> moduleLockedProjects = currentlyImportedProjects.get(sModule);
		if (moduleLockedProjects == null)
			throw new NoSuchElementException("There are currently no locked projects in database " + sModule);
		moduleLockedProjects.remove(sProject);
	}

	public static void lockModuleForWriting(String sModule) {
		Set<String> projects = currentlyImportedProjects.get(sModule);
		if (projects != null) {
			projects.add(null);
		} else {
			projects = new HashSet<String>();
			projects.add(null);
			currentlyImportedProjects.put(sModule, projects);
		}
	}

	public static void unlockModuleForWriting(String sModule) {
		Set<String> projects = currentlyImportedProjects.get(sModule);
		if (projects != null) {
			projects.clear();
		}
	}
	
    public static void updateDatabaseLastModification(String sModule) {
    	MongoTemplateManager.updateDatabaseLastModification(sModule, new Date(), false);
    }
    
    public static void updateDatabaseLastModification(String sModule, Date lastModification, boolean restored) {
    	MongoTemplate template = MongoTemplateManager.get(sModule);
    	
    	Update update = new Update();
    	update.set(DatabaseInformation.FIELDNAME_LAST_MODIFICATION, lastModification);
    	update.set(DatabaseInformation.FIELDNAME_RESTORE_DATE, restored ? new Date() : null);
    	template.upsert(new Query(), update, "dbInfo");
    }
    
    public static DatabaseInformation getDatabaseInformation(String sModule) {
    	MongoTemplate template = MongoTemplateManager.get(sModule);
    	return template.findOne(new Query(), DatabaseInformation.class, "dbInfo");
    }
    
    public static String getDatabaseName(String sModule) {
    	String sModuleKey = (isModulePublic(sModule) ? "*" : "") + sModule + (isModuleHidden(sModule) ? "*" : "");
    	String dataSource = dataSourceProperties.getProperty(sModuleKey);
    	return dataSource.split(",")[1];
    }
    
    public static List<String> getServerHosts(String sHost) {
    	MongoClient client = mongoClients.get(sHost);
    	ClusterDescription cluster = client.getClusterDescription();
    	List<ServerDescription> servers = cluster.getServerDescriptions();
    	List<String> hosts = new ArrayList<String>();
    	for (ServerDescription desc : servers) {
    		ServerAddress address = desc.getAddress();
    		hosts.add(address.getHost() + ":" + address.getPort());
    	}
    	return hosts;
    }
}
