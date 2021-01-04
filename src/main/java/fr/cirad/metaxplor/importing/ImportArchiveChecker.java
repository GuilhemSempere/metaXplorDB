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
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.biojava.bio.seq.DNATools;
import org.biojava.bio.symbol.SoftMaskedAlphabet;
import org.biojavax.bio.seq.RichSequence;
import org.biojavax.bio.seq.RichSequenceIterator;
import org.springframework.context.support.GenericXmlApplicationContext;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import fr.cirad.metaxplor.model.Accession;
import fr.cirad.metaxplor.model.AssignedSequence;
import fr.cirad.metaxplor.model.Assignment;
import fr.cirad.metaxplor.model.DBField;
import fr.cirad.metaxplor.model.Sample;
import fr.cirad.metaxplor.model.Sequence;
import fr.cirad.tools.Helper;
import fr.cirad.tools.ProgressIndicator;
import fr.cirad.tools.mongo.DBConstant;
import fr.cirad.tools.mongo.MongoTemplateManager;
import htsjdk.samtools.reference.FastaSequenceIndexCreator;

/**
 * Class to test contents of .tsv files before import
 *
 * @author petel, sempere
 */
public class ImportArchiveChecker {

    private ImportArchiveChecker() {
    }
    
    /**
     * logger
     */
    private static final Logger LOG = Logger.getLogger(ImportArchiveChecker.class);
    /**
     * Default value if a field is not specified
     */
    public final static List<String> EMPTY_FIELD_CODES = Arrays.asList("", ".");
    /**
     * check if a String can be cast to double
     */
    private static final Pattern DOUBLE_PATTERN = Pattern.compile(
            "[\\x00-\\x20]*[+-]?(NaN|Infinity|((((\\p{Digit}+)(\\.)?((\\p{Digit}+)?)"
            + "([eE][+-]?(\\p{Digit}+))?)|(\\.((\\p{Digit}+))([eE][+-]?(\\p{Digit}+))?)|"
            + "(((0[xX](\\p{XDigit}+)(\\.)?)|(0[xX](\\p{XDigit}+)?(\\.)(\\p{XDigit}+)))"
            + "[pP][+-]?(\\p{Digit}+)))[fFdD]?))[\\x00-\\x20]*");
    /**
     * check for YYYY-MM-dd fromat
     */
    private static final Pattern DATE_PATTERN = Pattern.compile("^\\d{4}[\\-\\/\\s]?((((0[13578])|(1[02]))[\\-\\/\\s]?(([0-2][0-9])|(3[01])))|(((0[469])|(11))[\\-\\/\\s]?(([0-2][0-9])|(30)))|(02[\\-\\/\\s]?[0-2][0-9]))$");

    public static void main(String[] args) throws Exception {
		GenericXmlApplicationContext ctx = null;
		try
		{
			MongoTemplate mongoTemplate = MongoTemplateManager.get(args[0]);
			if (mongoTemplate == null)
			{	// we are probably being invoked offline
				ctx = new GenericXmlApplicationContext("applicationContext-data.xml");
	
				MongoTemplateManager.initialize(ctx);
				mongoTemplate = MongoTemplateManager.get(args[0]);
				if (mongoTemplate == null)
					throw new Exception("DATASOURCE '" + args[0] + "' is not supported!");
			}
			
			System.out.println(testAssignmentFile(mongoTemplate, new FileInputStream(args[1]), null, new HashMap<>(), null));
		}
		finally
		{
			if (ctx != null)
				ctx.close();
		}
    }
    
    /**
     * Check whether the Sequence composition file is correctly formatted
     * 
     * @param zis
     * @param numberOfEntriesToCheck
     * @param sampleCodes
     * @param progress
     * @param nSeqCount
     * @return "ok" if no error, the error message otherwise
     * @throws java.io.IOException
     */
	public static String testSequenceFile(InputStream zis, Integer numberOfEntriesToCheck, Collection<String> sampleCodes, ProgressIndicator progress, int nSeqCount) throws IOException {
    	if (progress != null) {
     	   	progress.addStep("Checking sequence composition file");
     	   	progress.moveToNextStep();
    		progress.setPercentageEnabled(true);
    	}

        BufferedReader br = new BufferedReader(new InputStreamReader(zis, Charset.forName("UTF-8")));

        String line = br.readLine();
        List<String> headers = Arrays.asList(line.split("\t"));

        int qseqidColumnIndex = -1;

        List<String> missingSampleColumns = new ArrayList<>(sampleCodes);
        for (int i=0; i<headers.size(); i++) {
        	String header = headers.get(i);
        	if (!missingSampleColumns.remove(header) && standardizeHeader(header).equals(Sequence.FIELDNAME_QSEQID))
        		qseqidColumnIndex = i;
        }
        if (qseqidColumnIndex == -1)
        	return "No column named " + Sequence.FIELDNAME_QSEQID + " in sequence composition file";
        if (missingSampleColumns.size() > 0)
        	return "No column found in sequence composition file for sample(s): " + StringUtils.join(missingSampleColumns, ", ");

        HashSet<String> encounteredSequences = new HashSet<>();
        int lineNb = 1;
        while ((line = br.readLine()) != null) {
            lineNb++;
	        List<String> fields = Helper.split(line, '\t');
	        if (headers.size() > fields.size())
	        	return "Invalid number of fields, got " + headers.size() + " headers but " + fields.size() + " fields at line " + lineNb;

	        int nTotalCount = 0;
	        for (int i=0; i<fields.size(); i++) {
	            if (i != qseqidColumnIndex && !fields.get(i).isEmpty())
            		try {
            			nTotalCount += Integer.parseInt(fields.get(i));                     		
            		}
	            	catch (NumberFormatException nfe) {
	            		return "Invalid sample contribution value '" + fields.get(i) + "' for column " + headers.get(i) + " at line " + lineNb + " (only integers accepted)";
	            	}
	        }
	        if (nTotalCount <= 0)
	        	return "Invalid total sample contribution for line " + lineNb + ": " + nTotalCount;

            if (numberOfEntriesToCheck != null && numberOfEntriesToCheck <= lineNb)
            	break;

            if (progress != null && nSeqCount > 0) {
	            encounteredSequences.add(fields.get(qseqidColumnIndex));
	            if (encounteredSequences.size() % 1000 == 0)
	            	progress.setCurrentStepProgress(encounteredSequences.size() * 100 / (numberOfEntriesToCheck != null ? numberOfEntriesToCheck  : nSeqCount));
            }
        }

        return "ok";
	}

    /**
     * Check whether the Assignment file is correctly formatted, and build lists of existing / provided fields for user information
     * 
     * @param mongoTemplate 
     * @param is
     * @param numberOfEntriesToCheck
     * @param assignmentFieldsToFill
     * @param progress 
     * @return "ok" if no error, the error message otherwise
     * @throws java.io.IOException
     * @throws InterruptedException 
     */
    public static String testAssignmentFile(MongoTemplate mongoTemplate, InputStream is, Integer numberOfEntriesToCheck, Map<String, Object> assignmentFieldsToFill, ProgressIndicator progress) throws IOException {
    	if (progress != null) {
	 	   	progress.addStep("Checking assignment file");
	 	   	progress.moveToNextStep();
    		progress.setPercentageEnabled(numberOfEntriesToCheck != null);
    	}

        BufferedReader br = new BufferedReader(new InputStreamReader(is, Charset.forName("UTF-8")));

        String line = br.readLine();
        List<String> headers = Arrays.asList(line.split("\t")), stdHeaders = standardizeHeaders(headers);

        assignmentFieldsToFill.put("provided", stdHeaders.stream().sorted(Comparator.comparing(String::toString)).collect(Collectors.toList()));

        List<String> requiredHeaders = new ArrayList<>();
        requiredHeaders.addAll(DBField.getRequiredFields().get(AssignedSequence.FIELDNAME_ASSIGNMENT));

        String sMissingHeaders = "";
        for (String requiredHeader : requiredHeaders)
            if (!stdHeaders.contains(requiredHeader))
            	sMissingHeaders += (sMissingHeaders.length() == 0 ? "" : ", ") + requiredHeader;

        if (sMissingHeaders.length() > 0)
        	return "Missing header column(s): " + sMissingHeaders;
        
        HashMap<String /*qseqid*/, HashMap<String /*assign_method*/, Integer[] /*number of assignments and number of best hit assignments*/>> consistencyData = new HashMap<>(); 

        int qseqidColumnIndex = stdHeaders.indexOf(Sequence.FIELDNAME_QSEQID), taxidColumnIndex = stdHeaders.indexOf(DBConstant.FIELDNAME_TAXON), sseqidColumnIndex = stdHeaders.indexOf(Assignment.FIELDNAME_SSEQID), assignMethodColumnIndex = stdHeaders.indexOf(Assignment.FIELDNAME_ASSIGN_METHOD), bestHitColumnIndex = stdHeaders.indexOf(DBField.bestHitFieldName);
        
        if (taxidColumnIndex == -1 && sseqidColumnIndex == -1)
        	return "Header columns must contain at least one of " + Assignment.FIELDNAME_SSEQID + ", " + DBConstant.FIELDNAME_TAXON;
        
        int lineNb = 1;
        while ((line = br.readLine()) != null) {
            lineNb++;       
            String[] fields = line.split("\t");

            if (EMPTY_FIELD_CODES.contains(fields[qseqidColumnIndex])) 
            	return "Missing " + Sequence.FIELDNAME_QSEQID + " on line " + lineNb;
            
            boolean fGotSseqId = sseqidColumnIndex != -1 && !EMPTY_FIELD_CODES.contains(fields[sseqidColumnIndex]), fGotTaxId = taxidColumnIndex != -1 && !EMPTY_FIELD_CODES.contains(fields[taxidColumnIndex]);
            if (!fGotSseqId && !fGotTaxId) 
            	return "Neither " + Assignment.FIELDNAME_SSEQID + " nor " + DBConstant.FIELDNAME_TAXON + " found on line " + lineNb;
            
            if (fGotSseqId) {
            	for (String aSseqID : fields[sseqidColumnIndex].split(","))
            		if (!aSseqID.startsWith(Accession.ID_NUCLEOTIDE_PREFIX) && !aSseqID.startsWith(Accession.ID_PROTEIN_PREFIX))
            			return "Missing accession prefix on line " + lineNb;
            }
            
            HashMap<String, Integer[]> consistencyDataForSequence = consistencyData.get(fields[qseqidColumnIndex]);
            if (consistencyDataForSequence == null) {
            	consistencyDataForSequence = new HashMap<>();
            	consistencyData.put(fields[qseqidColumnIndex], consistencyDataForSequence);
            }
            Integer[] consistencyDataForAssignmentMethod = consistencyDataForSequence.get(fields[assignMethodColumnIndex]);
            if (consistencyDataForAssignmentMethod == null) {
            	consistencyDataForAssignmentMethod = new Integer[2];
            	consistencyDataForSequence.put(fields[assignMethodColumnIndex], consistencyDataForAssignmentMethod);
            }
            consistencyDataForAssignmentMethod[0] = consistencyDataForAssignmentMethod[0] == null ? 1 : consistencyDataForAssignmentMethod[0] + 1;
            consistencyDataForAssignmentMethod[1] = (consistencyDataForAssignmentMethod[1] == null ? 0 : consistencyDataForAssignmentMethod[1]) + (bestHitColumnIndex == -1 || fields.length < bestHitColumnIndex + 1 || EMPTY_FIELD_CODES.contains(fields[bestHitColumnIndex]) ? 0 : 1);

            if (numberOfEntriesToCheck != null && numberOfEntriesToCheck < lineNb)
            	break;
            
            if (progress != null)
	            progress.setCurrentStepProgress(numberOfEntriesToCheck != null ? lineNb * 100 / numberOfEntriesToCheck : lineNb);
        }
        
        for (String qseqid : consistencyData.keySet()) {
        	HashMap<String, Integer[]> consistencyDataForSequence = consistencyData.get(qseqid);
        	for (String assignMethod : consistencyDataForSequence.keySet()) {
        		Integer[] consistencyDataForAssignmentMethod = consistencyDataForSequence.get(assignMethod);
        		if (consistencyDataForAssignmentMethod[1] > 1)
        			return "Several best hits specified for qseqid " + qseqid + " and assignment method " + assignMethod;
        		if (consistencyDataForAssignmentMethod[1] == 0 && consistencyDataForAssignmentMethod[0] > 1)
        			return "No best hit specified for qseqid " + qseqid + " and assignment method " + assignMethod;
        	}
        }

        Set<String> existingFields = new TreeSet<>();
        for (DBField existingField : mongoTemplate.find(new Query(Criteria.where(DBField.FIELDNAME_ENTITY_TYPEALIAS).is(AssignedSequence.FIELDNAME_ASSIGNMENT)), DBField.class))
       		existingFields.add(existingField.getFieldName());
        if (existingFields.size() > 0)
        	existingFields.add(Sequence.FIELDNAME_QSEQID);
        assignmentFieldsToFill.put("existing", existingFields);
        
        return "ok";
    }
    
    /**
     * Check whether the sample file is correctly formatted
     * @param mongoTemplate 
     * @param is
     * @param sampleFieldsToFill 
     * @param sampleCodesToFill 
     * @return "ok" if no error, the error message otherwise
     * @throws java.io.IOException
     * @throws ClassNotFoundException 
     */
    public static String testSampleFile(MongoTemplate mongoTemplate, InputStream is, Map<String, Object> sampleFieldsToFill, Collection<String> sampleCodesToFill) throws IOException, ClassNotFoundException {

        String response = "ok";
        BufferedReader br = new BufferedReader(new InputStreamReader(is, Charset.forName("UTF-8")));

        String line = br.readLine();
        List<String> headers = Arrays.asList(line.split("\t")), stdHeaders = standardizeHeaders(headers);
        sampleFieldsToFill.put("provided", new TreeSet<>(stdHeaders));

        List<String> requiredHeaders = new ArrayList<>();
        requiredHeaders.addAll(DBField.getRequiredFields().get(Sample.TYPE_ALIAS));

        String sMissingHeaders = "";
        for (String requiredHeader : requiredHeaders)
            if (!stdHeaders.contains(requiredHeader))
            	sMissingHeaders += (sMissingHeaders.length() == 0 ? "" : ", ") + requiredHeader;

        if (sMissingHeaders.length() > 0)
        	return "Missing header column(s) in sample file: " + sMissingHeaders;

        int gpsColumnIndex = stdHeaders.indexOf(Sample.FIELDNAME_COLLECT_GPS);
        int dateColumnIndex = stdHeaders.indexOf(Sample.FIELDNAME_COLLECT_DATE);
        
        HashMap<String, DBField> existingFields = new HashMap<>();
        for (DBField existingField : mongoTemplate.find(new Query(Criteria.where(DBField.FIELDNAME_ENTITY_TYPEALIAS).is(Sample.TYPE_ALIAS)), DBField.class))
        	existingFields.put(existingField.getEntityTypeAlias() + "§" + existingField.getFieldName(), existingField);
        List<String> existingFieldNames = existingFields.values().stream().sorted(Comparator.comparing(DBField::getFieldName)).map(dbf -> dbf.getFieldName()).collect(Collectors.toList());
        if (existingFieldNames.size() > 0)
        	existingFieldNames.add(Sample.FIELDNAME_SAMPLE_CODE);
        sampleFieldsToFill.put("existing", standardizeHeaders(existingFieldNames));
        
        mainLoop: while ((line = br.readLine()) != null) {
            String[] fields = line.split("\t");
            String sampleName = fields[headers.indexOf(Sample.FIELDNAME_SAMPLE_CODE)];
            if (stdHeaders.size() != fields.length) {
                response = "Invalid number of fields, got " + headers.size() + " headers but " + fields.length + " fields for sample " + sampleName;
                break;
            }

            String dateField = fields[dateColumnIndex];
            if (!EMPTY_FIELD_CODES.contains(dateField)) {
                if (!DATE_PATTERN.matcher(dateField).matches()) {
                    response = "Invalid date for column " + headers.get(dateColumnIndex) + " and sample " + sampleName + ".\nExpected format is YYYY-MM-dd";
                    break;
                }
            }

            String gpsField = fields[gpsColumnIndex];
            if (!EMPTY_FIELD_CODES.contains(gpsField)) {
                String[] pos = gpsField.split(",");
                if (pos.length < 2 || !DOUBLE_PATTERN.matcher(pos[0]).matches() || !DOUBLE_PATTERN.matcher(pos[1]).matches()) {
                    response = "Invalid gps position for column " + headers.get(gpsColumnIndex) + " and sample " + sampleName + ".\nExpected format is [lat, long]";
                    break;
                }
            }

            // make sure we don't have an existing field with a different type
            for (int i=0; i<fields.length; i++) {
            	String colName = headers.get(i);
            	if (colName.equals(Sample.FIELDNAME_SAMPLE_CODE)) {
            		sampleCodesToFill.add(sampleName);
            		continue;
            	}

            	DBField dbField = existingFields.get(Sample.TYPE_ALIAS + "§" + colName);
            	if (dbField != null)
            	{
            		boolean fGotDoubleInFile = MtxImport.parseToDouble(fields[i]) != null;
            		if ((dbField.getTypeClass().equals(Double.class) && !fGotDoubleInFile)/* || (dbField.getType().equals(String.class) && fGotDoubleInFile)*/)
            		{
                        response = "Invalid type for field " + colName + " and sample " + sampleName + ".\nExpected type is " + dbField.getTypeClass().getSimpleName().replaceAll("\\.class",  "");
                        break mainLoop;
            		}
            	}
            }
        }
        return response;
    }

    /**
     * tests that fasta file contents are correct
     *
     * @param is
     * @param numberOfEntriesToCheck
     * @return number of sequences
     * @throws java.io.Exception
     */
	public static int testFastaFile(InputStream is, Integer numberOfEntriesToCheck, ProgressIndicator progress) throws Exception {
		if (progress != null) {
     		progress.addStep("Checking fasta file structure");
     		progress.moveToNextStep();
		}

		File tmpFastaFile = File.createTempFile("tmp", Sequence.FULL_FASTA_EXT);
        Files.copy(is, tmpFastaFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
		int n = 0;
		
		try {
			// build an index to be sure it will pass when we actually import it (it may reveal errors such as blank lines)
			try {
				try {
					FastaSequenceIndexCreator.buildFromFasta(tmpFastaFile.toPath()).iterator();
				}
				catch (StringIndexOutOfBoundsException siobe) {
	        		if ("String index out of range: 0".equals(siobe.getMessage()))
	                	throw new Exception("Invalid fasta file: make sure it contains no empty lines!");
	        	}
			}
			catch (Exception e) {
				throw new Exception("Error parsing fasta file: " + e.getMessage());
			}

			// check that all sequences are correct
			if (progress != null) {
	     		progress.addStep("Checking fasta file sequences");
	     		progress.moveToNextStep();
				progress.setPercentageEnabled(numberOfEntriesToCheck != null);
			}
			try {
				RichSequenceIterator iterator = RichSequence.IOTools.readFasta(new BufferedReader(new FileReader(tmpFastaFile)), SoftMaskedAlphabet.getInstance(DNATools.getDNA()).getTokenization("token"), null);
				while (iterator.hasNext()) {
					iterator.nextSequence().seqString();
					if (progress != null && n % 1000 == 0)
						progress.setCurrentStepProgress(numberOfEntriesToCheck != null ? n * 100 / numberOfEntriesToCheck : n);
					
					n++;
		            if (numberOfEntriesToCheck != null && numberOfEntriesToCheck <= n)
		            	break;
				}
			}
			catch (Exception e) {
				throw new Exception("Error parsing sequence number " + ++n + ": " + e.getMessage());
			}
		
		} finally {
			tmpFastaFile.delete();
		}
		return n;
	}

    /**
     * standardize header name
     *
     * @param headers
     * @return
     */
    public static String standardizeHeader(String header) {
        return header.toLowerCase().replaceAll("[^A-Za-z0-9]", "_");
    }

    /**
     * standardize names in header list
     *
     * @param headers
     * @return
     */
    public static List<String> standardizeHeaders(Collection<String> headers) {
        List<String> stdHeaders = new ArrayList<>();
        for (String header : headers)
            stdHeaders.add(standardizeHeader(header));
        return stdHeaders;
    }
}