package msa;

import java.util.*;

import com.google.gson.Gson;

import align.AnnotationGridElement;
import align.AnnotationSequenceGrid;
import align.GenAnnotationGrid;
import align.SmithWatermanDim;
import msa.ProfileReader.Order;
import msa.db.CassandraDBInterface;
import msa.db.MSADBException;
import msa.db.MSADBInterface;
import msa.db.MySQLDBInterface;
import nlputils.sequence.SequenceUtilities;
import utils.db.DBConnection;

import java.io.*;
import java.sql.*;

public class AutoAnnotate
{
	private Gson gson;
	private Map<String, MultipleSequenceAlignment> msaMap;
	private List<MultipleSequenceAlignment> msaList;
	private List<List<String>> profileList;
	private List<String> profileStrList;
	private Boolean punct;
	private List<AnnotationSequence> posSeqList;
	private List<AnnotationSequence> negSeqList;
	
	private String host;
	private String dbType;
	private String docNamespace;
	private String docTable;
	private String keyspace;
	private String msaKeyspace;
	
	private String docDBHost;
	private String docDBName;
	private String docDBQuery;
	private String docDBType;
	
	
	//private String profileAnnotType;
	//private String negProfileAnnotType;

	//private String group;
	private boolean write;
	private boolean writeAnnots;
	//private String provenance;
	
	private boolean verbose;
	private List<Map<String, Object>> msaAnnotFilterList;
	private List<Map<String, Object>> contextAnnotFilterList;
	private List<Double> scoreList;
	private int limit;
	private boolean requireTarget = false;
	
	private MSADBInterface db;
	private Connection docDBConn;
	private Connection conn;
	private Connection conn2;
	private PreparedStatement pstmt;
	private PreparedStatement pstmt2;
	private PreparedStatement pstmt3;
	private PreparedStatement pstmtWrite;
	private PreparedStatement pstmtCheck;
	private PreparedStatement pstmtDeleteAnnots;
	
	private int maxGaps = 1;
	private int syntax = 1;
	private int phrase = 0;
	//private int maxGapsContext = 10;
	
	private List<Long> docIDList = null;
	
	private Map<String, String> ansMap;
	private Map<String, String> negAnsMap;
	private Map<Long, List<int[]>> ansRangeMap;
	private Map<String, Boolean> matchMap;
	private Map<String, AnnotationSequence> ansSeqMap;
	
	private List<String> ansAnnotTypeList;
	private List<Annotation> finalAnnotList;
	private List<ProfileMatch> finalMatchList;
	private List<String> goldAnnotTypeList;
	private String targetType;
	private String targetProvenance;
	private String autoProvenance;
	//private Map<String, Boolean> extractMap;
	
	private GenContext genContext;
	private boolean contextFlag = false;
	private PrintWriter pw;
	
	private GenAnnotationGrid genGrid;
	private ProfileMatcher profileMatcher;
	private String tokType;
	
	private SmithWatermanDim sw;
	private List<ProfileMatch> noMatchList;
	
	private Map<Long, MSAProfile> profileIDMap;
	private int profileMinTotal;
	private double profileMinPrec;
	private double minMatchPrec;
	private double minMatchTotal;
	private Boolean filterAllCaps;
	
	private String finalTable = "msa_profile_target_final";
	private String profileTable = "msa_profile";
	
	private String autoMatchTable;
	private String runName;
	
	private List<String> annotTypeList;
	private List<String> profileTableList;
	private List<String> indexTableList;
	private List<String> finalTableList;
	
	private boolean evalFlag;
	
	
	
	public AutoAnnotate()
	{
		gson = new Gson();
		genContext = new GenContext();
		sw = new SmithWatermanDim();
		profileMatcher = new ProfileMatcher();
		//profileMatcher.setProfileMatch(true);
	}
	
	public void setProfileTable(String profileTable)
	{
		this.profileTable = profileTable;
	}
	
	public void setFinalProfileTable(String finalProfileTable)
	{
		this.finalTable = finalProfileTable;
	}
	
	public void setAnnotTypeList(List<String> annotTypeList)
	{
		this.annotTypeList = annotTypeList;
	}
	
	public void setProfileTableList(List<String> profileTableList)
	{
		this.profileTableList = profileTableList;
	}
	
	public void setIndexTableList(List<String> indexTableList)
	{
		this.indexTableList = indexTableList;
	}
	
	public void setFinalTableList(List<String> finalTableList)
	{
		this.finalTableList = finalTableList;
	}
	
	public void init(String config)
	{
		try {
			Properties props = new Properties();
			props.load(new FileReader(config));
			init(props);
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	public void init(Properties props)
	{
		try {			
			host = props.getProperty("host");
			keyspace = props.getProperty("keyspace");
			msaKeyspace = props.getProperty("msaKeyspace");
			dbType = props.getProperty("dbType");
			
			docDBHost = props.getProperty("docDBHost");
			docDBType = props.getProperty("docDBType");
			docNamespace = props.getProperty("docNamespace");
			docTable = props.getProperty("docTable");
			targetType = props.getProperty("targetAnnotType");
			
			//profileAnnotType = props.getProperty("profileAnnotType");
			//negProfileAnnotType = props.getProperty("negProfileAnnotType");

			//provenance = props.getProperty("provenance");
			
			punct = Boolean.parseBoolean(props.getProperty("punctuation"));
			write = Boolean.parseBoolean(props.getProperty("write"));
			writeAnnots = Boolean.parseBoolean(props.getProperty("writeAnnots"));
			verbose = Boolean.parseBoolean(props.getProperty("verbose"));
			maxGaps = Integer.parseInt(props.getProperty("maxGaps"));
			syntax = Integer.parseInt(props.getProperty("syntax"));
			phrase = Integer.parseInt(props.getProperty("phrase"));
			//maxGapsContext = Integer.parseInt(props.getProperty("maxGapsContext"));
			requireTarget = Boolean.parseBoolean(props.getProperty("requireTarget"));
			contextFlag = Boolean.parseBoolean(props.getProperty("contextFlag"));
			tokType = props.getProperty("tokType");
			
			
			msaAnnotFilterList = new ArrayList<Map<String, Object>>();
			msaAnnotFilterList = gson.fromJson(props.getProperty("msaAnnotFilterList"), msaAnnotFilterList.getClass());
			
			//contextAnnotFilterList = new ArrayList<Map<String, Object>>();
			//contextAnnotFilterList = gson.fromJson(props.getProperty("contextAnnotFilterList"), annotFilterList.getClass());
			
			ansAnnotTypeList = new ArrayList<String>();
			ansAnnotTypeList = gson.fromJson(props.getProperty("ansAnnotTypeList"), ansAnnotTypeList.getClass());
			
			scoreList = new ArrayList<Double>();
			scoreList = gson.fromJson(props.getProperty("scoreList"), scoreList.getClass());
			
			targetType = props.getProperty("targetType");
			targetProvenance = props.getProperty("targetProvenance");
			autoProvenance = props.getProperty("autoProvenance");
			
			profileMinTotal = Integer.parseInt(props.getProperty("profileMinTotal"));
			profileMinPrec = Double.parseDouble(props.getProperty("profileMinPrec"));
			
			//minMatchTotal = Integer.parseInt(props.getProperty("minMatchTotal"));
			//minMatchPrec = Double.parseDouble(props.getProperty("minMatchPrec"));
			
			filterAllCaps = Boolean.parseBoolean(props.getProperty("filterAllCaps"));
			
			finalTable = props.getProperty("finalProfileTable");
			profileTable = props.getProperty("profileTable");
			autoMatchTable = props.getProperty("autoMatchTable");
			runName = props.getProperty("runName");
			
			limit = Integer.parseInt(props.getProperty("limit"));
			
			String outFile = props.getProperty("autoOutFile");
			evalFlag = Boolean.parseBoolean(props.getProperty("evalFlag"));
			
			
			pw = new PrintWriter(outFile);
			
			if (requireTarget) {
				Map<String, Object> targetMap = new HashMap<String, Object>();
				targetMap.put("annotType", targetType);
				targetMap.put("provenance", targetProvenance);
				targetMap.put("targetStr", ":target");
				msaAnnotFilterList.add(targetMap);
			}
			
			
			//add annotation type tags from previous runs
			Map<String, Object> targetMap = new HashMap<String, Object>();
			targetMap.put("annotType", targetType);
			targetMap.put("provenance", "conll2003-final");
			targetMap.put("literal", ":" + targetType.toLowerCase());
			List<String> featureList = new ArrayList<String>();
			featureList.add("$literal");
			targetMap.put("features", featureList);
			msaAnnotFilterList.add(targetMap);
			
			
			
			docDBQuery = props.getProperty("docDBQuery");
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	public void close()
	{
		try {
			pw.close();
			db.close();
			
			if (docDBConn != null)
				docDBConn.close();
			
			conn.close();
			conn2.close();
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	public void annotate(String msaUser, String msaPassword, String docUser, String docPassword)
	{
		System.out.println("auto annotate...");
		
		finalAnnotList = new ArrayList<Annotation>();
		finalMatchList = new ArrayList<ProfileMatch>();
		//extractMap = new HashMap<String, Boolean>();
		
		try {
			//init DB connections
			db = new MySQLDBInterface();
			db.init(msaUser, msaPassword, host, keyspace, msaKeyspace);
			
			conn = DBConnection.dbConnection("fmeng", "fmeng", host, keyspace, "mysql");
			conn2 = DBConnection.dbConnection("fmeng", "fmeng", host, "msa", "mysql");
			pstmt = conn.prepareStatement("select max(id) from annotation where document_namespace = '" + docNamespace + "' and document_table = '" + docTable + "' "
				+ "and document_id = ?");
			pstmt2 = conn.prepareStatement("insert into annotation (id, document_namespace, document_table, document_id, annotation_type, start, end, value, features, provenance, score) "
				+ "values (?, '" + docNamespace + "', '" + docTable + "',?,?,?,?,?,?,?,?)");
			
			pstmt3 = conn.prepareStatement("select distinct a.annotation_type, a.value, a.document_id, a.start, a.end from annotation a, annotation b "
				+ "where a.provenance = 'conll2003-token' and b.value = ? and a.start >= b.start and a.end <= b.end and a.document_id = b.document_id "
				+ "and a.document_id in (select distinct c.document_id from ner.conll2003_document c where c.`group` = 'train')");
			
			pstmtWrite = conn2.prepareStatement("insert into " + autoMatchTable + " (profile_id, document_id, start, end, target_id, run_name) values (?,?,?,?,?,'" + runName + "')");
			
			pstmtCheck = conn2.prepareStatement("select count(*) from " + autoMatchTable + " where document_id = ? and start = ? and end = ? and run_name = '" + runName + "'");
			
			pstmtDeleteAnnots = conn.prepareStatement("delete from annotation where provenance = '" + autoProvenance + "'");
			
			
			

			
			
			//check for docID list info			
			if (docDBQuery != null) {				
				docDBConn = DBConnection.dbConnection(docUser, docPassword, docDBHost, docNamespace, docDBType);	
				docIDList = MSAUtils.getDocIDList(docDBConn, docDBQuery);
				docDBConn.close();
			}
			
			for (int index=0; index<annotTypeList.size(); index++) {
				targetType = annotTypeList.get(index);
				profileTable = profileTableList.get(index);
				finalTable = finalTableList.get(index);
				
				if (targetType.length() == 0 || profileTable.length() == 0 || finalTable.length() == 0)
					continue;
				
				
			
				ansMap = new TreeMap<String, String>();
				negAnsMap = new TreeMap<String, String>();
				ansRangeMap = new HashMap<Long, List<int[]>>();
				ansSeqMap = new HashMap<String, AnnotationSequence>();
				
				List<String> annotTypeNameList = MSAUtils.getAnnotationTypeNameList(msaAnnotFilterList, tokType);
				annotTypeNameList.add(":" + targetType.toLowerCase());
				scoreList.add(10.0);
				
				genGrid = new GenAnnotationGrid(annotTypeNameList, tokType);
				
				sw.setScoreMap(annotTypeNameList, scoreList);
				//sw.setVerbose(true);
				profileMatcher.setAligner(sw);
				profileMatcher.setPrintWriter(pw);
				profileMatcher.setProfileMatch(true);
				
				GenSentences genSent = new GenSentences();
				genSent.setRequireTarget(requireTarget);
				genSent.setPunct(punct);
				genSent.setVerbose(verbose);
				genSent.setTokenType(tokType);
				genSent.setMaskTarget(true);
				genSent.init(db, msaAnnotFilterList, targetType, targetProvenance);
				
				System.out.println("gen sents...");
				if (docIDList == null)
					genSent.genSentences(docNamespace, docTable, null, limit);
				else {
					genSent.setDocIDList(docIDList);
					genSent.genSentenceAnnots(docNamespace, docTable);
				}
				
				//get sequences
				//posSeqList = genSent.getPosSeqList();
				negSeqList = genSent.getNegSeqList();
				
				
				//filter all caps sentences because all words get tagged as NNP
				if (filterAllCaps) {
					for (int i=0; i<negSeqList.size(); i++) {
						AnnotationSequence seq = negSeqList.get(i);
						List<Annotation> orthList = seq.getAnnotList(":token|orth");
						
						if (orthList == null)
							continue;
						
						boolean allCaps = true;
						for (int j=0; j<orthList.size(); j++) {
							Annotation orthAnnot = orthList.get(j);
							
							String orth = (String) orthAnnot.getFeature("orth");
							String root = (String) orthAnnot.getFeature("root");
							if (Character.isLetter(root.charAt(0)) && !orth.equals("allCaps")) {
								allCaps = false;
								break;
							}
						}
						
						if (allCaps) {
							negSeqList.remove(i);
							i--;
						}
					}
				}
				
				
				System.out.println("reading answers...");
				for (long docID : docIDList)
					readAnswers(targetType, docID, negSeqList);
	
	
				
				//get grids
				System.out.println("getting grids...");
				/*
				List<AnnotationSequenceGrid> posGridList = new ArrayList<AnnotationSequenceGrid>();
				for (AnnotationSequence seq : posSeqList) {
					List<AnnotationSequenceGrid> gridList = genGrid.toAnnotSeqGrid(seq, true, false);
					posGridList.addAll(gridList);
				}
				*/
				
				List<AnnotationSequenceGrid> negGridList = new ArrayList<AnnotationSequenceGrid>();
				for (AnnotationSequence seq : negSeqList) {
					List<AnnotationSequenceGrid> gridList = genGrid.toAnnotSeqGrid(seq, false, false, false, true);
					negGridList.addAll(gridList);
				}
				
				
				//read profiles
				System.out.println("reading profiles...");
				ProfileReader reader = new ProfileReader();
				reader.setOrder(Order.DSC);
				reader.setMinScore(1.0);
				reader.init(msaUser, msaPassword, host, dbType, msaKeyspace);
	
				List<ProfileGrid> profileGridList = new ArrayList<ProfileGrid>();
				Map<AnnotationSequenceGrid, MSAProfile> msaProfileMap = new HashMap<AnnotationSequenceGrid, MSAProfile>();
				Map<AnnotationSequenceGrid, MSAProfile> msaTargetProfileMap = new HashMap<AnnotationSequenceGrid, MSAProfile>();
				
				Map<MSAProfile, List<MSAProfile>> profileMap = reader.readFinal(targetType, profileMinTotal, profileMinPrec, finalTable, profileTable);
				
				for (MSAProfile profile : profileMap.keySet()) {
					AnnotationSequenceGrid profileSeqGrid = genGrid.toAnnotSeqGrid(profile.getToks(), false);
					msaProfileMap.put(profileSeqGrid, profile);
					
					List<AnnotationSequenceGrid> targetSeqGridList = new ArrayList<AnnotationSequenceGrid>();
					List<MSAProfile> targetList = profileMap.get(profile);
					for (MSAProfile target : targetList) {
						AnnotationSequenceGrid targetGrid = genGrid.toAnnotSeqGrid(target.getToks(), false);
						
						MSAProfile target2 = msaTargetProfileMap.get(targetGrid);
						if (target2 == null)
							msaTargetProfileMap.put(targetGrid, target);
						
						targetSeqGridList.add(targetGrid);
					}
					
					ProfileGrid  profileGrid = new ProfileGrid(profileSeqGrid, targetSeqGridList);
					
					boolean inserted = false;
					for (int i=0; i<profileGridList.size(); i++) {
						ProfileGrid profileGrid2 = profileGridList.get(i);
						if (profileGrid2.getGrid().size() < profileGrid.getGrid().size()) {
							profileGridList.add(i, profileGrid);
							inserted = true;
							break;
						}
					}
					
					if (!inserted)
						profileGridList.add(profileGrid);
				}
				
				
				System.out.println("\n\nProfiles: " + profileGridList.size());
				pw.println("\n\nProfiles: " + profileGridList.size());
				for (ProfileGrid profileGrid : profileGridList) {
					String profileStr = gson.toJson(profileGrid.getGrid().getSequence().getToks());
	
					//System.out.println(profileStr);
					pw.println(profileStr);
					
					//System.out.println("Targets: ");
					pw.println("Targets:");
					List<AnnotationSequenceGrid> targetSeqGridList = profileGrid.getTargetGridList();
					for (AnnotationSequenceGrid targetSeqGrid : targetSeqGridList) {
						profileStr = gson.toJson(targetSeqGrid.getSequence().getToks());
						//System.out.println(profileStr);
						pw.println(profileStr);
					}
					
					//System.out.println("\n\n");
					pw.println("\n\n");
				}
				
				
	
				//match grids
				matchMap = new HashMap<String, Boolean>();
	
				List<ProfileMatch> matchList = profileMatcher.matchProfile(negGridList, profileGridList, targetType, true, maxGaps, syntax, phrase, false, msaProfileMap, msaTargetProfileMap);
				noMatchList = profileMatcher.getNoMatchList();
				
				
				List<String> features = new ArrayList<String>();
				features.add("$annotTypeName");
				
				
				
				for (ProfileMatch match: matchList) {
					
					//target can only be 1 token (only for CoNLL2003)
					String value = match.getTargetStr();
					
					/*
					if (value.indexOf(" ") >= 0 && value.length() > 3) {
						continue;
					}
					*/
					
					if (value.length() > 1 && !Character.isLetter(value.charAt(value.length()-1)))
						value = value.substring(0, value.length()-1);
					
					if (value.length() == 0)
						continue;
					
					
					AnnotationSequenceGrid grid = negGridList.get(match.getGridIndex());
					int[] targetCoords = match.getTargetIndexes();
					
					//extractMap.put(value, true);
					
					long docID = grid.getSequence().getDocID();
					
					
					//long start = targetAnnot.getStart();
					//long end = targetAnnot.getEnd();
					
					long start = targetCoords[0];
					long end = targetCoords[1];
					
					Annotation annot = new Annotation(docID, docNamespace, docTable, -1, targetType, 
						start, end, match.getTargetStr(), null);
					annot.setProvenance(autoProvenance);
					finalAnnotList.add(annot);
					finalMatchList.add(match);
					
					System.out.println(" ans: " + targetType + ", profile: " + match.getProfile().getProfileStr() + ", target: " + match.getTargetStr() + ", docID: " + match.getSequence().getDocID() + ", sent: " + match.getGridStr());
					pw.println(" ans: " + targetType + ", profile: " + match.getProfile().getProfileStr() + ", target: " + match.getTargetStr() + ", docID: " + match.getSequence().getDocID() + ", sent: " + match.getGridStr());

				}
				
				
				
				
	
				if (evalFlag) {
					eval(matchList, negGridList);
				}
				
				
				reader.close();
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}

	}
	
	public void eval(List<ProfileMatch> matchList, List<AnnotationSequenceGrid> negGridList)
	{
		//stats		

		
		List<String> features = new ArrayList<String>();
		features.add("$annotTypeName");

		Map<Long, Map<String, List<String>>> docAnsMap = new HashMap<Long, Map<String, List<String>>>();
		Map<String, Boolean> allValueMap = new HashMap<String, Boolean>();
		Map<String, double[]> precMap = new HashMap<String, double[]>();
		
		
		//for (ProfileMatch match : matchList) {
		
		/*
		for (int i=0; i<matchList.size(); i++) {
			ProfileMatch match = matchList.get(i);
			
			//target can only be 1 token (only for CoNLL2003)
			String value = match.getTargetStr();
			if (value.indexOf(" ") >= 0 && value.length() > 3) {
				continue;
			}
			
			if (value.length() > 1 && !Character.isLetter(value.charAt(value.length()-1)))
				value = value.substring(0, value.length()-1);
			
			if (value.length() == 0)
				continue;
			
			
			
			
			
//			double[] ret = precMap.get(value);
//			if (ret == null) {
//				ret = getPrec(value, "I-LOC", "I-MISC");
//				precMap.put(value, ret);
//			}
//			
//			double prec = ret[0] / (ret[0] + ret[1] + 0.00001);
//
//			
//			if ((ret[0] + ret[1] < minMatchTotal) && prec == 0.0) {
//				value = value.toLowerCase();
//				ret = getPrec(value, "I-LOC", "I-MISC");
//				prec = ret[0] / (ret[0] + ret[1] + 0.00001);
//			}
//			
//			
//			if (prec < minMatchPrec && (ret[0] + ret[1]) > minMatchTotal) {
//				matchList.remove(i);
//				i--;
//				
//				System.out.println("removing: " + value + ", " + prec + " | " + match.getGridStr());
//				pw.println("removing: " + value + ", " + prec + " | " + match.getGridStr());
//				continue;
//			}
			
			
			
			
			
			
			long matchDocID = match.getSequence().getDocID();
			Map<String, List<String>> valueMap = docAnsMap.get(matchDocID);
			if (valueMap == null) {
				valueMap = new HashMap<String, List<String>>();
				docAnsMap.put(matchDocID, valueMap);
			}				
			
			List<String> docAnsList = valueMap.get(value);
			if (docAnsList == null) {
				docAnsList = new ArrayList<String>();
				valueMap.put(value, docAnsList);
			}
			
			
			int[] indexes = match.getTargetIndexes();
			docAnsList.add(indexes[0] + "|" + indexes[1]);
			
			
			AnnotationSequenceGrid grid = negGridList.get(match.getGridIndex());
			int[] targetCoords = match.getTargetIndexes();
			
			//extractMap.put(value, true);
			
			long docID = grid.getSequence().getDocID();
			
			
			//long start = targetAnnot.getStart();
			//long end = targetAnnot.getEnd();
			
			long start = targetCoords[0];
			long end = targetCoords[1];
			
			Annotation annot = new Annotation(docID, docNamespace, docTable, -1, targetType, 
				start, end, match.getTargetStr(), null);
			annot.setProvenance(autoProvenance);
			*/
			
			
		for (Annotation annot : finalAnnotList) {
			
			long docID = annot.getDocID();
			long start = annot.getStart();
			long end = annot.getEnd();
			
			System.out.println(docID + "|" + start + "|" + end);
			
			
			String ansType = ansMap.get(docID + "|" + start + "|" + end);
			
			
			if (ansType == null) {
				ansType = ansMap.get(docID + "|" + start+1 + "|" + end+1);
				if (ansType != null) {
					start++;
					end++;
				}
			}
				
			if (ansType == null) {
				ansType = ansMap.get(docID + "|" + (start-1) + "|" + (end-1));
				
				if (ansType != null) {
					start--;
					end--;
				}
			}
				
			
			
			if (ansType == null) {
				ansType = ansMap.get(docID + "|" + (start-1) + "|" + (end));
				if (ansType != null) {
					start--;
				}
			}
			
			if (ansType == null) {
				ansType = ansMap.get(docID + "|" + (start+1) + "|" + (end));
				if (ansType != null) {
					start++;
				}
			}
			
			if (ansType == null) {
				ansType = ansMap.get(docID + "|" + start + "|" + (end+1));
				if (ansType != null) {
					end++;
				}
			}
			
			if (ansType == null) {
				ansType = ansMap.get(docID + "|" + start + "|" + (end-1));
				if (ansType != null) {
					end--;
				}
			}
			
			if (ansType == null) {
				ansType = ansMap.get(docID + "|" + (start-1) + "|" + (end+1));
				if (ansType != null) {
					start--;
					end++;
				}
			}
			
			Map<String, String> ansTypeMap = new HashMap<String, String>();
			String annotType = null;
			
			if (ansType != null) {
				ansTypeMap = (Map<String, String>) gson.fromJson(ansType, ansTypeMap.getClass());
				annotType = ansTypeMap.get("annotType");
			}
			
			if (annotType != null && annotType.equals(targetType)) {
				matchMap.put(docID + "|" + start + "|" + end, true);
				annot.setScore(1.0);
				System.out.print("Correct!: ");
				pw.print("Correct!: ");
			}
			else {
				annot.setScore(0.0);
				matchMap.put(docID + "|" + start + "|" + end, false);
				System.out.print("Wrong!: ");
				pw.print("Wrong!: ");
			}
			
			//AnnotationSequence seq = grid.getSequence();
			//seq.addAnnotation(annot, features, false, "");
			
			
		}
		
		
		
		
		/*
		//fill in values that were matched per document
		int index = 0;
		for (AnnotationSequence seq : negSeqList) {
			List<Annotation> annotList = seq.getAnnotList(":token|string");
			long docID = seq.getDocID();
			
			Map<String, List<String>> valueMap = docAnsMap.get(docID);
			if (valueMap == null)
				continue;
			
			for (Annotation annot : annotList) {
				long start = annot.getStart();
				long end = annot.getEnd();
				String value = annot.getValue().toLowerCase();
				
				Boolean flag = matchMap.get(docID + "|" + start + "|" + end);
				if (flag != null)
					continue;
				
				List<String> docAnsList = valueMap.get(value);
				if (docAnsList == null)
					continue;
				
				Double prec = precMap.get(value);
				if (prec == null) {
					prec = getPrec(value, 1);
					precMap.put(value, prec);
				}
				
				System.out.println("Value: " + value + ", " + prec);
				
				if (prec < 0.90)
					continue;
					
				//if (!docAnsList.contains(start + "|" + end)) {
					System.out.println("adding: " + value + "|" + start + "|" + end);
					pw.println("adding: " + value + "|" + start + "|" + end);
				
					int[] ansIndexes = MSAUtils.matchAnswer(ansMap, docID, (int) start, (int) end);
					if (ansIndexes[0] >= 0) {
						start = ansIndexes[0];
						end = ansIndexes[1];
					}
					
					Annotation annot2 = new Annotation(docID, docNamespace, docTable, -1, profileAnnotType, 
						start, end, value, null);
					
					if (ansIndexes[0] >= 0) {
						annot2.setScore(1.0);
						matchMap.put(docID + "|" + start + "|" + end, true);
					}
					else {
						matchMap.put(docID + "|" + start + "|" + end, false);
						annot2.setScore(0.0);
					}
					
					seq.addAnnotation(annot2, features, false, "");
					
					ProfileMatch match = new ProfileMatch();
					match.setSequence(seq);
					
					finalAnnotList.add(annot2);
					finalMatchList.add(match);
				}					
			
		}
		*/
		
		
		/*
		Map<Long, List<AnnotationEntity>> combineMap = combineEntities(matchList);			
		
		
		//fill in values that were matched per document
		System.out.println("filling in values within same document...");
		PreparedStatement pstmtFill = conn.prepareStatement("select distinct a.start, a.end, b.value from ner.annotation a, ner.annotation b "
			+ "where a.document_id = ? and b.value = ? and a.annotation_type = 'Token' and "
			+ "a.start >= b.start and a.end <= b.end and a.document_id = b.document_id");
		
		Map<Long, List<String>> combineStrMap = new HashMap<Long, List<String>>();
		for (long docID : combineMap.keySet()) {
			List<String> docStrList = new ArrayList<String>();
			combineStrMap.put(docID, docStrList);
			List<AnnotationEntity> docEntityList = combineMap.get(docID);
			for (AnnotationEntity entity : docEntityList) {
				String value = entity.getValue();
				docStrList.add(entity.getValue());
				Boolean flag = allValueMap.get(value);
				if (flag == null)
					allValueMap.put(value, true);
			}
		}
		
		
		precMap = new HashMap<String, double[]>();
		
		for (long docID : combineStrMap.keySet()) {
			pstmtFill.setLong(1, docID);
			
			List<String> docStrList = combineStrMap.get(docID);
			if (docStrList == null)
				continue;
			
			//List<AnnotationEntity> docEntityList = combineMap.get(docID);
			
			double[] ret = {0.0, 0.0};
			for (String value : docStrList) {
				System.out.println("adding value: " + value);
				pw.println("adding value: " + value);
				
				ret = precMap.get(value);
				if (ret == null) {
					ret = getPrec(value, "", "");
					ret[0]++;
					precMap.put(value, ret);
				}
				
				double prec = ret[0] / (ret[0] + ret[1] + 0.00001);
				
				if (prec < 0.9)
					continue;
				
				pstmtFill.setString(2, value);
				
				ResultSet rs = pstmtFill.executeQuery();
				
				while (rs.next()) {				
					long start = rs.getLong(1);
					long end = rs.getLong(2);
					String value2 = rs.getString(3);
					
					if (Character.isUpperCase(value.charAt(0)))
						value = value.toUpperCase();
					
					if (Character.isUpperCase(value2.charAt(0)))
						value2 = value2.toUpperCase();
					
					if (!value.equals(value2))
						continue;
					
					
					Boolean flag = matchMap.get(docID + "|" + start + "|" + end);
					if (flag != null)
						continue;
						
					System.out.println("adding: " + value + "|" + docID + "|" + start + "|" + end + "|" + ret[0] + "|" + ret[1]);
					pw.println("adding: " + value + "|" + docID + "|" + start + "|" + end + "|" + ret[0] + "|" + ret[1]);
				
					int[] ansIndexes = MSAUtils.matchAnswer(ansMap, docID, (int) start, (int) end);
					if (ansIndexes[0] >= 0) {
						start = ansIndexes[0];
						end = ansIndexes[1];
					}
					
					Annotation annot2 = new Annotation(docID, docNamespace, docTable, -1, profileAnnotType, 
						start, end, value, null);
					
					if (ansIndexes[0] >= 0) {
						annot2.setScore(1.0);
						matchMap.put(docID + "|" + start + "|" + end, true);
					}
					else {
						matchMap.put(docID + "|" + start + "|" + end, false);
						annot2.setScore(0.0);
					}
					
					AnnotationSequence seq = new AnnotationSequence();
					seq.setDocID(docID);
					seq.addAnnotation(annot2, features, false, "");
					
					ProfileMatch match = new ProfileMatch();
					match.setSequence(seq);
					
					finalAnnotList.add(annot2);
					finalMatchList.add(match);
				}
			}
			
		}
		
		pstmtFill.close();
		
		
		
		//all values
		System.out.println("adding all values...");
		PreparedStatement pstmtValue = conn.prepareStatement("select distinct a.document_id, a.start, a.end, b.value from ner.annotation a, ner.annotation b "
			+ "where a.annotation_type = 'Token' and a.document_id = b.document_id and a.start >= b.start and "
			+ "a.end <= b.end and b.value = ? and a.document_id in "
			+ "(select distinct c.document_id from conll2003_document c where c.`group` = 'testb')");
		
		precMap = new HashMap<String, double[]>();
		
		double[] ret = {0.0, 0.0};
		for (String allValue : allValueMap.keySet()) {
			ret = precMap.get(allValue);
			if (ret == null) {
				ret = getPrec(allValue, "", "");
				//ret[0]++;
				precMap.put(allValue, ret);
			}
			
			double prec = ret[0] / (ret[0] + ret[1] + 0.0001);
			
			if (prec < 0.9)
				continue;
				
			pstmtValue.setString(1, allValue);
			ResultSet rs = pstmtValue.executeQuery();
				
			while (rs.next()) {
				long docID = rs.getLong(1);
				long start = rs.getLong(2);
				long end = rs.getLong(3);
				String value2 = rs.getString(4);
				
				
				if (Character.isUpperCase(allValue.charAt(0)))
					allValue = allValue.toUpperCase();
				
				if (Character.isUpperCase(value2.charAt(0)))
					value2 = value2.toUpperCase();
				
				if (!allValue.equals(value2))
					continue;
				
				Boolean flag = matchMap.get(docID + "|" + start + "|" + end);
				if (flag == null) {
					int[] ansIndexes = MSAUtils.matchAnswer(ansMap, docID, (int) start, (int) end);
					
					if (ansIndexes[0] >= 0) {
						start = ansIndexes[0];
						end = ansIndexes[1];
					}
					
					Annotation annot2 = new Annotation(docID, docNamespace, docTable, -1, profileAnnotType, 
						start, end, allValue, null);
					System.out.println("adding all value: " + docID + "|" + start + "|" + end + "|" + allValue + "|" + ret[0] + "|" + ret[1]);
					pw.println("adding all value: " + docID + "|" + start + "|" + end + "|" + allValue + "|" + ret[0] + "|" + ret[1]);

					
					if (ansIndexes[0] >= 0) {
						annot2.setScore(1.0);
						matchMap.put(docID + "|" + start + "|" + end, true);
					}
					else {
						annot2.setScore(0.0);
						matchMap.put(docID + "|" + start + "|" + end, false);
					}
					
					AnnotationSequence seq = new AnnotationSequence();
					seq.setDocID(docID);
					seq.addAnnotation(annot2, features, false, "");
					
					ProfileMatch match = new ProfileMatch();
					match.setSequence(seq);
					
					finalAnnotList.add(annot2);
					finalMatchList.add(match);
				}
			}
		}
		
		pstmtValue.close();
		
		
		
		System.out.println("adding known values...");
		PreparedStatement pstmtKnown = conn.prepareStatement("select distinct a.start, a.end, a.value, b.value, c.value from ner.annotation a, ner.annotation b, msa.known_values c "
			+ "where b.value = c.value and a.annotation_type = 'Token' and c.annotation_type = '" + profileAnnotType + "' and a.document_id = b.document_id and "
			+ "a.start >= b.start and a.end <= b.end and a.document_id = ?");
		
		for (long docID : docIDList) {
			pstmtKnown.setLong(1, docID);
			ResultSet rs = pstmtKnown.executeQuery();
			while (rs.next()) {
				long start = rs.getLong(1);
				long end = rs.getLong(2);
				String value = rs.getString(4);
				String value2 = rs.getString(5);
				
				if (!value.equals(value2))
					continue;
				
				Boolean flag = matchMap.get(docID + "|" + start + "|" + end);
				if (flag == null) {

					int[] ansIndexes = MSAUtils.matchAnswer(ansMap, docID, (int) start, (int) end);
					
					if (ansIndexes[0] >= 0) {
						start = ansIndexes[0];
						end = ansIndexes[1];
					}
					
					Annotation annot2 = new Annotation(docID, docNamespace, docTable, -1, profileAnnotType, 
						start, end, value, null);
					System.out.println("adding known: " + docID + "|" + start + "|" + end + "|" + value);
					pw.println("adding known: " + docID + "|" + start + "|" + end + "|" + value);

					
					if (ansIndexes[0] >= 0) {
						matchMap.put(docID + "|" + start + "|" + end, true);
						annot2.setScore(1.0);
					}
					else {
						matchMap.put(docID + "|" + start + "|" + end, false);
						annot2.setScore(0.0);
					}
					
					AnnotationSequence seq = new AnnotationSequence();
					seq.setDocID(docID);
					seq.addAnnotation(annot2, features, false, "");
					
					ProfileMatch match = new ProfileMatch();
					match.setSequence(seq);
					
					finalAnnotList.add(annot2);
					finalMatchList.add(match);
				}
			}
		}
		*/
	}
	
	public void writeAnnotations()
	{
		try {
			pstmtDeleteAnnots.execute();
			
			for (Annotation annot : finalAnnotList) {
					
				int id = getNextAnnotID(annot.getDocID());
				pstmt2.setInt(1, id);
				pstmt2.setLong(2, annot.getDocID());
				pstmt2.setString(3, annot.getAnnotationType());
				pstmt2.setLong(4, annot.getStart());
				pstmt2.setLong(5, annot.getEnd());
				pstmt2.setString(6, annot.getValue());
				pstmt2.setString(7, annot.getFeatures());
				pstmt2.setString(8, annot.getProvenance());
				pstmt2.setDouble(9, 1);
				pstmt2.execute();
				
				System.out.println(annot.toString());
					
			}
			
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	public void evalProfiles()
	{
		int tp = 0;
		int fp = 0;
		int fn = 0;
		
		for (int i=0; i<finalAnnotList.size(); i++) {
			Annotation annot = finalAnnotList.get(i);
			ProfileMatch match = finalMatchList.get(i);
			
			double score = annot.getScore();			
			
			if (score == 1.0) {
				tp++;
				
				System.out.println("correct: " + match.getSequence().getDocID() + "|" + annot.getStart() + "|" + annot.getEnd() + ": " + annot.getValue() + ", " + annot.getAnnotationType());
				pw.println("correct: " + match.getSequence().getDocID() + "|" + annot.getStart() + "|" + annot.getEnd() + ": " + annot.getValue() + ", " + annot.getAnnotationType());
			}
			else {
				fp++;
				System.out.println("wrong: " + match.getSequence().getDocID() + "|" + annot.getStart() + "|" + annot.getEnd() + ": " + annot.getValue() + ", " + annot.getAnnotationType());
				pw.println("wrong: " + match.getSequence().getDocID() + "|" + annot.getStart() + "|" + annot.getEnd() + ": " + annot.getValue() + ", " + annot.getAnnotationType());
			}
		}
		
		
		
		fn = ansMap.size() - tp;
		for (String key : ansMap.keySet()) {
			if (matchMap.get(key) == null) {
				String ansStr = ansMap.get(key);
				Map<String, String> map = new HashMap<String, String>();
				map = gson.fromJson(ansStr, map.getClass());
				String value = map.get("value");
				AnnotationSequence seq = ansSeqMap.get(key);
				String seqStr = "";
				if (seq != null)
					seqStr = gson.toJson(seq.getToks());
				System.out.println("not found: " + key + ", " + value + " | " + seqStr);
				pw.println("not found: " + key + ", " + value + " | " + seqStr);
			}
		}
		
		
		int total = finalAnnotList.size();
		double prec = ((double) tp) / (((double) tp) + ((double) fp));
		double recall = ((double) tp) / (((double) tp) + ((double) fn));
		
		System.out.println("tp: " + tp + ", fp: " + fp + ", fn: " + fn + ", ansMap: " + ansMap.size());
		pw.println("tp: " + tp + ", fp: " + fp + ", fn: " + fn);
		System.out.println("prec: " + prec + ", recall: " + recall + ", total: " + total);
		pw.println("prec: " + prec + ", recall: " + recall + ", total: " + total);
	}
	
	
	
	private Map<Long, List<AnnotationEntity>> combineEntities(List<ProfileMatch> matchList)
	{
		Map<Long, List<AnnotationEntity>> docEntityMap = new HashMap<Long, List<AnnotationEntity>>();
		for (ProfileMatch match : matchList) {
			//System.out.println("match: " + match.getTargetStr());
			
			
			long docID = match.getSequence().getDocID();
			List<AnnotationEntity> docEntityList = docEntityMap.get(docID);
			if (docEntityList == null) {
				docEntityList = new ArrayList<AnnotationEntity>();
				docEntityMap.put(docID, docEntityList);
			}
			
			//check if this match can be combined
			long newStart = -1;
			long newEnd = -1;
			String targetStr = "";
			int[] indexes = match.getTargetIndexes();
			AnnotationEntity matchEntity = new AnnotationEntity(match.getTargetStr(), match.getSequence().getDocID(), indexes[0], indexes[1]);
			AnnotationEntity currEntity = new AnnotationEntity(match.getTargetStr(), match.getSequence().getDocID(), indexes[0], indexes[1]);
			
			for (int i=0; i<docEntityList.size(); i++) {
				newStart = -1;
				long start = currEntity.getStart();
				long end = currEntity.getEnd();

				AnnotationEntity docEntity = docEntityList.get(i);
				long start2 = docEntity.getStart();
				long end2 = docEntity.getEnd();
				String delimit = "";
				
				//System.out.println("currMatch: " + currEntity.getValue() + ", " + currEntity.getStart() + ", " + currEntity.getEnd());
				//System.out.println("docMatch: " + docEntity.getValue() + ", " + docEntity.getStart() + ", " + docEntity.getEnd());
				boolean left = false;
				boolean right = false;
				
				if (!right && (start2 == end || start2 == end+1)) {
					newStart = start;
					newEnd = end2;
					
					if (start2 == end+1)
						delimit = " ";
					
					targetStr = currEntity.getValue() + delimit + docEntity.getValue();
					right = true;
					
					//System.out.println("targetStr: " + targetStr + ", newStart: " + newStart);
				}
				else if (!left && (end2 == start || end2+1 == start)) {
					newStart = start2;
					newEnd = end;
					
					if (end2+1 == start)
						delimit = " ";
					
					targetStr =  docEntity.getValue() + delimit + currEntity.getValue();
					left = true;
				}
				
				if (newStart >= 0) {
					currEntity.setStart(newStart);
					currEntity.setEnd(newEnd);
					currEntity.setValue(targetStr);
					
					for (long[] newIndexes : docEntity.getIndexList())
						currEntity.addIndexes(newIndexes);
					
					//System.out.println("combined: " + targetStr);
					
					
					//remove the match from docMatchList if it is multi-token
					if (docEntity.getValue().indexOf(" ") >= 0) {
						docEntityList.remove(i);
						i--;
					}
				}
			}
			
			docEntityList.add(matchEntity);
			if (currEntity.getStart() != matchEntity.getStart() || currEntity.getEnd() != matchEntity.getEnd())
				docEntityList.add(0, currEntity);			
		}
		
		
		/*
		for (long docID : docEntityMap.keySet()) {
			List<AnnotationEntity> docEntityList = docEntityMap.get(docID);
			System.out.println("docID: " + docID);
			
			for (AnnotationEntity entity : docEntityList) {
				System.out.println(entity.getValue());
				
				for (long[] indexes : entity.getIndexList()) {
					System.out.println(indexes[0] + ", " + indexes[1]);
				}
			}
			
			System.out.println("\n\n");
		}
		*/
		
		
		
		return docEntityMap;
	}
	
	
	
	private void getDocIDList(String docDBUser, String docDBPassword, String docDBHost, String docDBName, String docDBType, String docDBQuery) throws Exception
	{
		//Connection docDBConn = DBConnection.dbConnection(docDBUser, docDBPassword, docDBHost, docDBName, docDBType);
		java.sql.Statement stmt = docDBConn.createStatement();
		java.sql.ResultSet rs = stmt.executeQuery(docDBQuery);
		
		docIDList = new ArrayList<Long>();
		
		while (rs.next()) {
			docIDList.add(rs.getLong(1));
		}
		
		stmt.close();
		//docDBConn.close();
		
	}
	
	private int getNextAnnotID(long docID) throws SQLException
	{
		int id = -1;
		
		pstmt.setLong(1, docID);
		ResultSet rs = pstmt.executeQuery();
		if (rs.next()) {
			id = rs.getInt(1) + 1;
		}

		return id;
	}
	
	private void readAnswers(String annotType, long docID, List<AnnotationSequence> seqList) throws SQLException
	{
		Statement stmt = conn.createStatement();
		PreparedStatement pstmt = conn.prepareStatement("select start, end from annotation where document_id = ? and annotation_type = 'Token' and start >= ? and end <= ? order by start");
		
		String annotType2 = "B" + annotType.substring(1);
		String queryStr = "select start, end, value from annotation "
			+ "where document_id = " + docID + " and provenance = '" + targetProvenance + "' and ";
		
		String annotClause = "(annotation_type = '" + annotType + "' or annotation_type = '" + annotType2 + "')";
		
		ResultSet rs = stmt.executeQuery(queryStr + annotClause + " order by start");
		while (rs.next()) {
			//int docID = rs.getInt(1);
			int start = rs.getInt(1);
			int end = rs.getInt(2);
			String value = rs.getString(3);
			
			Map<String, String> map = new HashMap<String, String>();
			map.put("annotType", annotType);
			map.put("value", value);
			
			pstmt.setLong(1, docID);
			pstmt.setInt(2, start);
			pstmt.setInt(3, end);
			ResultSet rs2 = pstmt.executeQuery();
			while (rs2.next()) {
				int start2 = rs2.getInt(1);
				int end2 = rs2.getInt(2);
			
				ansMap.put(docID + "|" + start2 + "|" + end2, gson.toJson(map));
				//System.out.println(docID + "|" + start2 + "|" + end2 + ", " + value);
			
				for (AnnotationSequence seq : seqList) {
					long docID2 = seq.getDocID();
					long start3 = seq.getStart();
					long end3 = seq.getEnd();
					
					if (docID2 == docID && start3 <= start && end3 >= end) {
						ansSeqMap.put(docID + "|" + start2 + "|" + end2, seq);
						break;
					}
				}
			}
		}
		
		
		/*
		List<int[]> rangeList = new ArrayList<int[]>();
		ansRangeMap.put(docID, rangeList);
		
		rs = stmt.executeQuery("select start, end from annotation where document_id = " + docID + " and provenance = 'conll2003-entity' and annotation_type = 'I-PER'");
		while (rs.next()) {
			int[] range = new int[2];
			range[0] = rs.getInt(1);
			range[1] = rs.getInt(2);
			rangeList.add(range);
		}
		*/
		
		
		stmt.close();
		pstmt.close();
	}
	
	private double[] getPrec(String value, String annotType2, String annotType3) throws SQLException
	{
		/*
		int tp = initTP;
		int fp = 0;
		
		if (initTP < 0) {
			tp = 0;
		}
		*/
		
		int tp = 0;
		int fp = 0;
		
		if (Character.isUpperCase(value.charAt(0)))
			value = value.toUpperCase();
		
		pstmt3.setString(1, value);
		ResultSet rs = pstmt3.executeQuery();
		while (rs.next()) {
			String annotType = rs.getString(1);
			String value2 = rs.getString(2);
			
			if (Character.isUpperCase(value2.charAt(0)))
				value2 = value2.toUpperCase();
			
			if (value.indexOf(value2) >= 0) {
				if (annotType.equals(targetType) || annotType.equals(annotType2) || annotType.equals(annotType3))
					tp++;
				else
					fp++;
			}
		}
		
		/*
		if ((tp + fp) < minTotal)
			return -1.0;
		
		double prec = ((double) tp) / ((double) (tp + fp) + 0.00001);
		
		if (initTP < 0 && tp == 0 && fp == 0)
			prec = 0.01;
			*/
		double[] ret = new double[2];
		ret[0] = tp;
		ret[1] = fp;
		
		return ret;
	}
	
	public static void main(String[] args)
	{
		if (args.length != 5) {
			System.out.println("usage: user password docUser docPassword config");
			System.exit(0);
		}
		
		try {
			AutoAnnotate auto = new AutoAnnotate();
			auto.init(args[4]);
			auto.annotate(args[0], args[1], args[2], args[3]);
			auto.evalProfiles();
			
			//if (auto.write)
				// auto.writeAnnotations();
			
			auto.close();
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
}
