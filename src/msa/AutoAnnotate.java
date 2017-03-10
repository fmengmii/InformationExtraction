package msa;

import java.util.*;

import org.apache.commons.lang.StringUtils;

import com.google.gson.Gson;

import align.AnnotationSequenceGrid;
import align.GenAnnotationGrid;
import align.SmithWatermanDim;
import msa.ProfileReader.Order;
import msa.db.MSADBInterface;
import msa.db.MySQLDBInterface;
import utils.db.DBConnection;

import java.io.*;
import java.sql.*;

public class AutoAnnotate
{
	private Gson gson;
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
	private String docDBQuery;
	private String docDBType;
	
	
	//private String profileAnnotType;
	//private String negProfileAnnotType;

	//private String group;
	private boolean writeAnnots;
	//private String provenance;
	
	private boolean verbose;
	private List<Map<String, Object>> msaAnnotFilterList;
	private List<Map<String, Object>> contextAnnotFilterList;
	private List<Double> scoreList;
	private int limit;
	private Boolean requireTarget;
		
	private MSADBInterface db;
	private Connection docDBConn;
	private Connection conn;
	private Connection conn2;
	private PreparedStatement pstmt;
	private PreparedStatement pstmt2;
	private PreparedStatement pstmt3;
	private PreparedStatement pstmtWrite;
	private PreparedStatement pstmtCheck;
	private PreparedStatement pstmtDeleteFrameData;
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
	private String negTargetType;
	private String negTargetProvenance;
	private Map<String, Boolean> requireTargetMap;
	
	private boolean contextFlag = false;
	private PrintWriter pw;
	
	private GenAnnotationGrid genGrid;
	private ProfileMatcher profileMatcher;
	private String tokType;
	
	private SmithWatermanDim sw;
	private List<ProfileMatch> noMatchList;
	
	//private Map<Long, MSAProfile> profileIDMap;
	private int profileMinTotal;
	private double profileMinPrec;
	private double minMatchPrec;
	private double minMatchTotal;
	private Boolean filterAllCaps;
	
	private String finalTable;
	private String profileTable;
	
	private String autoMatchTable;
	private String runName;
	
	private List<String> annotTypeList;
	private List<String> profileTableList;
	private List<String> indexTableList;
	private List<String> finalTableList;
	
	private boolean evalFlag;
	
	private int totalTP;
	
	
	public AutoAnnotate()
	{
		gson = new Gson();
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
			//write = Boolean.parseBoolean(props.getProperty("write"));
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
			negTargetType = props.getProperty("negTargetType");
			negTargetProvenance = props.getProperty("negTargetProvenance");
			
			finalTable = props.getProperty("finalTable");
			profileTable = props.getProperty("profileTable");

			
			if (targetType != null) {
				annotTypeList = new ArrayList<String>();
				annotTypeList.add(targetType);
				
				profileTableList = new ArrayList<String>();
				profileTableList.add(profileTable);
				
				finalTableList = new ArrayList<String>();
				finalTableList.add(finalTable);
			}
			
			if (requireTarget != null) {
				requireTargetMap = new HashMap<String, Boolean>();
				requireTargetMap.put(targetType, requireTarget);
			}
			
			profileMinTotal = Integer.parseInt(props.getProperty("profileMinTotal"));
			profileMinPrec = Double.parseDouble(props.getProperty("profileMinPrec"));
			
			//minMatchTotal = Integer.parseInt(props.getProperty("minMatchTotal"));
			//minMatchPrec = Double.parseDouble(props.getProperty("minMatchPrec"));
			
			filterAllCaps = Boolean.parseBoolean(props.getProperty("filterAllCaps"));
			
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
			/*
			Map<String, Object> targetMap = new HashMap<String, Object>();
			targetMap.put("annotType", targetType);
			targetMap.put("provenance", "conll2003-final");
			targetMap.put("literal", ":" + targetType.toLowerCase());
			List<String> featureList = new ArrayList<String>();
			featureList.add("$literal");
			targetMap.put("features", featureList);
			msaAnnotFilterList.add(targetMap);
			*/
			
			
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
		
		//extractMap = new HashMap<String, Boolean>();
		
		totalTP = 0;
		
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
			
			pstmtDeleteFrameData = conn.prepareStatement("delete from frame_instance_data where document_id = ? and annotation_id in (select a.id from annotation a where a.document_id = ? and a.annotation_type = ? and provenance = '" + autoProvenance + "')");
			pstmtDeleteAnnots = conn.prepareStatement("delete from annotation where annotation_type = ? and document_id = ? and provenance = '" + autoProvenance + "'");
			
			
			

			
			
			//check for docID list info		
			/*
			if (docDBQuery != null) {				
				docDBConn = DBConnection.dbConnection(docUser, docPassword, docDBHost, docNamespace, docDBType);	
				docIDList = MSAUtils.getDocIDList(docDBConn, docDBQuery);
				docDBConn.close();
			}
			*/
			
			Statement stmt = conn.createStatement();
			/*
			ResultSet rs = stmt.executeQuery("select max(document_id) from " + docNamespace + "." + docTable);
			long maxDocID = -1;
			if (rs.next()) {
				maxDocID = rs.getLong(1);
			}
			*/
			
			
			for (int index=0; index<annotTypeList.size(); index++) {
				finalAnnotList = new ArrayList<Annotation>();
				finalMatchList = new ArrayList<ProfileMatch>();

				targetType = annotTypeList.get(index);
				profileTable = profileTableList.get(index);
				finalTable = finalTableList.get(index);
				
				if (targetType.length() == 0 || profileTable.length() == 0 || finalTable.length() == 0)
					continue;

				
				docIDList = new ArrayList<Long>();
				//ResultSet rs = stmt.executeQuery("select distinct document_id from document_status where status = 0 order by document_id");
				ResultSet rs = stmt.executeQuery(docDBQuery);
				while (rs.next()) {
					docIDList.add(rs.getLong(1));
				}
			
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
				//genSent.setMaskTarget(true);
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
					readAnswers(targetType, negTargetType, docID, negSeqList);
	
	
				
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
				reader.setMinScore(0.0);
				reader.init(msaUser, msaPassword, host, dbType, msaKeyspace);
	
				List<ProfileGrid> profileGridList = new ArrayList<ProfileGrid>();
				Map<AnnotationSequenceGrid, MSAProfile> msaProfileMap = new HashMap<AnnotationSequenceGrid, MSAProfile>();
				Map<AnnotationSequenceGrid, MSAProfile> msaTargetProfileMap = new HashMap<AnnotationSequenceGrid, MSAProfile>();
				Map<Long, ProfileGrid> profileIDMap = new HashMap<Long, ProfileGrid>();
				Map<Long, AnnotationSequenceGrid> targetIDMap = new HashMap<Long, AnnotationSequenceGrid>();
				
				Map<MSAProfile, List<MSAProfile>> profileMap = reader.readFinal(targetType, profileMinTotal, profileMinPrec, finalTable, profileTable);
				
				for (MSAProfile profile : profileMap.keySet()) {
					AnnotationSequenceGrid profileSeqGrid = genGrid.toAnnotSeqGrid(profile.getToks(), false);
					msaProfileMap.put(profileSeqGrid, profile);
					
					Map<AnnotationSequenceGrid, Boolean> targetSeqGridMap = new HashMap<AnnotationSequenceGrid, Boolean>();
					List<MSAProfile> targetList = profileMap.get(profile);
					for (MSAProfile target : targetList) {
						AnnotationSequenceGrid targetGrid = genGrid.toAnnotSeqGrid(target.getToks(), false);
						
						MSAProfile target2 = msaTargetProfileMap.get(targetGrid);
						if (target2 == null) {
							msaTargetProfileMap.put(targetGrid, target);
							targetIDMap.put(target.getProfileID(), targetGrid);
						}
						
						targetSeqGridMap.put(targetGrid, true);
					}
					
					ProfileGrid  profileGrid = new ProfileGrid(profileSeqGrid, targetSeqGridMap);
					profileIDMap.put(profile.getProfileID(), profileGrid);
					
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
				
				
				Map<AnnotationSequenceGrid, Boolean> targetGridMap = new HashMap<AnnotationSequenceGrid, Boolean>();
				List<AnnotationSequenceGrid> targetGridList = new ArrayList<AnnotationSequenceGrid>();

				System.out.println("\n\nProfiles: " + profileGridList.size());
				pw.println("\n\nProfiles: " + profileGridList.size());
				for (ProfileGrid profileGrid : profileGridList) {
					String profileStr = gson.toJson(profileGrid.getGrid().getSequence().getToks());
	
					System.out.println(profileStr);
					pw.println(profileStr);
					
					System.out.println("Targets: ");
					pw.println("Targets:");
					Map<AnnotationSequenceGrid, Boolean> targetSeqGridMap = profileGrid.getTargetGridMap();
					for (AnnotationSequenceGrid targetSeqGrid : targetSeqGridMap.keySet()) {
						profileStr = gson.toJson(targetSeqGrid.getSequence().getToks());
						System.out.println(profileStr);
						pw.println(profileStr);
						
						Boolean flag = targetGridMap.get(targetSeqGrid);
						if (flag == null) {
							targetGridMap.put(targetSeqGrid, true);
							targetGridList.add(targetSeqGrid);
						}
					}
					
					System.out.println("\n\n");
					pw.println("\n\n");
				}
				
				
	
				//match grids
				matchMap = new HashMap<String, Boolean>();
				
				ProfileInvertedIndex invertedIndex = new ProfileInvertedIndex();
				invertedIndex.setTargetFlag(true);
				invertedIndex.genIndex(profileGridList, targetGridList, profileIDMap, targetIDMap);
	
				List<ProfileMatch> matchList = profileMatcher.matchProfile(negGridList, profileGridList, null, targetType, true, maxGaps, syntax, phrase, false, msaProfileMap, msaTargetProfileMap, invertedIndex);
				
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
					
					if (StringUtils.isAllUpperCase(value))
						continue;
					
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
				
				if (writeAnnots)
					writeAnnotations(targetType);
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
		
		

			
			
		for (Annotation annot : finalAnnotList) {
			
			long docID = annot.getDocID();
			long start = annot.getStart();
			long end = annot.getEnd();
			
			
			
			String ansJSON = ansMap.get(docID + "|" + start + "|" + end);
			
			
			
			if (ansJSON == null) {
				ansJSON = ansMap.get(docID + "|" + start+1 + "|" + end+1);
				if (ansJSON != null) {
					start++;
					end++;
				}
			}
				
			if (ansJSON == null) {
				ansJSON = ansMap.get(docID + "|" + (start-1) + "|" + (end-1));
				
				if (ansJSON != null) {
					start--;
					end--;
				}
			}
				
			
			
			if (ansJSON == null) {
				ansJSON = ansMap.get(docID + "|" + (start-1) + "|" + (end));
				if (ansJSON != null) {
					start--;
				}
			}
			
			if (ansJSON == null) {
				ansJSON = ansMap.get(docID + "|" + (start+1) + "|" + (end));
				if (ansJSON != null) {
					start++;
				}
			}
			
			if (ansJSON == null) {
				ansJSON = ansMap.get(docID + "|" + start + "|" + (end+1));
				if (ansJSON != null) {
					end++;
				}
			}
			
			if (ansJSON == null) {
				ansJSON = ansMap.get(docID + "|" + start + "|" + (end-1));
				if (ansJSON != null) {
					end--;
				}
			}
			
			if (ansJSON == null) {
				ansJSON = ansMap.get(docID + "|" + (start-1) + "|" + (end+1));
				if (ansJSON != null) {
					start--;
					end++;
				}
			}
			
			Map<String, String> ansTypeMap = new HashMap<String, String>();
			String annotType = null;
			
			if (ansJSON != null) {
				ansTypeMap = (Map<String, String>) gson.fromJson(ansJSON, ansTypeMap.getClass());
				annotType = ansTypeMap.get("annotType");
			}
			
			if (annotType != null && annotType.equals(targetType)) {
				matchMap.put(docID + "|" + start + "|" + end, true);
				annot.setScore(1.0);
				System.out.print("Correct!: ");
				pw.print("Correct!: ");
			}
			else if ((negTargetType != null && annotType != null && annotType.equals(negTargetType)) || (negTargetType == null && annotType == null)) {
				annot.setScore(0.0);
				matchMap.put(docID + "|" + start + "|" + end, false);
				System.out.print("Wrong!: ");
				pw.print("Wrong!: ");
			}
			else if (negTargetType != null && annotType == null) {
				annot.setScore(0.5);
				//matchMap.put(docID + "|" + start + "|" + end, false);
				System.out.print("Neutral!: ");
				pw.print("Neutral!: ");
			}
			
			System.out.println(docID + "|" + start + "|" + end);
			pw.println(docID + "|" + start + "|" + end);

			
			//AnnotationSequence seq = grid.getSequence();
			//seq.addAnnotation(annot, features, false, "");
			
			
		}
		
		
		
		
		
		
		
		if (evalFlag) {
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
				else if (score == 0.0) {
					fp++;
					System.out.println("wrong: " + match.getSequence().getDocID() + "|" + annot.getStart() + "|" + annot.getEnd() + ": " + annot.getValue() + ", " + annot.getAnnotationType());
					pw.println("wrong: " + match.getSequence().getDocID() + "|" + annot.getStart() + "|" + annot.getEnd() + ": " + annot.getValue() + ", " + annot.getAnnotationType());
				}
			}
			
			
			
			fn = totalTP - tp;
			for (String key : ansMap.keySet()) {
				if (matchMap.get(key) == null) {
					String ansStr = ansMap.get(key);
					Map<String, String> map = new HashMap<String, String>();
					map = gson.fromJson(ansStr, map.getClass());
					String annotType = map.get("annotType");
					
					if (annotType.equals(targetType)) {
						String value = map.get("value");
						AnnotationSequence seq = ansSeqMap.get(key);
						String seqStr = "";
						if (seq != null)
							seqStr = gson.toJson(seq.getToks());
						System.out.println("not found: " + key + ", " + value + " | " + seqStr);
						pw.println("not found: " + key + ", " + value + " | " + seqStr);
					}
				}
			}
			
			
			int total = finalAnnotList.size();
			double prec = ((double) tp) / (((double) tp) + ((double) fp));
			double recall = ((double) tp) / (((double) tp) + ((double) fn));
			
			System.out.println("tp: " + tp + ", fp: " + fp + ", fn: " + fn + ", ansMap: " + ansMap.size());
			pw.println("tp: " + tp + ", fp: " + fp + ", fn: " + fn+ ", ansMap: " + ansMap.size());
			System.out.println("prec: " + prec + ", recall: " + recall + ", total: " + total);
			pw.println("prec: " + prec + ", recall: " + recall + ", total: " + total);
		}
		
		/*
		if (writeAnnots) {
			writeAnnotations();
		}
		*/
		
	}
	
	public void writeAnnotations(String annotType)
	{
		try {
			if (finalAnnotList.size() > 0) {
				pstmtDeleteAnnots.setString(1, annotType);
				for (long docID : docIDList) {
					pstmtDeleteFrameData.setLong(1, docID);
					pstmtDeleteFrameData.setLong(2, docID);
					pstmtDeleteFrameData.setString(3, annotType);
					pstmtDeleteFrameData.execute();
					
					pstmtDeleteAnnots.setLong(2, docID);
					pstmtDeleteAnnots.execute();
				}
			}
			
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
				
				//System.out.println(annot.toString());
					
			}
			
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
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
	
	private void readAnswers(String annotType, String negAnnotType, long docID, List<AnnotationSequence> seqList) throws SQLException
	{
		Statement stmt = conn.createStatement();
		PreparedStatement pstmt = conn.prepareStatement("select start, end from annotation where document_id = ? and annotation_type = 'Token' and start >= ? and end <= ? order by start");
		
		String annotType2 = "B" + annotType.substring(1);
		String queryStr = "select start, end, value, annotation_type from annotation "
			+ "where document_id = " + docID + " and ";
		
		String annotClause = "(provenance = '" + targetProvenance + "' and (annotation_type = '" + annotType + "' or annotation_type = '" + annotType2 + "')";
		if (negAnnotType != null)
			annotClause = annotClause + " or (provenance = '" + negTargetProvenance + "' and annotation_type = '" + negAnnotType + "')";
		
		annotClause = annotClause + ")";
		
		ResultSet rs = stmt.executeQuery(queryStr + annotClause + " order by start");
		while (rs.next()) {
			//int docID = rs.getInt(1);
			int start = rs.getInt(1);
			int end = rs.getInt(2);
			String value = rs.getString(3);
			String aType = rs.getString(4);
			
			Map<String, String> map = new HashMap<String, String>();
			map.put("annotType", aType);
			map.put("value", value);
			
			String mapStr = gson.toJson(map);
			
			
			pstmt.setLong(1, docID);
			pstmt.setInt(2, start);
			pstmt.setInt(3, end);
			ResultSet rs2 = pstmt.executeQuery();
			while (rs2.next()) {
				
				if (aType.equals(targetType) || aType.equals(annotType2))
					totalTP++;

				
				int start2 = rs2.getInt(1);
				int end2 = rs2.getInt(2);
			
				ansMap.put(docID + "|" + start2 + "|" + end2, mapStr);
				//System.out.println(docID + "|" + start2 + "|" + end2 + ", " + value);
			
				/*
				for (AnnotationSequence seq : seqList) {
					long docID2 = seq.getDocID();
					long start3 = seq.getStart();
					long end3 = seq.getEnd();
					
					if (docID2 == docID && start3 <= start && end3 >= end) {
						ansSeqMap.put(docID + "|" + start2 + "|" + end2, seq);
						break;
					}
				}
				*/
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
			//auto.eval();
			
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
