package msa;

import java.util.*;

import org.apache.commons.lang.StringUtils;

import com.google.gson.Gson;

import align.AnnotationGridElement;
import align.AnnotationSequenceGrid;
import align.GenAnnotationGrid;
import align.SmithWatermanDim;
import msa.ProfileReader.Order;
import msa.db.MSADBInterface;
import msa.db.MySQLDBInterface;
import nlputils.sequence.SequenceUtilities;
import utils.db.DBConnection;

import java.io.*;
import java.sql.*;

public class AutoAnnotateNER
{
	private Gson gson;
	private Boolean punct;
	private List<AnnotationSequence> posSeqList;
	private List<AnnotationSequence> negSeqList;
	private Map<String, AnnotationSequence> seqMap;
	
	private String host;
	private String dbName;
	private String dbType;
	private String docNamespace;
	private String docTable;
	//private String keyspace;
	//private String msaKeyspace;
	
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
	List<AnnotationSequenceGrid> targetGridList;
	Map<AnnotationSequenceGrid, MSAProfile> msaProfileMap;
	Map<AnnotationSequenceGrid, MSAProfile> msaTargetProfileMap;
	Map<Long, AnnotationSequenceGrid> targetIDMap;
		
	private MySQLDBInterface db;
	private Connection docDBConn;
	private Connection conn;
	private Connection conn2;
	private PreparedStatement pstmt;
	private PreparedStatement pstmt2;
	private PreparedStatement pstmt3;
	private PreparedStatement pstmtWrite;
	private PreparedStatement pstmtCheck;
	//private PreparedStatement pstmtDeleteFrameData;
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
	
	private Map<String, Double> valCountMap;
	
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
	
	private Map<String, Boolean> valMap;
	
	private boolean contextFlag = false;
	private PrintWriter pw;
	
	private GenAnnotationGrid genGrid;
	private ProfileMatcher profileMatcher;
	private String tokType;
	
	private SmithWatermanDim sw;
	private List<ProfileMatch> noMatchList;
	
	private Map<Long, ProfileGrid> profileIDMap;
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
	private double minGlobalPrec;
	private int minGlobalCount;
	private double minGlobalNegPrec;
	private int minGlobalNegCount;
	private boolean localMatch;
	private boolean global;
	private boolean globalEntity;
	private boolean sameDocToken;
	private boolean sameDocEntity;
	private boolean highProb;
	private boolean entity;
	private boolean prior;
	
	private String probTable;
	private String probEntityTable;
	
	private String existingProvenance;
	
	private boolean extraction = true;
	
	private int totalTP;
	
	private String schema = "";
	
	private String rq;
	
	public AutoAnnotateNER()
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
	
	public void setDocDBQuery(String docDBQuery)
	{
		this.docDBQuery = docDBQuery;
	}
	
	public void setProfileMinTotal(int profileMinTotal)
	{
		this.profileMinTotal = profileMinTotal;
	}
	
	public void setProfileMinPrec(double profileMinPrec)
	{
		this.profileMinPrec = profileMinPrec;
	}
	
	public void setAutoProvenance(String autoProvenance)
	{
		this.autoProvenance = autoProvenance;
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
			dbName = props.getProperty("dbName");
			//keyspace = props.getProperty("keyspace");
			//msaKeyspace = props.getProperty("msaKeyspace");
			dbType = props.getProperty("dbType");
			
			docDBHost = props.getProperty("docDBHost");
			docDBType = props.getProperty("docDBType");
			docNamespace = props.getProperty("docNamespace");
			docTable = props.getProperty("docTable");
			targetType = props.getProperty("targetAnnotType");
			schema = props.getProperty("schema");
			
			
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
			
			minGlobalPrec = Double.parseDouble(props.getProperty("minGlobalPrec"));
			minGlobalCount = Integer.parseInt(props.getProperty("minGlobalCount"));
			minGlobalNegPrec = Double.parseDouble(props.getProperty("minGlobalNegPrec"));
			minGlobalNegCount = Integer.parseInt(props.getProperty("minGlobalNegCount"));
			
			
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
			
			localMatch = false;
			String localMatchStr = props.getProperty("localMatch");
			if (localMatchStr != null)
				localMatch = Boolean.parseBoolean(localMatchStr);
			
			global = Boolean.parseBoolean(props.getProperty("global"));
			globalEntity = Boolean.parseBoolean(props.getProperty("globalEntity"));
			sameDocToken = Boolean.parseBoolean(props.getProperty("sameDocToken"));
			sameDocEntity = Boolean.parseBoolean(props.getProperty("sameDocEntity"));
			highProb = Boolean.parseBoolean(props.getProperty("highProb"));
			entity = Boolean.parseBoolean(props.getProperty("entity"));
			prior = Boolean.parseBoolean(props.getProperty("prior"));
			
			probTable = props.getProperty("probTable");
			probEntityTable = props.getProperty("probEntityTable");
			
			existingProvenance = props.getProperty("existingProvenance");
			
			String extractionStr = props.getProperty("extraction");
			if (extractionStr != null)
				extraction = Boolean.parseBoolean(extractionStr);
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
	
	public void annotate(String user, String password, String docUser, String docPassword)
	{
		System.out.println("auto annotate...");
		
		//extractMap = new HashMap<String, Boolean>();
		
		valMap = new HashMap<String, Boolean>();
		
		totalTP = 0;
		
		try {
			//init DB connections
			db = new MySQLDBInterface();
			db.setDBType(dbType);
			db.setSchema(schema);
			db.init(user, password, host, dbName, dbName);
			
			schema = schema + ".";
			
			conn = DBConnection.dbConnection(user, password, host, dbName, dbType);
			conn2 = DBConnection.dbConnection(user, password, host, dbName, dbType);
			rq = DBConnection.reservedQuote;
			
			pstmt = conn.prepareStatement("select max(id) from " + schema + "annotation where document_namespace = '" + docNamespace + "' and document_table = '" + docTable + "' "
				+ "and document_id = ?");
			pstmt2 = conn.prepareStatement("insert into " + schema + "annotation (id, document_namespace, document_table, document_id, annotation_type, start, " + rq + "end" + rq + ", value, features, provenance, score) "
				+ "values (?, '" + docNamespace + "', '" + docTable + "',?,?,?,?,?,?,?,?)");
			
			pstmt3 = conn.prepareStatement("select distinct a.annotation_type, a.value, a.document_id, a.start, a." + rq + "end" + rq + " from " + schema + "annotation a, " + schema + "annotation b "
				+ "where a.provenance = 'conll2003-token' and b.value = ? and a.start >= b.start and a." + rq + "end" + rq + " <= b." + rq + "end" + rq + " and a.document_id = b.document_id "
				+ "and a.document_id in (select distinct c.document_id from ner.conll2003_document c where c.`group` = 'train')");
			
			pstmtWrite = conn2.prepareStatement("insert into " + autoMatchTable + " (profile_id, document_id, start, " + rq + "end" + rq + ", target_id, run_name) values (?,?,?,?,?,'" + runName + "')");
			
			pstmtCheck = conn2.prepareStatement("select count(*) from " + autoMatchTable + " where document_id = ? and start = ? and " + rq + "end" + rq + " = ? and run_name = '" + runName + "'");
			
			//pstmtDeleteFrameData = conn.prepareStatement("delete from " + schema + "frame_instance_data where document_id = ? and annotation_id in (select a.id from annotation a where a.document_id = ? and a.annotation_type = ? and provenance = '" + autoProvenance + "')");
			pstmtDeleteAnnots = conn.prepareStatement("delete from " + schema + "annotation where annotation_type = ? and document_id = ? and provenance = '" + autoProvenance + "'");
			
			rq = DBConnection.reservedQuote;
			
			

			
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
			
			
			//genValProbMap();
			
			
			
			for (int index=0; index<annotTypeList.size(); index++) {
				finalAnnotList = new ArrayList<Annotation>();
				finalMatchList = new ArrayList<ProfileMatch>();

				targetType = annotTypeList.get(index);
				profileTable = profileTableList.get(index);
				finalTable = finalTableList.get(index);
				
				if (targetType.length() == 0 || profileTable.length() == 0 || finalTable.length() == 0)
					continue;
				
				if (requireTarget) {
					msaAnnotFilterList.remove(msaAnnotFilterList.size()-1);
					Map<String, Object> targetMap = new HashMap<String, Object>();
					targetMap.put("annotType", targetType);
					targetMap.put("provenance", targetProvenance);
					targetMap.put("targetStr", ":target");
					msaAnnotFilterList.add(targetMap);
				}

				
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
				
				List<String> annotTypeNameList = MSAUtils.getAnnotationTypeNameList(msaAnnotFilterList, tokType, scoreList);
				annotTypeNameList.add(annotTypeNameList.size()-1, ":" + targetType.toLowerCase());
				scoreList.add(10.0);
				
				genGrid = new GenAnnotationGrid(annotTypeNameList, tokType);
				
				sw.setScoreMap(annotTypeNameList, scoreList);
				//sw.setVerbose(true);
				profileMatcher.setAligner(sw);
				profileMatcher.setPrintWriter(pw);
				profileMatcher.setProfileMatch(true);
				
				profileMatcher.setTargetProbMap(valCountMap);
				

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
				seqMap = genSent.getSeqMap();
				
				
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
					readAnswers(targetType, targetProvenance, docID);
	
	
				
				//get grids
				System.out.println("getting grids...");
				/*
				List<AnnotationSequenceGrid> posGridList = new ArrayList<AnnotationSequenceGrid>();
				for (AnnotationSequence seq : posSeqList) {
					List<AnnotationSequenceGrid> gridList = genGrid.toAnnotSeqGrid(seq, true, false);
					posGridList.addAll(gridList);
				}
				*/
				
				//append previous sentence??
				List<AnnotationSequenceGrid> negGridList = new ArrayList<AnnotationSequenceGrid>();
				AnnotationSequence prevSeq = null;
				for (AnnotationSequence seq : negSeqList) {
					AnnotationSequence seq2 = seq;
					if (prevSeq != null) {
						seq2 = prevSeq.clone();
						seq2.append(seq);
					}
					
					List<AnnotationSequenceGrid> gridList = genGrid.toAnnotSeqGrid(seq2, false, false, false, true, false);
					negGridList.addAll(gridList);
					prevSeq = seq;
				}
				
				
				//read profiles
				System.out.println("reading profiles...");
				ProfileReader reader = new ProfileReader();
				reader.setOrder(Order.ASC);
				reader.setMinScore(0.0);
				reader.init(user, password, host, dbType, dbName);
	
				List<ProfileGrid> profileGridList = new ArrayList<ProfileGrid>();
				msaProfileMap = new HashMap<AnnotationSequenceGrid, MSAProfile>();
				msaTargetProfileMap = new HashMap<AnnotationSequenceGrid, MSAProfile>();
				profileIDMap = new HashMap<Long, ProfileGrid>();
				targetIDMap = new HashMap<Long, AnnotationSequenceGrid>();
				
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
							//System.out.println(target.getProfileID() + " " + gson.toJson(targetGrid.getSequence().getToks()));
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
				targetGridList = new ArrayList<AnnotationSequenceGrid>();

				System.out.println("\n\nProfiles: " + profileGridList.size());
				pw.println("\n\nProfiles: " + profileGridList.size());
				for (ProfileGrid profileGrid : profileGridList) {
					String profileStr = gson.toJson(profileGrid.getGrid().getSequence().getToks());
					MSAProfile profile = msaProfileMap.get(profileGrid.getGrid());
	
					System.out.println(profile.getProfileID() + "|" + profileStr);
					pw.println(profile.getProfileID() + "|" + profileStr);
					
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
	
				List<ProfileMatch> matchList = profileMatcher.matchProfile(negGridList, profileGridList, null, targetType, extraction, maxGaps, syntax, phrase, false, msaProfileMap, msaTargetProfileMap, invertedIndex);
				
				noMatchList = profileMatcher.getNoMatchList();
				
				
				List<String> features = new ArrayList<String>();
				features.add("$annotTypeName");
				
				Map<String, List<Long>> valDocMap = new HashMap<String, List<Long>>();
				Map<String, List<Long>> valDocProfileMap = new HashMap<String, List<Long>>();
				
				
				for (ProfileMatch match: matchList) {
					
					//target can only be 1 token (only for CoNLL2003)
					String value = match.getTargetStr();
					
					/*
					if (value.indexOf(" ") >= 0 && value.length() > 3) {
						continue;
					}
					*/
					
					AnnotationSequenceGrid grid = negGridList.get(match.getGridIndex());					
					long docID = grid.getSequence().getDocID();
					System.out.println(docID + "|" + match.getTargetIndexes()[0] + "|" + match.getTargetIndexes()[1]);
					
					
					if (value.length() > 1 && !Character.isLetter(value.charAt(value.length()-1)))
						value = value.substring(0, value.length()-1);
					
					//if (StringUtils.isAllUpperCase(value))
					//	continue;
					
					if (value.length() == 0)
						continue;
					
					
					int[] targetCoords = match.getTargetIndexes();
					
					//extractMap.put(value, true);
					
					
					
					//long start = targetAnnot.getStart();
					//long end = targetAnnot.getEnd();
					
					long start = targetCoords[0];
					long end = targetCoords[1];
					
					//value = value.toLowerCase();
					
					List<Long> valDocList = valDocMap.get(value);
					List<Long> valDocProfileList = valDocProfileMap.get(value);
					if (valDocList == null) {
						valDocList = new ArrayList<Long>();
						valDocMap.put(value, valDocList);
						valDocProfileList = new ArrayList<Long>();
						valDocProfileMap.put(value, valDocProfileList);
						
					}
					
					if (!valDocList.contains(docID)) {
						valDocList.add(docID);
						valDocProfileList.add(match.getProfile().getProfileID());
					}
					
					Annotation annot = new Annotation(docID, docNamespace, docTable, -1, targetType, 
						start, end, match.getTargetStr(), null);
					annot.setProvenance(autoProvenance);
					Map<String, Object> featureMap = new HashMap<String, Object>();
					featureMap.put("profileID", match.getProfile().getProfileID());
					featureMap.put("targetID", match.getTargetList().get(0).getProfileID());
					List<int[]> matchCoords = match.getMatchCoords2();
					int seqStart = match.getSequence().getStart();
					featureMap.put("start", matchCoords.get(0)[0] + seqStart);
					featureMap.put("end", matchCoords.get(matchCoords.size()-1)[0] + seqStart);
					String featuresStr = gson.toJson(featureMap);
					annot.setFeatures(featuresStr);
					
					Boolean flag = valMap.get(docID + "|" + start + "|" + end);
					if (flag == null) {
						valMap.put(docID + "|" + start + "|" + end, true);
						finalAnnotList.add(annot);
						finalMatchList.add(match);
					}
					
					System.out.println(" ans: " + targetType + ", profile: " + match.getProfile().getProfileStr() + ", target: " + match.getTargetStr() + ", docID: " + match.getSequence().getDocID() + ", sent: " + match.getGridStr());
					pw.println(" ans: " + targetType + ", profile: " + match.getProfile().getProfileStr() + ", target: " + match.getTargetStr() + ", docID: " + match.getSequence().getDocID() + ", sent: " + match.getGridStr());

				}
				
				
				if (localMatch)
					localPatternMatcher(matchList);
				
				
				if (prior)
					addSameDocAnnotations(valDocMap, valDocProfileMap);
	
				if (evalFlag) {
					eval();
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

	
	public void eval()
	{
		//stats		

		
		List<String> features = new ArrayList<String>();
		features.add("$annotTypeName");

		Map<Long, Map<String, List<String>>> docAnsMap = new HashMap<Long, Map<String, List<String>>>();
		Map<String, Boolean> allValueMap = new HashMap<String, Boolean>();
		Map<String, double[]> precMap = new HashMap<String, double[]>();
		
		

			
			
		for (int i=0; i<finalAnnotList.size(); i++) {
			Annotation annot = finalAnnotList.get(i);
			long docID = annot.getDocID();
			long start = annot.getStart();
			long end = annot.getEnd();
			String value = annot.getValue();
			
			
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
				//matchMap.put(docID + "|" + start + "|" + end, true);
				annot.setScore(1.0);
				System.out.print("Correct!: ");
				pw.print("Correct!: ");
			}
			else if ((negTargetType != null && annotType != null && annotType.equals(negTargetType)) || (negTargetType == null && annotType == null)) {
				annot.setScore(0.0);
				//matchMap.put(docID + "|" + start + "|" + end, false);
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
				//ProfileMatch match = finalMatchList.get(i);
				
				double score = annot.getScore();			
				
				if (score == 1.0) {
					tp++;
					
					System.out.println("correct: " + annot.getDocID() + "|" + annot.getStart() + "|" + annot.getEnd() + ": " + annot.getValue() + ", " + annot.getAnnotationType());
					pw.println("correct: " + annot.getDocID() + "|" + annot.getStart() + "|" + annot.getEnd() + ": " + annot.getValue() + ", " + annot.getAnnotationType());
				}
				else if (score == 0.0) {
					fp++;
					System.out.println("wrong: " + annot.getDocID() + "|" + annot.getStart() + "|" + annot.getEnd() + ": " + annot.getValue() + ", " + annot.getAnnotationType());
					pw.println("wrong: " + annot.getDocID() + "|" + annot.getStart() + "|" + annot.getEnd() + ": " + annot.getValue() + ", " + annot.getAnnotationType());
				}
			}
			
			
			
			//fn = totalTP - tp;
			fn = ansMap.size() - tp;
			for (String key : ansMap.keySet()) {
				if (valMap.get(key) == null) {
					String ansStr = ansMap.get(key);
					Map<String, String> map = new HashMap<String, String>();
					map = gson.fromJson(ansStr, map.getClass());
					String annotType = map.get("annotType");
					
					if (annotType.equals(targetType)) {
						String value = map.get("value");
						//AnnotationSequence seq = ansSeqMap.get(key);
						//String seqStr = "";
						//if (seq != null)
						//	seqStr = gson.toJson(seq.getToks());
						
						String[] parts = key.split("\\|");
						long docID = Long.parseLong(parts[0]);
						long start = Long.parseLong(parts[1]);
						long end = Long.parseLong(parts[2]);
						
						AnnotationSequence seq = getSequence(docID, start, end);
						String seqStr = "";
						if (seq != null)
							seqStr = SequenceUtilities.getStrFromToks(seq.getToks());
						
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
					//pstmtDeleteFrameData.setLong(1, docID);
					//pstmtDeleteFrameData.setLong(2, docID);
					//pstmtDeleteFrameData.setString(3, annotType);
					//pstmtDeleteFrameData.execute();
					
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
	
	private AnnotationSequence getSequence(long docID, long start, long end)
	{
		AnnotationSequence seq = null;
		
		//System.out.println("docID: " + docID + " start: " + start + " end: " + end);
		
		for (AnnotationSequence seq2 : negSeqList) {
			//System.out.println("seq: docID: " + seq2.getDocID() + " start: " + seq2.getStart() + " end:" + seq2.getEnd());
			if (docID == seq2.getDocID() && start >= seq2.getStart() && end <= seq2.getEnd()) {
				seq = seq2;
				break;
			}
		}
		
		return seq;
	}
	
	
	private void addSameDocAnnotations(Map<String, List<Long>> valDocMap, Map<String, List<Long>> valDocProfileMap) throws SQLException
	{

		//genValProbMap();
		
		if (highProb) {
			PreparedStatement pstmt = conn.prepareStatement("select distinct document_id, start, end from annotation where document_id >= 1163 and value = ? and annotation_type = 'Token' and (features like '%upperinitial%' or features like '%allCaps%')");
			for (String value : valCountMap.keySet()) {
				double prob = valCountMap.get(value);
				if (prob > 0.0) {
					pstmt.setString(1, value);
					ResultSet rs = pstmt.executeQuery();
					while (rs.next()) {
						long docID = rs.getLong(1);
						long start = rs.getLong(2);
						long end = rs.getLong(3);
						
						Boolean flag = valMap.get(docID + "|" + start + "|" + end);
						if (flag == null) {
							System.out.println("adding: " + value + "|" + docID + "|" + start + "|" + prob);
							pw.println("adding: " + value + "|" + docID + "|" + start + "|" + prob);
							valMap.put(docID + "|" + start + "|" + end, true);
							matchMap.put(docID + "|" + start + "|" + end, true);
							Map<String, Object> featureMap = new HashMap<String, Object>();
							featureMap.put("profileID", -1);
							Annotation annot = new Annotation(docID, docNamespace, docTable, -1, targetType, start, end, value, featureMap);
							annot.setProvenance(autoProvenance);
							String featuresStr = gson.toJson(featureMap);
							annot.setFeatures(featuresStr);
							finalAnnotList.add(annot);
							
							/*
							List<Long> valDocList = valDocMap.get(value);
							if (valDocList == null) {
								valDocList = new ArrayList<Long>();
								valDocMap.put(value, valDocList);
							}
							
							if (!valDocList.contains(docID))
								valDocList.add(docID);
								*/
						}
					}
				}
				/*
				else if (prob < 0.0) {
					for (int i=0; i<finalAnnotList.size(); i++) {
						Annotation annot = finalAnnotList.get(i);
						if (annot.getValue().toLowerCase().equals(value)) {
							System.out.println("removing: " + value + "|" + prob);
							pw.println("removing: " + value + "|" + prob);
							finalAnnotList.remove(i);
							valMap.remove(annot.getDocID() + "|" + annot.getStart() + "|" + annot.getEnd());
							valDocMap.remove(value);
							i--;
						}
							
					}
				}
				*/
			}
		}
		
		
		//adding existing annots from previous run
		Statement stmt = conn.createStatement();
		
		ResultSet rs = stmt.executeQuery("select document_id, start, end, value, features from annotation where provenance = '" + existingProvenance + "' and annotation_type = '" + targetType + "' order by document_id, start");
		println("select document_id, start, end, value from annotation where provenance = '" + existingProvenance + "' and annotation_type = '" + targetType + "' order by document_id, start");
		while (rs.next()) {
			long docID = rs.getLong(1);
			long start = rs.getLong(2);
			long end = rs.getLong(3);
			String value = rs.getString(4);
			String features = rs.getString(5);
			
			Boolean flag = valMap.get(docID + "|" + start + "|" + end);
			if (flag == null) {
				println("adding existing: " + value + "|" + docID + "|" + start);
				valMap.put(docID + "|" + start + "|" + end, true);
				matchMap.put(docID + "|" + start + "|" + end, true);
				Map<String, Object> featureMap = new HashMap<String, Object>();
				featureMap = gson.fromJson(features, featureMap.getClass());
				Annotation annot = new Annotation(docID, docNamespace, docTable, -1, targetType, start, end, value, featureMap);
				annot.setProvenance(autoProvenance);
				String featuresStr = gson.toJson(featureMap);
				annot.setFeatures(featuresStr);
				finalAnnotList.add(annot);
			}
		}
			
			
		
		// adding global
		if (global) {
			//pstmt = conn.prepareStatement("select distinct start, end from annotation where document_id = ? and value = ? and provenance = 'conll2003-token'");
			PreparedStatement pstmt = conn.prepareStatement("select distinct document_id, start, end from annotation where document_id >= 1163 and value = ? and annotation_type = 'Token' and (features like '%upperinitial%' or features like '%allCaps%')");
			PreparedStatement pstmt2 = conn.prepareStatement("select count(*) from annotation where document_id < 1163 and value = ? and provenance = 'conll2003-token' and annotation_type != '" + targetType + "'");
			
			List<Annotation> annotList = new ArrayList<Annotation>();
			for (Annotation annot : finalAnnotList) {
				String value = annot.getValue().toLowerCase();
				
				if (value.length() < 2)
					continue;
				
				Double prob = valCountMap.get(value);
				if (prob == null)
					prob = 0.0;

			
				int count = 0;
				pstmt2.setString(1, value);
				rs = pstmt2.executeQuery();
				if (rs.next()) {
					count = rs.getInt(1);
				}
				
				//if (prob == 0.0 && count >= 20)
				if (prob < 0.0 || (prob == 0.0 && count >= 1))
					continue;
				
				pstmt.setString(1, value);
				rs = pstmt.executeQuery();
				while (rs.next()) {
					long docID = rs.getLong(1);
					long start = rs.getLong(2);
					long end = rs.getLong(3);
					
					String key = docID + "|" + start + "|" + end;
					if (valMap.get(key) == null) {  
						System.out.println("adding global: " + value + "|" + docID + "|" + start);
						pw.println("adding global: " + value + "|" + docID + "|" + start);
						Map<String, Object> featureMap = new HashMap<String, Object>();
						featureMap.put("profileID", -2);
						Annotation annot2 = new Annotation(docID, docNamespace, docTable, -1, targetType, start, end, value, featureMap);
						annot2.setProvenance(autoProvenance);
						String featuresStr = gson.toJson(featureMap);
						annot2.setFeatures(featuresStr);
						annotList.add(annot2);
						valMap.put(key, true);
					}
				}
			}
			
			finalAnnotList.addAll(annotList);
		}
		
		
		
		Map<String, Double> entityProbMap = new HashMap<String, Double>();			
		rs = stmt.executeQuery("select value, pos, total, prob from " + probEntityTable);
		while (rs.next()) {
			String value = rs.getString(1);
			double prob = rs.getDouble(4);
			entityProbMap.put(value, prob);
		}
		
		
		// adding global entities
		Map<String, List<String>> globalEntityMap = new HashMap<String, List<String>>();
		if (globalEntity) {		
			
			//get entities
			List<String> entityList = new ArrayList<String>();
			List<Annotation> orderedAnnotList = new ArrayList<Annotation>();
			for (Annotation annot : finalAnnotList) {
				long docID = annot.getDocID();
				long start = annot.getStart();
				
				boolean inserted = false;
				for (int i=0; i<orderedAnnotList.size(); i++) {
					Annotation annot2 = orderedAnnotList.get(i);
					if (docID == annot2.getDocID() && start < annot2.getStart()) {
						orderedAnnotList.add(i, annot);
						inserted = true;
						break;
					}
					else if (docID < annot2.getDocID()) {
						orderedAnnotList.add(i, annot);
						inserted = true;
						break;
					}
				}
				
				if (!inserted)
					orderedAnnotList.add(annot);
			}
			
			long lastDocID = -1;
			long lastStart = -1;
			long lastEnd = -1;
			String lastVal = "";
			List<Long> docIDList = new ArrayList<Long>();
			List<Long> startList = new ArrayList<Long>();
			List<Long> endList = new ArrayList<Long>();
			StringBuilder strBlder = new StringBuilder();
			
			for (Annotation annot : orderedAnnotList) {
				long docID = annot.getDocID();
				
				if (docID == lastDocID && annot.getStart() <= lastEnd + 1) {
					String space = " ";
					if (annot.getStart() == lastEnd)
						space = "";
					
					strBlder.append(space + annot.getValue());
					docIDList.add(annot.getDocID());
					startList.add(annot.getStart());
					endList.add(annot.getEnd());
				}
				else {
					
					if (strBlder.length() > 0) {
						String entityVal = strBlder.toString();
						
						if (entityVal.indexOf(" ") >= 0) {
							/*Double prob = entityProbMap.get(entityVal);
							 
							
							if (prob == null)
								prob = 2.0;
							
							if (prob >= 0.8) {
							*/
												
							entityList.add(entityVal + "|" + docID);
							
							for (int i=0; i<docIDList.size(); i++) {
								long docID2 = docIDList.get(i);
								long start = startList.get(i);
								long end = endList.get(i);
								List<String> entityLocList = globalEntityMap.get(entityVal);
								if (entityLocList == null) {
									entityLocList = new ArrayList<String>();
									globalEntityMap.put(entityVal, entityLocList);
								}
								
								entityLocList.add(docID2 + "|" + start + "|" + end);
								println("adding entity to list: " + entityVal + "|" + docID2 + "|" + start + "|" + end);
		
							}
						}
						
						strBlder = new StringBuilder();
						docIDList.clear();
						startList.clear();
						endList.clear();
					}
					
					//get ngrams
					/*
					String[] toks = entityVal.split(" ");
					for (int i=0; i<toks.length-1; i++) {
						String ngram = toks[i] + " " + toks[i+1];
						entityList.add(ngram);
						println("adding entity to list: " + ngram);
					}
					*/

					strBlder.append(annot.getValue());
					docIDList.add(annot.getDocID());
					startList.add(annot.getStart());
					endList.add(annot.getEnd());
				}
				
				lastDocID = docID;
				lastStart = annot.getStart();
				lastEnd = annot.getEnd();
				lastVal = annot.getValue().toLowerCase();
			}
			
			//pstmt = conn.prepareStatement("select distinct document_id, start, end from annotation where document_id >= 1163 and value = ? and annotation_type = 'Token' and features like '%nnp%'");
			//PreparedStatement pstmt2 = conn.prepareStatement("select count(*) from annotation where document_id < 1163 and value = ? and provenance = 'conll2003-entity' and annotation_type != '" + targetType + "'");
			PreparedStatement pstmt = conn.prepareStatement("select document_id, start, end ,value from annotation where value like ? and annotation_type = 'Sentence' and document_id >= 1163");
			PreparedStatement pstmt2 = conn.prepareStatement("select start, end, value from annotation where document_id = ? and start >= ? and end <= ? and annotation_type = 'Token' order by start");
			PreparedStatement pstmt3 = conn.prepareStatement("select prob from " + probEntityTable + " where value = ?");
			
			List<Annotation> annotList = new ArrayList<Annotation>();
			for (String valStr : entityList) {
				
				String[] parts = valStr.split("\\|");
				String value = parts[0];
				long docID = Long.parseLong(parts[1]);
				
				if (value.length() < 2 || value.indexOf(" ") < 0)
					continue;
				
				pstmt3.setString(1, value);
				rs = pstmt3.executeQuery();
				double prob = 2.0;
				if (rs.next()) {
					prob = rs.getDouble(1);
				}
				
				//low prob or not same doc
				if (prob < 0.8)
					continue;

				
				/*
				if (prob < 0.8)
					continue;
				 */
				
				/*
				Double prob = valCountMap.get(value);
				if (prob == null)
					prob = 0.0;
					*/
			
				/*
				int count = 0;
				pstmt2.setString(1, value);
				rs = pstmt2.executeQuery();
				if (rs.next()) {
					count = rs.getInt(1);
				}
				
				//System.out.println("prob: " + prob + " count: " + count);
				
				if (prob == 0.0 && count >= 5)
				*/
				
								
				pstmt.setString(1, "%" + value + "%");
				rs = pstmt.executeQuery();
				while (rs.next()) {
					long docID2 = rs.getLong(1);
					long start = rs.getLong(2);
					long end = rs.getLong(3);
					String value2 = rs.getString(4);
					
					int step = 0;
					step = value2.indexOf(value);
					
					while (true) {
						if (step < 0)
							break;
						
						long index1 = start + step;
						long index2 = index1 + value.length();
						if (value2.indexOf(value) < 0)
							continue;
						
						pstmt2.setLong(1, docID2);
						pstmt2.setLong(2, start);
						pstmt2.setLong(3, end);
						
						ResultSet rs2 = pstmt2.executeQuery();
						
						while (rs2.next()) {
							long start2 = rs2.getLong(1);
							long end2 = rs2.getLong(2);
							String value3 = rs2.getString(3);
							
							if (!(start2 >= index1 && end2 <= index2))
								continue;
							
							if (value.indexOf(" " + value3 + " ") > 0 || value.indexOf(value3 + " ") == 0 || 
									(value.indexOf(" " + value3) == (value.length() - value3.length()-1) && value3.length() < value.length())) {
								String key = docID2 + "|" + start2 + "|" + end2;
								
								
								
								//System.out.println("value: " + value + " value2: " + value2 + "|" + key);
		
								
								if (valMap.get(key) == null) {  
									println("adding global entity: " + value + "|" + docID2 + "|" + start2 + "|" + value3);
									Map<String, Object> featureMap = new HashMap<String, Object>();
									featureMap.put("profileID", -2);
									Annotation annot2 = new Annotation(docID2, docNamespace, docTable, -1, targetType, start2, end2, value3, featureMap);
									annot2.setProvenance(autoProvenance);
									String featuresStr = gson.toJson(featureMap);
									annot2.setFeatures(featuresStr);
									annotList.add(annot2);
									valMap.put(key, true);
								}
							}
						}
						
						step = value2.indexOf(value, step+1);
					}
				}
			}
			
			finalAnnotList.addAll(annotList);
			
		}
			
		
		
		addSingleEntities(autoProvenance, targetType, probEntityTable, valDocMap, valDocProfileMap);
	
		
		
		//adding same doc token
		
		if (sameDocToken) {
			pstmt = conn.prepareStatement("select distinct start, end from annotation where document_id = ? and value = ? and annotation_type = 'Token' and features like '%nnp%'");
			for (String value : valDocMap.keySet()) {
				if (value.length() < 2)
					continue;
				
				List<Long> docList = valDocMap.get(value);
				List<Long> docProfileList = valDocProfileMap.get(value);
				
				for (int i=0; i<docList.size(); i++) {
					long docID = docList.get(i);
					long profileID = docProfileList.get(i);
					
					pstmt.setLong(1, docID);
					pstmt.setString(2, value);
					rs = pstmt.executeQuery();
					while (rs.next()) {
						long start = rs.getLong(1);
						long end = rs.getLong(2);
						
						Double prob = valCountMap.get(value);
						
						println("same doc checking: " + value + "|" + docID + "|" + start + "|" + prob);
						
						if (prob != null && prob < 0.0)
							continue;
						
						Boolean flag = valMap.get(docID + "|" + start + "|" + end);
						if (flag == null) {
							println("adding same doc: " + value + "|" + docID + "|" + start + "|" + end);
							
							valMap.put(docID + "|" + start + "|" + end, true);
							matchMap.put(docID + "|" + start + "|" + end, true);
							Map<String, Object> featureMap = new HashMap<String, Object>();
							featureMap.put("profileID", profileID);
							Annotation annot = new Annotation(docID, docNamespace, docTable, -1, targetType, start, end, value, featureMap);
							annot.setProvenance(autoProvenance);
							String featuresStr = gson.toJson(featureMap);
							annot.setFeatures(featuresStr);
							finalAnnotList.add(annot);
						}
					}
				}
			}
		}
		
		
		
		
		//remove low prob values
		
		/*
		for (String value : valCountMap.keySet()) {
			double prob = valCountMap.get(value);
			if (prob < 0.0) {
				for (int i=0; i<finalAnnotList.size(); i++) {
					Annotation annot = finalAnnotList.get(i);
					
					
					if (globalEntityMap.get(annot.getDocID() + "|" + annot.getStart() + "|" + annot.getEnd()) != null)
						continue;
						
					
					if (annot.getValue().toLowerCase().equals(value)) {
						System.out.println("removing: " + value + "|" + prob);
						pw.println("removing: " + value + "|" + prob);
						finalAnnotList.remove(i);
						valMap.remove(annot.getDocID() + "|" + annot.getStart() + "|" + annot.getEnd());
						valDocMap.remove(value);
						i--;
					}
				}
					
			}
		}
		*/
		
		
		//check multi word terms
		if (entity)	 {
			Map<String, Boolean> entityMap = new HashMap<String, Boolean>();
			//PreparedStatement pstmt = conn.prepareStatement("select distinct a.document_id, a.start, a.end, a.value from annotation a, annotation b where b.value like ? and b.provenance = 'gate8.0' and "
			//	+ "a.annotation_type = 'Sentence' and a.start >= b.start and a.end <= b.end and a.document_id = b.document_id and a.document_id >= 1163");
			PreparedStatement pstmt = conn.prepareStatement("select distinct document_id, start, end, value from annotation where value like ? and annotation_type = 'Sentence' and document_id >= 1163");
			PreparedStatement pstmt2 = conn.prepareStatement("select start, end, value from annotation where annotation_type = 'Token' and start >= ? and end <= ? and document_id = ? order by start");
			PreparedStatement pstmt3 = conn.prepareStatement("delete from annotation where annotation_type = '" + targetType + "' and provenance = '" + autoProvenance + "' and start >= ? and end <= ? and document_id = ?");
			//Statement stmt = conn.createStatement();
			rs = stmt.executeQuery("select value, pos, total, prob from " + probEntityTable);
			while (rs.next()) {
				String value = rs.getString(1);

				//preprocess value
				List<String> toks = SequenceUtilities.getToksFromStr(value);
				
				int count = 0;
				for (String tok : toks) {
					if (StringUtils.isAlphanumeric(tok))
						count++;
				}
				
				if (count < 2)
					continue;
				
				/*
				String lastTok = null;
				StringBuilder strBlder = new StringBuilder();
				for (String tok : toks) {
					String space = " ";

					if (lastTok != null) {
						if (!StringUtils.isAlphanumeric(lastTok.substring(lastTok.length()-1)) && !lastTok.substring(lastTok.length()-1).equals(".") && tok.length() > 1)
							space = "";
						else if (lastTok.equals(".") && Character.isUpperCase(tok.charAt(0)) && tok.length() == 1)
							space = "";
						else if (Character.isAlphabetic(lastTok.charAt(lastTok.length()-1)) && !StringUtils.isAlphanumeric(tok))
							space = "";
					}
					
					lastTok = tok;
					
					strBlder.append(space + tok);
				}
				
				value = strBlder.toString().trim();
				*/
				
				String queryValue = value;
				queryValue = queryValue.replaceAll("(\\s)+", "%");
				
					
				
				//List<String> valList = new ArrayList<String>();
				//valList.add(value);
				
				//get ngrams
				/*
				for (int len = 2; len<4; len++) {
					String[] toks = value.split(" ");
					for (int i=0; i<toks.length-len+1; i++) {
						StringBuilder strBlder = new StringBuilder();
						for (int j=0; j<len; j++)
							strBlder.append(toks[i+j] + " ");
						
						valList.add(strBlder.toString().trim());
						//println("adding entity to list2: " + strBlder.toString().trim());
					}
				}
				*/
				
				
				int pos = rs.getInt(2);
				int total = rs.getInt(3);
				double prob = rs.getDouble(4);
				
				Boolean flag = null;
				if (prob < minGlobalNegPrec)
					flag = false;
				else if (prob > minGlobalPrec)
					flag = true;
				else
					continue;
				
				//for (String value2 : valList) {
				println("checking entity: " + value + "|" + queryValue + "|" + flag);
				
				
				pstmt.setString(1, "%" + queryValue + "%");
				ResultSet rs2 = pstmt.executeQuery();
				while (rs2.next()) {
					long sentDocID = rs2.getLong(1);
					long sentStart = rs2.getLong(2);
					long sentEnd = rs2.getLong(3);
					String value3 = rs2.getString(4);
					
					AnnotationSequence seq = seqMap.get(sentDocID + "|" + sentStart);
					List<String> sentToks = seq.getToks();
					//sentToks.remove(0);
					//sentToks.remove(sentToks.size()-1);
					
					println("found in: " + sentDocID + "|" + sentStart + "|" + value3 + "|" + gson.toJson(toks) + "|" + gson.toJson(sentToks));
					
					int startIndex = sentToks.size();

					while (true) {
						
						sentToks = sentToks.subList(0, startIndex);
						startIndex = Collections.lastIndexOfSubList(sentToks, toks);
						
						if (startIndex < 0) {
							
							List<String> toks2 = new ArrayList<String>();
							List<String> sentToks2 = new ArrayList<String>();
							for (int i=0;  i<toks.size(); i++) {
								String tok = toks.get(i);
								toks2.add(tok.toLowerCase());
							}
							
							for (int i=0; i<sentToks.size(); i++) {
								String tok = sentToks.get(i);
								sentToks2.add(tok.toLowerCase());
							}
							
							startIndex = Collections.indexOfSubList(sentToks2, toks2);
							
							boolean flag2 = false;
							if (startIndex >= 0) {
								for (int i=startIndex; i<startIndex + toks2.size(); i++) {
									String tok2 = sentToks.get(i);
									for (int j=0; j< tok2.length(); j++) {
										if (Character.isUpperCase(tok2.charAt(j))) {
											flag2 = true;
											break;
										}
									}
								}
							}
							
							if (!flag2)
								startIndex = -1;
						}
						
						if (startIndex >= 0) {
							int endIndex = startIndex + toks.size()-1;
							
							String sentStr = SequenceUtilities.getStrFromToks(sentToks); 
							List<Annotation> annotList = seq.getAnnotList(":token|string");
							
							println("found sublist: " + sentDocID + "|" + sentStart + "|" + startIndex);
							//println("seq: " + gson.toJson(seq));
							
							long valueStart = annotList.get(startIndex).getStart();
							long valueEnd = annotList.get(endIndex).getEnd();
							
							println("found in: " + value3 + "|" + sentStr);
							
							
							//check if phrase is surrounded by lowercase
							PreparedStatement pstmt4 = conn.prepareStatement("select features, value, start from annotation where document_id = ? and end <= ? and annotation_type = 'Token' order by end desc limit 0, 1");
							PreparedStatement pstmt5 = conn.prepareStatement("select features, value, start from annotation where document_id = ? and start >= ? and annotation_type = 'Token' order by start limit 0, 1");
							PreparedStatement pstmt6 = conn.prepareStatement("select count(*) from annotation where document_id = ? and end >= ? and annotation_type = 'Sentence' order by end limit 0, 1");
							PreparedStatement pstmt7 = conn.prepareStatement("select count(*) from annotation where document_id = ? and start >= ? and annotation_type = 'Sentence' order by start limit 0, 1");
	
							boolean flag1 = false;
							boolean flag2 = false;
							boolean flag3 = false;
							boolean flag4 = false;
							
							pstmt4.setLong(1, sentDocID);
							pstmt4.setLong(2, valueStart);
							ResultSet rs3 = pstmt4.executeQuery();
							if (rs3.next()) {
								String features2 = rs3.getString(1);
								String val2 = rs3.getString(2);
								long start2 = rs3.getLong(3);
								
								println("checking entity prev: " + features2 + "|" + val2 + "|" + start2);
								
								if (features2.indexOf("upper") < 0 && features2.indexOf("allCaps") < 0 && features2.indexOf("mixedCaps") < 0 && !val2.equals(".")) {
									flag1 = true;
									flag3 = true;
								}
								else {
									pstmt7.setLong(1, sentDocID);
									pstmt7.setLong(2, start2+1);
									
									ResultSet rs4 = pstmt7.executeQuery();
									int sentCount = 0;
									if (rs4.next()) {
										sentCount = rs4.getInt(1);
									}
									
									if (sentCount > 0) {
										flag1 = true;
										//flag3 = true;
									}
								}
							}
							else
								flag1 = true;
							
							pstmt5.setLong(1, sentDocID);
							pstmt5.setLong(2, valueEnd);
							rs3 = pstmt5.executeQuery();
							if (rs3.next()) {
								String features2 = rs3.getString(1);
								String val2 = rs3.getString(2);
								long start2 = rs3.getLong(3);
								
								println("checking entity next: " + features2 + "|" + val2 + "|" + start2);
								
								
								if (features2.indexOf("upper") < 0 && features2.indexOf("allCaps") < 0 && features2.indexOf("mixedCaps") < 0 && !val2.equals(".")) {
									flag2 = true;
									flag4 = true;
								}
								else {
									pstmt6.setLong(1, sentDocID);
									pstmt6.setLong(2, start2+1);
									
									ResultSet rs4 = pstmt6.executeQuery();
									int sentCount = 0;
									if (rs4.next()) {
										sentCount = rs4.getInt(1);
									}
									
									if (sentCount > 0) {
										flag2 = true;
										//flag4 = true;
									}
								}
							}
							else
								flag2 = true;
							
							
							
							if (!flag1 || !flag2)
								continue;
							
							
							
							/*
							if (sentStr.indexOf(" " + value + " ") > 0 || sentStr.indexOf(value + " ") == 0 || 
								(sentStr.indexOf(value + " ")-1 >= 0 && !Character.isAlphabetic(sentStr.charAt(sentStr.indexOf(value + " ")-1))) || 
								(sentStr.indexOf(" " + value) + value.length()+1 < sentStr.length() && !Character.isAlphabetic(sentStr.charAt(sentStr.indexOf(" " + value) + value.length()+1))) ||
								((sentStr.indexOf(value)-1 >= 0 && !Character.isAlphabetic(sentStr.charAt(sentStr.indexOf(value)-1))) && 
									(sentStr.indexOf(value) + value.length() < sentStr.length() && !Character.isAlphabetic(sentStr.charAt(sentStr.indexOf(value) + value.length())))) ||
								(sentStr.indexOf(" " + value) == (sentStr.length() - value.length()-1) && value.length() < sentStr.length())) {
							
							
							int step = sentStr.indexOf(value);
							if (step < 0)
								continue;
							*/
							
							
							String key = sentDocID + "|" + valueStart + "|" + valueEnd;
							
							//System.out.println("entity map: " + value + "|" + docID + "|" + start + "|" + end + "|" + flag);
							entityMap.put(key, flag);
							
							if (entity && flag != null) {
								println("entity add/remove: " + value + "|" + sentStr + "|" + key);
								
								pstmt2.setLong(1, valueStart);
								pstmt2.setLong(2, valueEnd);
								pstmt2.setLong(3, sentDocID);
								
								rs3 = pstmt2.executeQuery();
								while (rs3.next()) {
									long start2 = rs3.getLong(1);
									long end2 = rs3.getLong(2);
									String value4 = rs3.getString(3);
									
									String key2 = sentDocID + "|" + start2 + "|" + end2;
									
									if (flag && valMap.get(key2) == null) {
										println("adding entity token: " + key2 + "|" + value4);
										valMap.put(key2, true);
										matchMap.put(key2, true);
										Map<String, Object> featureMap = new HashMap<String, Object>();
										featureMap.put("profileID", -1);
										Annotation annot2 = new Annotation(sentDocID, docNamespace, docTable, -1, targetType, start2, 
											end2, value4.toLowerCase(), featureMap);
										annot2.setProvenance(autoProvenance);
										String featuresStr = gson.toJson(featureMap);
										annot2.setFeatures(featuresStr);
										finalAnnotList.add(annot2);
										
										List<Long> docList = valDocMap.get(value4);
										List<Long> docProfileList = valDocProfileMap.get(value4);
										if (docList == null) {
											docList = new ArrayList<Long>();
											valDocMap.put(value4, docList);
											docProfileList = new ArrayList<Long>();
											valDocProfileMap.put(value4, docProfileList);
										}
										
										if (!docList.contains(sentDocID)) {
											docList.add(sentDocID);
											docProfileList.add((long) -1);
										}
									}
									else if (!flag && valMap.get(key2) != null && flag3 && flag4) {
										println("removing entity token: " + key2 + "|" + value4);
										for (int i=0; i<finalAnnotList.size(); i++) {
											Annotation annot = finalAnnotList.get(i);
											if (annot.getDocID() == sentDocID && annot.getStart() >= start2 && annot.getEnd() <= end2) {
												finalAnnotList.remove(i);
												i--;
												valMap.remove(key2);
											}
										}
									}
								}
							}
							
							//sentToks = sentToks.subList(startIndex+1, sentToks.size());
							//startIndex = Collections.indexOfSubList(sentToks, toks);
						}
						else
							break;
					}
				}
			
				/*
				else if (!flag) {

					List<String> entityInfoList = globalEntityMap.get(value);
					if (entityInfoList == null) {
						if (StringUtils.isAllUpperCase(value)) {
							StringBuilder strBlder = new StringBuilder();
							for (String tok : toks) {
								strBlder.append(tok.substring(0, 1) + tok.substring(1).toLowerCase() + " ");
							}
							
							value = strBlder.toString().trim();
							entityInfoList = globalEntityMap.get(value);
						}
						
						else {
							value = value.toUpperCase();
							entityInfoList = globalEntityMap.get(value);
						}							
					}
					
					if (entityInfoList != null) {
						for (String entityInfo : entityInfoList) {
							String[] parts = entityInfo.split("\\|");
							long docID = Long.parseLong(parts[0]);
							long start = Long.parseLong(parts[1]);
							long end = Long.parseLong(parts[2]);
							
							println("removing low prob entity: " + value + "|" + entityInfo);
							
							for (int i=0; i<finalAnnotList.size(); i++) {
								Annotation annot = finalAnnotList.get(i);
								if (annot.getDocID() == docID && annot.getStart() >= start && annot.getEnd() <= end) {
									finalAnnotList.remove(i);
									i--;
								}
							}
						}
					}
				}
				*/
			}
			
		//}
		
		
		
			for (int i=0; i<finalAnnotList.size(); i++) {
				Annotation annot = finalAnnotList.get(i);
				String key = annot.getDocID() + "|" + annot.getStart() + "|" + annot.getEnd();
				Boolean flag = entityMap.get(key);
				
				//System.out.println(annot.getValue() + "|" + key + "|" + flag);
	
				
				if (flag != null && !flag) {
					println("removing entity: " + annot.getValue() + "|" + key);
					finalAnnotList.remove(i);
					i--;
					valMap.remove(key);
				}
			}
		}
		
		
		//pstmt.close();
	}
	
	
	private void localPatternMatcher(List<ProfileMatch> matchList) throws SQLException
	{
		println("\n\nLOCAL MATCH");
		
		
		Map<Long, MSAProfile> profileMap = new HashMap<Long, MSAProfile>();
		Map<Long, Map<Long, Integer>> profileDocMap = new HashMap<Long, Map<Long, Integer>>();
		for (ProfileMatch match : matchList) {
			long docID = match.getSequence().getDocID();
			Map<Long, Integer> profileCountMap = profileDocMap.get(docID);
			if (profileCountMap == null) {
				profileCountMap = new HashMap<Long, Integer>();
				profileDocMap.put(docID, profileCountMap);
			}
			
			
			long profileID = match.getProfile().getProfileID();
			Integer count = profileCountMap.get(profileID);
			if (count == null)
				count = 0;
			profileCountMap.put(profileID, ++count);
			
			//ProfileGrid profileGrid2 = profileIDMap.get(profileID);
			//System.out.println("docID: " + docID + " profileID: " + profileID + " count: " + count + " profile: " + gson.toJson(profileGrid2.getGrid().getSequence().getToks()));
			
			if (count >= 3) {
				MSAProfile profile = profileMap.get(profileID);
				if (profile == null)
					profileMap.put(profileID, match.getProfile());
			}
		}
		
		profileMatcher.setMinSizeOffset(1);
		
		for (long docID : profileDocMap.keySet()) {
			Map<Long, Integer> profileCountMap = profileDocMap.get(docID);
			List<ProfileGrid> profileGridList = new ArrayList<ProfileGrid>();

			for (long profileID : profileCountMap.keySet()) {
				int count = profileCountMap.get(profileID);
				if (count >= 0) {
					ProfileGrid profileGrid = profileIDMap.get(profileID);
					if (profileGrid.getGrid().getSequence().getToks().size() < 3)
						continue;
					
					MSAProfile profile = msaProfileMap.get(profileGrid.getGrid());
					println("docID: " + docID + " profileID: " + profileID + " count: " + count + " profile: " + gson.toJson(profileGrid.getGrid().getSequence().getToks()));
					
					
					
					AnnotationSequenceGrid grid = profileGrid.getGrid().clone();
					msaProfileMap.put(grid, profile);
					profileGrid.setGrid(grid);
					for (int i=0; i<grid.size(); i++) {
						List<AnnotationGridElement> col = grid.get(i);
						for (int j=0; j<col.size(); j++) {
							AnnotationGridElement elem = col.get(j);
							String tok = elem.getTok();
							/*
							if (tok.startsWith(":lookup") && col.size() > 1) {
								//System.out.println("tok: " + tok + " setting to :null");
								col.remove(j);
								j--;
							}
							*/
							
							if (tok.equals(":number")) {
								List<AnnotationGridElement> col2 = new ArrayList<AnnotationGridElement>();
								col2.add(elem);
								grid.setColumn(i, col2);
								break;
							}
							
							/*
							if (tok.equals(":" + targetType.toLowerCase()) && col.size() > 1) {
								//System.out.println("tok: " + tok + " setting to :null");
								col.remove(j);
								j--;
							}
							*/
						}
					}
					
					
					
					//profileGrid.addTarget(targetIDMap.get((long) 3251));
					//profileGrid.addTarget(targetIDMap.get((long) 3239));
					profileGrid.addTarget(targetIDMap.get((long) 95518));
					profileGrid.addTarget(targetIDMap.get((long) 96130));
					println("new profile: " + gson.toJson(profileGrid.getGrid().getSequence().getToks()));
					println(grid.toString());
					//msaProfileMap.put(grid, profile);
					
					
					profileGridList.add(profileGrid);
				}
			}
			
			if (profileGridList.size() > 0) {
				List<AnnotationSequence> seqList = getAnnotationSequencesForDoc(docID);
				List<AnnotationSequenceGrid> gridList = new ArrayList<AnnotationSequenceGrid>();
				for (AnnotationSequence seq : seqList) {
					List<AnnotationSequenceGrid> gridList2 = genGrid.toAnnotSeqGrid(seq, false, false, false, true, false);
					gridList.addAll(gridList2);
				}
				
				ProfileInvertedIndex invertedIndex = new ProfileInvertedIndex();
				invertedIndex.setTargetFlag(true);
				invertedIndex.setMaxGaps(maxGaps);
				invertedIndex.genIndex(profileGridList, targetGridList, profileIDMap, targetIDMap);
				
				List<ProfileMatch> matchList2 = profileMatcher.matchProfile(gridList, profileGridList, targetGridList, targetType, false, maxGaps, 1, 0, false, msaProfileMap, msaTargetProfileMap, invertedIndex);
				
				Map<Long, List<ProfileMatch>> localMatchMap = new HashMap<Long, List<ProfileMatch>>();
				for (ProfileMatch match : matchList2) {
					long profileID = match.getProfile().getProfileID();
					List<ProfileMatch> matchList3 = localMatchMap.get(profileID);
					if (matchList3 == null) {
						matchList3 = new ArrayList<ProfileMatch>();
						localMatchMap.put(profileID, matchList3);
					}
					
					matchList3.add(match);
				}
					
				for (long profileID : localMatchMap.keySet()) {
					List<ProfileMatch> matchList3 = localMatchMap.get(profileID);
					int pos = 0;
					for (ProfileMatch match : matchList3) {
						long start = match.getTargetIndexes()[0];
						long end = match.getTargetIndexes()[1];
						if (valMap.get(docID + "|" + start + "|" + end) != null) {
							pos++;
						}
					}
					
					if (pos >= 3) {
						for (ProfileMatch match :  matchList3) {
							//docID = match.getSequence().getDocID();
							long start = match.getTargetIndexes()[0];
							long end = match.getTargetIndexes()[1];
							
							List<MSAProfile> targetList = match.getTargetList();
							if (targetList.get(0).getProfileID() == 3251 && match.getTargetStr().length() > 1)
								continue;
							
							
							
							//Boolean flag = matchMap.get(docID + "|" + start + "|" + end);
							String value = match.getTargetStr().toLowerCase();
							Boolean flag = valMap.get(docID + "|" + start + "|" + end);
							if (flag == null) {
								matchMap.put(docID + "|" + start + "|" + end, true);
								valMap.put(docID + "|" + start + "|" + end, true);
								System.out.println("NEW MATCH! " + docID + "|" + start + "|" + end + ", profile: " + match.getProfile().getProfileStr() + ", value: " + value);
								pw.println("NEW MATCH! " + docID + "|" + start + "|" + end + ", profile: " + match.getProfile().getProfileStr() + ", value: " + value);
								
								Annotation annot = new Annotation(docID, docNamespace, docTable, -1, targetType, 
										start, end, match.getTargetStr(), null);
								annot.setProvenance(autoProvenance);
								finalAnnotList.add(annot);
								finalMatchList.add(match);
							}
						}
					}
				}
			}
		}
	}
	
	
	private void addSingleEntities(String provenance, String annotType, String probTable, Map<String, List<Long>> valDocMap, Map<String, List<Long>> valDocProfileMap) throws SQLException
	{
		Statement stmt = conn.createStatement();
		PreparedStatement pstmt = conn.prepareStatement("select document_id, start, end, features from annotation where value = ? and annotation_type = 'Token' and document_id >= 1163 order by document_id, start");
		//PreparedStatement pstmt2 = conn.prepareStatement("insert into annotation (id, document_namespace, document_table, document_id, annotation_type, start, end, value, features, provenance, score) "
		//		+ "values (?, 'ner', 'conll2003_document',?,?,?,?,?,?,?,?)");
		PreparedStatement pstmt3 = conn.prepareStatement("select features, value, start from annotation where document_id = ? and end >= ? and end < ? and annotation_type = 'Token'");
		PreparedStatement pstmt4 = conn.prepareStatement("select features, value, start from annotation where document_id = ? and start <= ? and start > ? and annotation_type = 'Token'");
		PreparedStatement pstmt5 = conn.prepareStatement("select count(*) from annotation where document_id = ? and start = ? and provenance = '" + provenance + "'");
		PreparedStatement pstmt6 = conn.prepareStatement("select count(*) from annotation where document_id = ? and end = ? and annotation_type = 'Sentence'");
		
		
		List<String> valList = new ArrayList<String>();
		List<Double> probList = new ArrayList<Double>();
		ResultSet rs = stmt.executeQuery("select distinct value, prob from " + probTable + " where value not like '% %' and (prob = 1.0 or prob = 0.0)");
		while (rs.next()) {
			String value = rs.getString(1);
			double prob = rs.getDouble(2);
			valList.add(value);
			probList.add(prob);
		}
		
		for (int i=0; i<valList.size(); i++) {
			String val = valList.get(i);
			double prob = probList.get(i);

			pstmt.setString(1, val);
			rs = pstmt.executeQuery();
			while (rs.next()) {
				long docID = rs.getLong(1);
				long start = rs.getLong(2);
				long end = rs.getLong(3);
				String features = rs.getString(4);
				
				
				if (features.indexOf("upper") >= 0 || features.indexOf("allCaps") >= 0 || features.indexOf("mixedCaps") >= 0) {
					println("checking: " + val + " " + docID + "|" + start + "|" + annotType + "|" + prob + "|" + provenance);
					
					boolean flag1 = false;
					boolean flag2 = false;
					
					pstmt3.setLong(1, docID);
					pstmt3.setLong(2, start-1);
					pstmt3.setLong(3, start+1);
					ResultSet rs2 = pstmt3.executeQuery();
					if (rs2.next()) {
						String features2 = rs2.getString(1);
						String val2 = rs2.getString(2);
						long start2 = rs2.getLong(3);
						
						println("checking prev: " + features2 + "|" + val2 + "|" + start2);
						
						if (features2.indexOf("upper") < 0 && features2.indexOf("allCaps") < 0 && features2.indexOf("mixedCaps") < 0 && !val2.equals(".") && start2 > 0)
							flag1 = true;
					}
					
					pstmt4.setLong(1, docID);
					pstmt4.setLong(2, end+1);
					pstmt4.setLong(3, end-1);
					rs2 = pstmt4.executeQuery();
					if (rs2.next()) {
						String features2 = rs2.getString(1);
						String val2 = rs2.getString(2);
						long start2 = rs2.getLong(3);
						
						println("checking next: " + features2 + "|" + val2 + "|" + start2);
						
						if (features2.indexOf("upper") < 0 && features2.indexOf("allCaps") < 0 && features2.indexOf("mixedCaps") < 0 && !val2.equals("."))
							flag2 = true;
						else if (val2.equals(".")) {
							pstmt6.setLong(1, docID);
							pstmt6.setLong(2, start2+1);
							
							ResultSet rs3 = pstmt6.executeQuery();
							int sentCount = 0;
							if (rs3.next()) {
								sentCount = rs3.getInt(1);
							}
							
							if (sentCount > 0)
								flag2 = true;
						}
					}
					
					
					/*
					int count = 0;
					if (flag1 && flag2) {
						pstmt5.setLong(1, docID);
						pstmt5.setLong(2, start);
						rs2 = pstmt5.executeQuery();
						if (rs2.next()) {
							count = rs2.getInt(1);
						}
					}
					*/
					
					println("prob: " + prob);
					
					if (flag1 && flag2) {
						if (prob == 1.0) {
							
							if (valMap.get(docID + "|" + start + "|" + end) != null)
								continue;
						
							/*
							int id = getNextAnnotID(docID);
							pstmt2.setInt(1, id);
							pstmt2.setLong(2, docID);
							pstmt2.setString(3, annotType);
							pstmt2.setLong(4, start);
							pstmt2.setLong(5, end);
							pstmt2.setString(6, val);
							pstmt2.setString(7, null);
							pstmt2.setString(8, provenance);
							pstmt2.setDouble(9, 1);
							pstmt2.execute();
							*/
							
							Map<String, Object> featureMap = new HashMap<String, Object>();
							featureMap.put("profileID", -1);
							Annotation annot = new Annotation(docID, docNamespace, docTable, -1, targetType, 
								start, end, val, featureMap);
							annot.setProvenance(autoProvenance);
							String featuresStr = gson.toJson(featureMap);
							annot.setFeatures(featuresStr);
							finalAnnotList.add(annot);
							
							println("adding single entity: " + val + " " + docID + "|" + start);
							
							valMap.put(docID + "|" + start + "|" + end, true);
							
							List<Long> docList = valDocMap.get(val);
							List<Long> docProfileList = valDocProfileMap.get(val);
							if (docList == null) {
								docList = new ArrayList<Long>();
								valDocMap.put(val, docList);
								docProfileList = new ArrayList<Long>();
								valDocProfileMap.put(val, docProfileList);
							}
							
							if (!docList.contains(docID)) {
								docList.add(docID);
								docProfileList.add((long) -1);
							}
							
						}
					
						else if (prob == 0.0) {
							for (int j=0; j<finalAnnotList.size(); j++) {
								Annotation annot = finalAnnotList.get(j);
								if (annot.getDocID() == docID && annot.getStart() == start && annot.getEnd() == end) {
									println("removing single entity: " + val + "|" + docID + "|" + start);
									finalAnnotList.remove(j);
									j--;
								}
							}
						}
					}
				}
			}
		}
		
		
		
		stmt.close();
	} 
	
	
	private List<AnnotationSequence> getAnnotationSequencesForDoc(long docID)
	{
		List<AnnotationSequence> seqList = new ArrayList<AnnotationSequence>();
		for (AnnotationSequence seq : negSeqList) {
			if (seq.getDocID() == docID)
				seqList.add(seq);
		}
		
		return seqList;
	}
	
	
	private void genValProbMap() throws SQLException
	{
		Statement stmt = conn.createStatement();
		
		valCountMap = new HashMap<String, Double>();
		Map<String, Integer> totalMap = new HashMap<String, Integer>();
		
		//ResultSet rs = stmt.executeQuery("select value, count(*) from annotation where document_id < 1163 and provenance = 'conll2003-token' and annotation_type = 'I-PER' group by value");
		ResultSet rs = stmt.executeQuery("select value, pos, total, prob from " + probTable);
		while (rs.next()) {
			String value = rs.getString(1);
			int count = rs.getInt(2);
			int total = rs.getInt(3);
			double prob = rs.getDouble(4);
			
			if (StringUtils.isAllUpperCase(value))
				value = value.substring(0, 1) + value.substring(1).toLowerCase();
			
			Integer total2 = totalMap.get(value);
			if (total2 == null) {
				total2 = 0;
			}
			else {
				double prob2 = valCountMap.get(value);
				prob = ((double) prob) * (((double) total) / ((double) total+total2)) +
						((double) prob2) * (((double) total2) / ((double) total+total2));
				
			}

			total += total2;
			totalMap.put(value, total);
			
			println("global prob: " + value + "|" + prob + "|" + count + "|" + total);
			
			valCountMap.put(value, prob);
		}
		
		for (String value : valCountMap.keySet()) {
			double prob = valCountMap.get(value);
			int total = totalMap.get(value);
			double prob2 = 0.0;
			
			if (prob > minGlobalPrec && total > minGlobalCount) {	
				prob2 = prob;
				valCountMap.put(value, prob);
			}
			else if (prob <= minGlobalNegPrec && total > minGlobalNegCount) {
				prob2 = -1.0;
				valCountMap.put(value, -1.0);
			}
			
			else
				valCountMap.put(value, 0.0);
			
			println("global prob score: " + value + "|" + prob + "|" + total + "|" + prob2);

				
		}
		
		stmt.close();
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
	
	private void println(String str)
	{
		System.out.println(str);
		pw.println(str);
	}
	
	private void readAnswers(String annotType, String provenance, long docID) throws SQLException
	{
		Statement stmt = conn.createStatement();
		
		/*
		PreparedStatement pstmt = conn.prepareStatement("select start, " + rq + "end" + rq + " from " + schema + "annotation where document_id = ? and annotation_type = 'Token' and start >= ? and " + rq + "end" + rq + " <= ? order by start");
		
		String annotType2 = "B" + annotType.substring(1);
		String queryStr = "select start, " + rq + "end" + rq + ", value, annotation_type from " + schema + "annotation "
			+ "where document_id = " + docID + " and ";
		
		String annotClause = "(provenance = '" + targetProvenance + "' and (annotation_type = '" + annotType + "' or annotation_type = '" + annotType2 + "')";
		
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
			
			}
		}
		 */			

		
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
		
		
		ResultSet rs = stmt.executeQuery("select distinct start, " + rq + "end" + rq + ", annotation_type, value "
			+ "from " + schema + "annotation where document_id = " + docID + " and annotation_type = '" + annotType
			+ "' and provenance = '" + provenance + "' order by start");
		
		while (rs.next()) {
			int start = rs.getInt(1);
			int end = rs.getInt(2);
			String aType = rs.getString(3);
			String value = rs.getString(4);
			
			Map<String, String> map = new HashMap<String, String>();
			map.put("annotType", aType);
			map.put("value", value);
			
			String mapStr = gson.toJson(map);
			ansMap.put(docID + "|" + start + "|" + end, mapStr);
		}
		
		
		stmt.close();
		//pstmt.close();
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
			AutoAnnotateNER auto = new AutoAnnotateNER();
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
