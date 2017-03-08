package msa;

import java.io.*;
import java.util.*;

import com.google.gson.Gson;

import align.AnnotationGridElement;
import align.AnnotationSequenceGrid;
import align.GenAnnotationGrid;

import java.sql.*;

import msa.ProfileReader.Order;
import msa.db.MSADBInterface;
import msa.db.MySQLDBInterface;
import utils.db.DBConnection;

public class FilterPatterns
{
	private Gson gson;
	private GenSentences genSent;
	private ProfileStats stats;
	private MSADBInterface db;
	
	private String host;
	private String dbType;
	private String keyspace;
	private String msaKeyspace;
	
	private String docNamespace;
	private String docTable;
	
	private String docDBHost;
	private String docDBName;
	private String docDBQuery;
	private String docDBType;

	private boolean punct;
	private boolean write;
	private boolean verbose;
	private int maxGaps;
	private int syntax;
	private int phrase;
	private int blockSize;
	
	private String tokType;
	private int limit;
	private List<String> annotTypeNameList;
	
	private Boolean requireTarget = true;
	
	private List<Map<String, Object>> msaAnnotFilterList;
	private List<Double> scoreList;
	//private List<Double> targetScoreList;
	private String targetType;
	private String targetType2;
	private String targetProvenance;
	private int profileType = 0;

	private String indexTable;
	private double negFilterThreshold;
	private int negFilterMinCount;
	private double posFilterThreshold;
	private int posFilterMinCount;
	private double targetMinScore;
	
	private String group;
	private String targetGroup;
	private String profileTable;
	private int clusterSize;
	
	private List<Long> docIDList;
	private Map<String, List<Long>> docIDMap;
	
	private Connection conn;
	private Connection docDBConn;
	
	private List<String> annotTypeList;
	private List<String> profileTableList;
	private List<String> indexTableList;
	private Map<String, Boolean> requireTargetMap;

	
	private Map<String, Object> targetMap;
	private Map<String, Object> targetMap2;
	
	private PreparedStatement pstmtGetFilterStatus;
	private PreparedStatement pstmtInsertFilterStatus;
	private PreparedStatement pstmtUpdateFilterStatus;
	
	private long minDocID;
	
	
	public FilterPatterns()
	{
		genSent = new GenSentences();
		stats = new ProfileStats();
		
		gson = new Gson();
	}
	
	public void setDocIDList(List<Long> docIDList)
	{
		this.docIDList = docIDList;
	}
	
	public void setProfileTable(String profileTable)
	{
		this.profileTable = profileTable;
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
	
	public void setMinDocID(long minDocID)
	{
		this.minDocID = minDocID;
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
	
	public void close()
	{
		try {
			stats.close();
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	public List<Long> getDocIDList()
	{
		return docIDList;
	}
	
	public Map<String, List<Long>> getDocIDMap()
	{
		return docIDMap;
	}
	
	public void init(Properties props)
	{
		try {			
			host = props.getProperty("host");
			keyspace = props.getProperty("keyspace");
			msaKeyspace = props.getProperty("msaKeyspace");
			dbType = props.getProperty("dbType");
			
			docNamespace = props.getProperty("docNamespace");
			docTable = props.getProperty("docTable");
			
			punct = Boolean.parseBoolean(props.getProperty("punctuation"));
			write = Boolean.parseBoolean(props.getProperty("write"));
			verbose = Boolean.parseBoolean(props.getProperty("verbose"));
			maxGaps = Integer.parseInt(props.getProperty("maxGaps"));
			syntax = Integer.parseInt(props.getProperty("syntax"));
			phrase = Integer.parseInt(props.getProperty("phrase"));

			targetType = props.getProperty("targetType");
			targetType2 = props.getProperty("targetType2");
			targetProvenance = props.getProperty("targetProvenance");
			tokType = props.getProperty("tokType");
			group = props.getProperty("group");
			targetGroup = props.getProperty("targetGroup");
			clusterSize = Integer.parseInt(props.getProperty("clusterSize"));
			
			profileTable = props.getProperty("profileTable");
			indexTable = props.getProperty("indexTable");
			requireTarget = Boolean.parseBoolean(props.getProperty("requireTarget"));

			
			if (targetType != null) {
				annotTypeList = new ArrayList<String>();
				annotTypeList.add(targetType);
				
				profileTableList = new ArrayList<String>();
				profileTableList.add(profileTable);
				
				indexTableList = new ArrayList<String>();
				indexTableList.add(indexTable);
			}
			
			if (requireTarget == null) {
				requireTargetMap = new HashMap<String, Boolean>();
				requireTargetMap.put(targetType, requireTarget);
			}
			
			
			
			limit = -1;
			String limitStr = props.getProperty("limit");
			if (limitStr != null)
				limit = Integer.parseInt(limitStr);
			
			if (dbType.equals("mysql"))
				db = new MySQLDBInterface();
				
			
			genSent.setVerbose(verbose);
			genSent.setTokenType(tokType);
			
			msaAnnotFilterList = new ArrayList<Map<String, Object>>();
			msaAnnotFilterList = gson.fromJson(props.getProperty("msaAnnotFilterList"), msaAnnotFilterList.getClass());
			
			scoreList = new ArrayList<Double>();
			scoreList = gson.fromJson(props.getProperty("scoreList"), scoreList.getClass());
			
			//targetScoreList = new ArrayList<Double>();
			//targetScoreList = gson.fromJson(props.getProperty("targetScoreList"), targetScoreList.getClass());
			
			negFilterThreshold = Double.parseDouble(props.getProperty("negFilterThreshold"));
			negFilterMinCount = Integer.parseInt(props.getProperty("negFilterMinCount"));
			posFilterThreshold = Double.parseDouble(props.getProperty("posFilterThreshold"));
			posFilterMinCount = Integer.parseInt(props.getProperty("posFilterMinCount"));
			
			
			targetMap = new HashMap<String, Object>();
			targetMap.put("annotType", targetType);
			targetMap.put("target", true);
			targetMap.put("provenance", targetProvenance);
			//targetMap.put("targetStr", ":" + targetType.toLowerCase());
			msaAnnotFilterList.add(targetMap);
			
			if (targetType2 != null) {
				targetMap2 = new HashMap<String, Object>();
				targetMap2.put("annotType", targetType2);
				targetMap2.put("target", true);
				targetMap2.put("provenance", targetProvenance);
				targetMap2.put("targetStr", ":target2");
				msaAnnotFilterList.add(targetMap);
				profileType = 2;
			}
			
			blockSize = Integer.parseInt(props.getProperty("blockSize"));
			
			
			annotTypeNameList = MSAUtils.getAnnotationTypeNameList(msaAnnotFilterList, tokType);
			//annotTypeNameList.add(":" + targetType.toLowerCase());
			//annotTypeNameList.add(":" + targetType.toLowerCase());
			scoreList.add(10.0);
			
			
			List<Map<String, Object>> contextAnnotFilterList = new ArrayList<Map<String, Object>>();
			contextAnnotFilterList = gson.fromJson(props.getProperty("contextAnnotFilterList"), contextAnnotFilterList.getClass());
			
			List<String> profileGroupList = new ArrayList<String>();
			profileGroupList = gson.fromJson(props.getProperty("profileGroupList"), profileGroupList.getClass());
			
			//check for docID list info
			docIDList = null;
			docDBQuery = props.getProperty("docDBQuery");
			docDBHost = props.getProperty("docDBHost");
			docDBName = props.getProperty("docDBName");
			docDBType = props.getProperty("docDBType");
			
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	public void filterPatterns(String annotUser, String annotPassword, String docUser, String docPassword, String msaUser, String msaPassword)
	{
		try {
			/*
			if (docDBQuery != null) {				
				docDBConn = DBConnection.dbConnection(docUser, docPassword, docDBHost, docDBName, docDBType);	
				//docIDList = getDocIDList(docDBQuery);
				docIDList = MSAUtils.getDocIDList(docDBConn, docDBQuery);
				docDBConn.close();
			}
			*/
			
			
			docIDMap = new HashMap<String, List<Long>>();
			
			db.init(annotUser, annotPassword, host, keyspace, msaKeyspace);
			
			conn = DBConnection.dbConnection(msaUser, msaPassword, host, msaKeyspace, dbType);
			pstmtGetFilterStatus = conn.prepareStatement("select document_id from filter_status where annotation_type = ?");
			pstmtInsertFilterStatus = conn.prepareStatement("insert into filter_status (annotation_type, document_id) values (?,?)");
			pstmtUpdateFilterStatus = conn.prepareStatement("update filter_status set document_id = ? where annotation_type = ?");
			
			if (docDBQuery == null)
				docDBQuery = "select distinct document_id from document_status where annotation_type = ? and b.status = 1 order by document_id";
			PreparedStatement pstmtGetDocIDs = conn.prepareStatement(docDBQuery);
			
			Statement stmt = conn.createStatement();
			
			for (int index=0; index<annotTypeList.size(); index++) {
				
				
				targetType = annotTypeList.get(index);
				profileTable = profileTableList.get(index);
				indexTable = indexTableList.get(index);

			
				if (targetType.length() == 0 || profileTable.length() == 0 || indexTable.length() == 0)
					continue;
				
				stmt.execute("delete from " + indexTable + " where document_id in (select distinct a.document_id from document_status a where status = 1) and "
					+ "profile_id in (select distinct b.profile_id from " + profileTable + " b where b.annotation_type = '" + targetType + "')");
			
				targetMap.put("targetStr", ":" + targetType.toLowerCase());		
				annotTypeNameList.add(":" + targetType.toLowerCase());

				
				docIDList = new ArrayList<Long>();
				
				/*
				long filterDocID = -1;
				pstmtGetFilterStatus.setString(1, targetType);
				ResultSet rs = pstmtGetFilterStatus.executeQuery();
				if (rs.next()) {
					filterDocID = rs.getLong(1);
				}
				
				if (filterDocID == -1) {
					pstmtInsertFilterStatus.setString(1, targetType);
					pstmtInsertFilterStatus.setLong(2, 0);
					pstmtInsertFilterStatus.execute();
				}
				*/
				
				//pstmtGetDocIDs.setString(1, targetType);
				//pstmtGetDocIDs.setLong(2, filterDocID);
				ResultSet rs = pstmtGetDocIDs.executeQuery();
				while (rs.next()) {
					docIDList.add(rs.getLong(1));
				}
				
				docIDMap.put(targetType, docIDList);
				
				/*
				if (docIDList.size() > 0) {
					pstmtUpdateFilterStatus.setLong(1, docIDList.get(docIDList.size()-1));
					pstmtUpdateFilterStatus.setString(2, targetType);
					pstmtUpdateFilterStatus.execute();
				}
				*/


				//generate the sentences
				genSentences(docIDList, docNamespace, docTable, requireTarget, punct, limit);
				List<AnnotationSequence> posSeqList = genSent.getPosSeqList();
				List<AnnotationSequence> negSeqList = genSent.getNegSeqList();
				
				
				//loop generating blocks of grids no more than size msaBlockSize
				
				//int numBlocks = (posSeqList.size() / msaBlockSize) + 1;
				//int currStartIndex = 0;
				
				//for (int blockNum=0; blockNum<numBlocks; blockNum++) {
				
				
				GenAnnotationGrid genGrid = new GenAnnotationGrid(annotTypeNameList, tokType);
				
				List<ProfileGrid> profileGridList = null;
				List<AnnotationSequenceGrid> targetProfileGridList = null;
				
				List<MSAProfile> profileList = null;
				List<MSAProfile> targetProfileList = null;
				Map<AnnotationSequenceGrid, MSAProfile> msaProfileMap = null;
				Map<AnnotationSequenceGrid, MSAProfile> msaTargetProfileMap = null;
			
				
	
				targetProfileGridList = new ArrayList<AnnotationSequenceGrid>();
				List<Double> targetProfileScoreList = new ArrayList<Double>();
				
				Map<String, Double> annotTypeScoreMap = new HashMap<String, Double>();
				for (int i=0; i<annotTypeNameList.size(); i++) {
					String annotType = annotTypeNameList.get(i);
					double score = scoreList.get(i);
					annotTypeScoreMap.put(annotType, score);
				}
				
				
				//init stats
				stats.setMaxGaps(maxGaps);
				stats.setSyntax(syntax);
				stats.setPhrase(phrase);
				stats.setAnnotTypeNameList(annotTypeNameList);
				stats.setScoreList(scoreList);
				stats.setBlockSize(blockSize);
				stats.setWrite(write);
				stats.setDocIDList(docIDList);
				stats.setNegFilterThreshold(negFilterThreshold);
				stats.setNegFilterMinCount(negFilterMinCount);
				stats.setPosFilterThreshold(posFilterThreshold);
				stats.setPosFilterMinCount(posFilterMinCount);
				stats.init(annotUser, annotPassword, msaUser, msaPassword, host, keyspace, msaKeyspace, dbType, indexTable,
					targetType, targetProvenance);
				
				
				//gen new gridlist without targets
				genSent.setRequireTarget(false);
				genSent.genExtractionSequences();
				negSeqList = genSent.getNegSeqList();
				
				List<AnnotationSequenceGrid> gridList = new ArrayList<AnnotationSequenceGrid>();
				for (AnnotationSequence negSeq : negSeqList) {
					List<AnnotationSequenceGrid> negGridList2 = genGrid.toAnnotSeqGrid(negSeq, false, false, true, true);
					gridList.addAll(negGridList2);
				}
				


				
			
				//read profiles
				System.out.println("reading profiles...");
				ProfileReader reader = new ProfileReader();
				reader.setOrder(Order.DSC);
				reader.init(msaUser, msaPassword, host, dbType, msaKeyspace);
				
				//TODO: may not need this anymore
				msaTargetProfileMap = new HashMap<AnnotationSequenceGrid, MSAProfile>();
				
				
				reader.setMinScore(targetMinScore);
				reader.setMaxScore(1.0);
				targetProfileList = reader.read(targetType, targetGroup, 0, Integer.MAX_VALUE, 1, profileTable);
				Map<MSAProfile, Map<MSAProfile, Boolean>> targetFilterMap = reader.readTargetFilters();
				Map<MSAProfile, AnnotationSequenceGrid> targetGridMap = new HashMap<MSAProfile, AnnotationSequenceGrid>();
				Map<Long, AnnotationSequenceGrid> targetIDMap = new HashMap<Long, AnnotationSequenceGrid>();
				
				for (MSAProfile targetProfile : targetProfileList) {
					double score = getProfileScore(targetProfile, annotTypeScoreMap);
					//System.out.println(targetProfile.getProfileStr() + ", " + score);
					AnnotationSequenceGrid profileGrid = genGrid.toAnnotSeqGrid(targetProfile.getToks(), false);
					
					targetGridMap.put(targetProfile, profileGrid);
					
					msaTargetProfileMap.put(profileGrid, targetProfile);
					
					targetIDMap.put(targetProfile.getProfileID(), profileGrid);
					
					boolean inserted = false;
					for (int i=0; i<targetProfileGridList.size(); i++) {
						//AnnotationSequenceGrid profileGrid2 = targetProfileGridList.get(i);
						double score2 = targetProfileScoreList.get(i);
						//if (profileGrid2.size() < profileGrid.size()) {
						if (score > score2) {
							targetProfileGridList.add(i, profileGrid);
							targetProfileScoreList.add(i, score);
							inserted = true;
							break;
						}
					}
					
					if (!inserted) {
						targetProfileGridList.add(profileGrid);
						targetProfileScoreList.add(score);
					}
				}
				
				System.out.println("\n\nTarget Profiles: " + targetProfileGridList.size());
				//pw.println("\n\nTarget Profiles: " + targetProfileGridList.size());
				for (AnnotationSequenceGrid profileGrid2 : targetProfileGridList) {
					String profileStr = gson.toJson(profileGrid2.getSequence().getToks());
					System.out.println(profileStr);
					//pw.println(profileStr);
				}
				
				
				//for (AnnotationSequenceGrid grid : targetProfileGridList)
				//	System.out.println("target profile: " + gson.toJson(grid.getSequence().getToks()));

				
				
				int readStart = 0;
				ProfileInvertedIndex invertedIndex = new ProfileInvertedIndex();

				while (true) {
					profileGridList = new ArrayList<ProfileGrid>();
					//targetProfileGridList = new ArrayList<AnnotationSequenceGrid>();
					//for (String ansAnnotType : ansAnnotTypeList) {
					
					Map<Long, ProfileGrid> profileIDMap = new HashMap<Long, ProfileGrid>();
					
					reader.setMinScore(0.0);
					reader.setMaxScore(0.99);
					profileList = reader.read(targetType, group, readStart, clusterSize, 0, profileTable);
					
					
					
					//check if there are profiles
					if (profileList.size() == 0)
						break;
					
					
					
					msaProfileMap = new HashMap<AnnotationSequenceGrid, MSAProfile>();
					for (MSAProfile msaProfile : profileList) {
						//AnnotationSequenceGrid profileGrid = genGrid.toAnnotSeqGrid(annotTypeProfile.getToks(), false);
						AnnotationSequenceGrid profileGrid = genGrid.toAnnotSeqGrid(msaProfile.getToks(), false);
						msaProfileMap.put(profileGrid, msaProfile);
						
						/*
						List<AnnotationSequenceGrid> targetProfileGridList2 = new ArrayList<AnnotationSequenceGrid>();
						for (AnnotationSequenceGrid targetProfileGrid : targetProfileGridList) {
							targetProfileGridList2.add(targetProfileGrid);
						}
						*/
						
						//get the target filter for this profile
						Map<MSAProfile, Boolean> msaProfileFilterMap = targetFilterMap.get(msaProfile);
						Map<AnnotationSequenceGrid, Boolean> targetFilterMap2 = new HashMap<AnnotationSequenceGrid, Boolean>();
						
						if (msaProfileFilterMap != null) {
							for (MSAProfile msaProfile2 : msaProfileFilterMap.keySet()) {
								AnnotationSequenceGrid grid = targetGridMap.get(msaProfile2);
								targetFilterMap2.put(grid, true);
							}
						}
						
						ProfileGrid profileGridObj = new ProfileGrid(profileGrid, targetFilterMap2);
						profileIDMap.put(msaProfile.getProfileID(), profileGridObj);

						
						boolean inserted = false;
						for (int i=0; i<profileGridList.size(); i++) {
							ProfileGrid profileGridObj2 = profileGridList.get(i);
							if (profileGridObj2.getGrid().size() < profileGrid.size()) {
								profileGridList.add(i, profileGridObj);
								inserted = true;
								break;
							}
						}
						
						if (!inserted)
							profileGridList.add(profileGridObj);
					}
					
					
					System.out.println("\n\nProfiles: " + profileGridList.size());
					//pw.println("\n\nProfiles: " + profileGridList.size());
					for (ProfileGrid profileGridObj : profileGridList) {
						String profileStr = gson.toJson(profileGridObj.getGrid().getSequence().getToks());
						System.out.println(profileStr);
						//pw.println(profileStr);
					}
					
					int prevProfileSize = profileGridList.size();
					
					//stats.setMinFilterNegCount(minFilterNegCount);
					
					//set up inverted index
					System.out.println("gen inverted index...");
					invertedIndex.genIndex(profileGridList, targetProfileGridList, profileIDMap, targetIDMap);
	
					
					stats.getProfileStats(gridList, profileGridList, targetProfileGridList, profileType, msaProfileMap, msaTargetProfileMap, invertedIndex);
					
					int filterProfileSize = profileGridList.size();
					
					System.out.println("prev size: " + prevProfileSize + ", filter size: " + filterProfileSize + ", size: " + profileList.size());
					
					readStart += clusterSize;					
				}
				

				reader.close();
				stats.close();
				
				annotTypeNameList.remove(annotTypeNameList.size()-1);
			}
			
			db.close();
			conn.close();
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	public List<AnnotationSequence> genSentences(List<Long> docIDList, String docNamespace, String docTable, boolean requireTarget, boolean punct, int limit)
	{
		try {
			//gen sentences
			genSent.setRequireTarget(requireTarget);
			genSent.setPunct(punct);
			genSent.init(db, msaAnnotFilterList, targetType, targetProvenance);
			
			if (docIDList == null)
				genSent.genSentences(docNamespace, docTable, null, limit);
			else {
				genSent.setDocIDList(docIDList);
				genSent.genSentenceAnnots(docNamespace, docTable);
			}	
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		
		return genSent.getSeqList();
	}
	
	private List<Long> getDocIDList(String docDBQuery) throws Exception
	{
		//Connection docDBConn = DBConnection.dbConnection(docDBUser, docDBPassword, docDBHost, docDBName, docDBType);
		java.sql.Statement stmt = docDBConn.createStatement();
		java.sql.ResultSet rs = stmt.executeQuery(docDBQuery);
		
		List<Long> docIDList = new ArrayList<Long>();
		
		while (rs.next()) {
			docIDList.add(rs.getLong(1));
		}
		
		stmt.close();
		
		return docIDList;
	}
	
	private List<AnnotationSequenceGrid> getTargetGridList(List<AnnotationSequenceGrid> gridList)
	{
		List<AnnotationSequenceGrid> targetGridList = new ArrayList<AnnotationSequenceGrid>();
		for (AnnotationSequenceGrid grid : gridList) {
			int[] targetCoords = grid.getTargetCoords();
			AnnotationGridElement elem = grid.get(targetCoords[0]).get(targetCoords[1]);
			int start = elem.getStartIndex();
			int end = elem.getEndIndex();
			
			//System.out.println("Grid: " + grid.toString());
			
			
			AnnotationSequenceGrid targetGrid = grid.subGrid(start, end);
			targetGrid.removeElement(targetCoords[0]-start, targetGrid.get(targetCoords[0]-start).size()-1);
			targetGridList.add(targetGrid);
		}
		
		return targetGridList;
	}
	
	private double getProfileScore(MSAProfile profile, Map<String, Double> annotTypeScoreMap)
	{
		double score = 0.0;
		List<String> toks = profile.getToks();
		for (String tok : toks) {
			String[] parts = tok.split("\\!");
			for (int i=0; i<parts.length; i++) {
				int index = parts[i].lastIndexOf("|");
				if (index == -1) {
					System.out.println(profile.getProfileStr());
				}
				
				Double typeScore = annotTypeScoreMap.get(parts[i].substring(0, index));
				if (typeScore != null) {
					score += typeScore;
				}
			}
		}
		
		return score;
	}
	
	public static void main(String[] args)
	{
		if (args.length != 7) {
			System.out.println("usage: user password docuser docpassword msauser msapassword config");
			System.exit(0);
		}
		
		try {
			FilterPatterns ie = new FilterPatterns();
			ie.init(args[6]);
			ie.filterPatterns(args[0], args[1], args[2], args[3], args[4], args[5]);
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
}
