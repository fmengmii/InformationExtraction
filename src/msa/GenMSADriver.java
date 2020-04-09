package msa;

import java.io.FileReader;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.google.gson.Gson;

//import align.AnnotationGridElement;
import align.AnnotationSequenceGrid;
import align.GenAnnotationGrid;

import msa.db.MSADBInterface;
import msa.db.MySQLDBInterface;
import nlputils.sequence.SequenceUtilities;
import utils.db.DBConnection;

public class GenMSADriver
{
	private GenSentences genSent;
	private GenMSA genMSA;
	private GenAnnotationGrid genGrid;
	
	private int maxGaps;
	private List<String> annotTypeNameList;
	private String tokType;
	
	private String docNamespace;
	private String docTable;
	private int limit;
	private String group;
	private String targetGroup;
	
	private String docDBHost;
	private String docDBName;
	private String docDBQuery;
	private String docDBType;
	
	private List<Long> docIDList;
	private int lastDocIndex;
	private boolean requireTarget;
	private boolean punct;
	private boolean write;
	private boolean verbose;
	private MySQLDBInterface db;
	
	private List<Map<String, Object>> msaAnnotFilterList;
	private String msaAnnotFilterStr;
	private String targetType;
	private String targetType2 = null;
	private String targetProvenance;
	
	private int msaBlockSize;
	private List<Double> scoreList;
	private int syntax;
	private int phrase;
	private int msaMinRows;
	private int targetMinRows;

	private String host;
	private String dbName;
	private String dbType;
	//private String keyspace;
	//private String msaKeyspace;
	private String schema;
	
	private String profileTable;
	
	private List<DocBean> totalDocList;
	
	private Gson gson;
	
	private Connection conn;
	
	private boolean incrementalFlag = false;
	
	
	public GenMSADriver()
	{
		gson = new Gson();
		genSent = new GenSentences();
		genMSA = new GenMSA();
	}
	
	public void setDocIDList(List<Long> docIDList)
	{
		this.docIDList = docIDList;
	}
	
	public void setProfileTable(String profileTable)
	{
		this.profileTable = profileTable;
	}
	
	public void setTargetType(String targetType)
	{
		this.targetType = targetType;
	}
	
	public List<DocBean> getDocList()
	{
		return totalDocList;
	}
	
	public int getLastDocIndex()
	{
		return lastDocIndex;
	}
	
	public void setRequireTarget(boolean requireTarget)
	{
		this.requireTarget = requireTarget;
	}
	
	public void setGroup(String group)
	{
		this.group = group;
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
			//this.user = user;
			//this.password = password;
			
			//Properties props = new Properties();
			//props.load(new FileReader(config));
			
			host = props.getProperty("host");
			dbName = props.getProperty("dbName");
			//keyspace = props.getProperty("keyspace");
			//msaKeyspace = props.getProperty("msaKeyspace");
			dbType = props.getProperty("dbType");
			
			docNamespace = props.getProperty("docNamespace");
			docTable = props.getProperty("docTable");
			group = props.getProperty("group");
			targetGroup = props.getProperty("targetGroup");
			schema = props.getProperty("schema");
			
			punct = Boolean.parseBoolean(props.getProperty("punctuation"));
			write = Boolean.parseBoolean(props.getProperty("write"));
			verbose = Boolean.parseBoolean(props.getProperty("verbose"));
			maxGaps = Integer.parseInt(props.getProperty("maxGaps"));
			syntax = Integer.parseInt(props.getProperty("syntax"));
			phrase = Integer.parseInt(props.getProperty("phrase"));
			requireTarget = Boolean.parseBoolean(props.getProperty("requireTarget"));
			targetType = props.getProperty("targetType");
			targetType2 = props.getProperty("targetType2");
			
			if (targetType != null)
				profileTable = props.getProperty("profileTableName");

			
			targetProvenance = props.getProperty("targetProvenance");
			tokType = props.getProperty("tokType");
			
			msaAnnotFilterStr = props.getProperty("msaAnnotFilterList");

			msaMinRows = Integer.parseInt(props.getProperty("msaMinRows"));
			targetMinRows = Integer.parseInt(props.getProperty("targetMinRows"));
			
			limit = -1;
			String limitStr = props.getProperty("limit");
			if (limitStr != null)
				limit = Integer.parseInt(limitStr);
			
			//if (dbType.equals("mysql"))
			db = new MySQLDBInterface();
			db.setDBType(dbType);
			db.setSchema(schema);
				
			
			genSent.setVerbose(verbose);
			genSent.setTokenType(tokType);
			
			
			scoreList = new ArrayList<Double>();
			scoreList = gson.fromJson(props.getProperty("scoreList"), scoreList.getClass());
			
				
			msaBlockSize = Integer.parseInt(props.getProperty("msaBlockSize"));
			
			incrementalFlag = Boolean.parseBoolean(props.getProperty("incrementalFlag"));
			
			
			
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
	
	public void close()
	{
		try {
			//db.close();
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	public void run(String user, String password, String docUser, String docPassword)
	{
		try {
			msaAnnotFilterList = new ArrayList<Map<String, Object>>();
			msaAnnotFilterList = gson.fromJson(msaAnnotFilterStr, msaAnnotFilterList.getClass());
			
			Map<String, Object> targetMap = new HashMap<String, Object>();
			targetMap.put("annotType", targetType);
			targetMap.put("target", true);
			targetMap.put("provenance", targetProvenance);
			targetMap.put("targetStr", ":target");
			msaAnnotFilterList.add(targetMap);
			
			if (targetType2 != null) {
				targetMap = new HashMap<String, Object>();
				targetMap.put("annotType", targetType2);
				targetMap.put("target", true);
				targetMap.put("provenance", targetProvenance);
				targetMap.put("targetStr", ":target2");
				msaAnnotFilterList.add(targetMap);
			}
			
			
			annotTypeNameList = MSAUtils.getAnnotationTypeNameList(msaAnnotFilterList, tokType, scoreList);
			annotTypeNameList.add(":" + targetType.toLowerCase());
			scoreList.add(10.0);
			
			if (docDBQuery != null) {			
				conn = DBConnection.dbConnection(user, password, host, dbName, dbType);	
				//docIDList = getDocIDList(docDBQuery);
				//docIDList = MSAUtils.getDocIDList(docDBConn, docDBQuery);
				
				PreparedStatement pstmtGetDocIDs = conn.prepareStatement(docDBQuery);
				//pstmtGetDocIDs.setString(1, targetType);
				
				docIDList = new ArrayList<Long>();
				ResultSet rs = pstmtGetDocIDs.executeQuery();
				while (rs.next()) {
					docIDList.add(rs.getLong(1));
				}
				
				conn.close();
			}
			
			/*
			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery("select max(cluster) from " + schema + "document_status");
			int maxCluster = -1;
			if (rs.next())
				maxCluster = rs.getInt(1) + 1;
			*/
			
			db.init(user, password, host, dbName, dbName);
			
			//generate the sentences
			System.out.println("requireTarget: " + requireTarget + " targetType: " + targetType);
			
			genSent.setRequireTarget(requireTarget);
			genSent.setPunct(punct);
			genSent.init(db, msaAnnotFilterList, targetType, targetProvenance);
			
			if (docIDList == null)
				genSent.genSentences(docNamespace, docTable, null, limit);
			else {
				genSent.setDocIDList(docIDList);
				genSent.genSentenceAnnots(docNamespace, docTable);
			}	
			
			List<AnnotationSequence> posSeqList = genSent.getPosSeqList();
			List<AnnotationSequence> negSeqList = genSent.getNegSeqList();
			List<AnnotationSequence> seqList = posSeqList;
			
			if (!requireTarget)
				seqList = negSeqList;
			
			
			
			//loop generating blocks of grids no more than size msaBlockSize
			
			int numBlocks = (seqList.size() / msaBlockSize) + 1;
			int currStartIndex = 0;
			
			System.out.println("msaBlockSize: " + msaBlockSize + " numBlocks: " + numBlocks + " seqList size: " + seqList.size());

			
			totalDocList = new ArrayList<DocBean>();
			List<DocBean> docList = new ArrayList<DocBean>();
			
			int cluster = 0;

			for (int blockNum=0; blockNum<numBlocks; blockNum++) {
				System.out.println("blockNum: " + blockNum);
				System.out.println("currStartIndex: " + currStartIndex);
				System.out.println("cluster: " + cluster);

				//remove the last docID because it may not have been fully used
				/*
				if (docList.size() > 0)
					docList.remove(docList.size()-1);
				
				if (blockNum > 0) {
					totalDocList.addAll(docList);
				}
				*/
				
				//totalDocList.addAll(docList);
				
				docList = new ArrayList<DocBean>();

				
				List<AnnotationSequence> seqList2 = new ArrayList<AnnotationSequence>();
				Map<Long, Boolean> docIDMap = new HashMap<Long, Boolean>();
				long lastDocID = -1;
				for (int i=currStartIndex; i<seqList.size(); i++) {
					long docID = seqList.get(i).getDocID();
					
					seqList2.add(seqList.get(i));

					if (seqList2.size() > msaBlockSize && docID != lastDocID) {
						for (DocBean docBean : docList) {
							docBean.setCluster(cluster);
						}
						break;
					}

					Boolean flag = docIDMap.get(docID);
					if (flag == null) {
						docIDMap.put(docID,  true);
						DocBean docBean = new DocBean();
						docBean.setDocID(seqList.get(i).getDocID());
						docList.add(docBean);
					}
					
					//set the lastDocID to be the doc when seqList2's size is at msaBlockSize
					//this prevents the blocks from dividing up a document
					if (seqList2.size() == msaBlockSize)
						lastDocID = docID;
				}
				
				System.out.println("pos seq list2 size: " + seqList2.size());
				
				currStartIndex += seqList2.size();
				
				
				GenAnnotationGrid genGrid = new GenAnnotationGrid(annotTypeNameList, tokType);
				
				List<ProfileGrid> profileGridList = null;
				List<AnnotationSequenceGrid> targetProfileGridList = null;
				
				List<MSAProfile> profileList = null;
				List<MSAProfile> targetProfileList = null;
				Map<AnnotationSequenceGrid, MSAProfile> msaProfileMap = null;
				Map<AnnotationSequenceGrid, MSAProfile> msaTargetProfileMap = null;
			
			
				//profile MSAs
				genMSA.setMaxGaps(maxGaps);
				genMSA.setMinSize(2);
				genMSA.setMsaMinRows(msaMinRows);
				genMSA.setAnnotTypeNameList(annotTypeNameList);
				genMSA.setVerbose(false);
				genMSA.setTokType(tokType);
				genMSA.setRequireTarget(requireTarget);
				genMSA.setMatchSize(false);
				genMSA.setGenGrid(genGrid);
				genMSA.setSeqList(seqList2);
				genMSA.setScoreList(scoreList);
				genMSA.setSyntax(syntax);
				genMSA.setPhrase(phrase);
				genMSA.setGridList(null);
				
							
				List<MultipleSequenceAlignment> msaList = genMSA.genMSA();
				List<AnnotationSequenceGrid> gridList = genMSA.getGridList();				
				
				Map<String, Boolean> msaMap = new HashMap<String, Boolean>();
				for (MultipleSequenceAlignment msa : msaList) {
					msaMap.put(msa.toProfileString(false), true);
				}
				
				
				//gen MSAs with answers
				
				if (requireTarget) {
					List<AnnotationSequenceGrid> gridListAnswers = new ArrayList<AnnotationSequenceGrid>();
					for (AnnotationSequence seq : seqList2) {
						List<AnnotationSequenceGrid> gridList2 = genGrid.toAnnotSeqGrid(seq, true, true, true, false, false);
						gridListAnswers.addAll(gridList2);
					}
					
					
					genMSA.setGridList(gridListAnswers);
					List<MultipleSequenceAlignment> ansMSAList = genMSA.genMSA();
					
					for (MultipleSequenceAlignment msa : ansMSAList) {
						if (msaMap.get(msa.toProfileString(false)) == null)
							msaList.add(msa);
					}
					
					ansMSAList = null;
				}
				
				
				//relation MSAs
				//get pairs of annotations
				
				
				for (AnnotationSequence seq : seqList) {
					
					List<AnnotationSequenceGrid> gridList2 = genGrid.toAnnotSeqGrid(seq, true, true, true, false, false);
					gridListAnswers.addAll(gridList2);
				}
				
				msaMap = null;

					
				
				
				//get grids from profile MSAs
				System.out.println("\n\nProfiles");
	
				List<AnnotationSequenceGrid> profileSeqGridList = new ArrayList<AnnotationSequenceGrid>();
				List<Integer> msaRowList = new ArrayList<Integer>();
				for (MultipleSequenceAlignment msa : msaList) {
					//MSAProfile profile = new MSAProfile(msa.getID(), msa.toProfileString(false));
					//profileList.add(profile);
					
					List<String> profileToks = SequenceUtilities.getToksFromStr(msa.toProfileString(false));				
	
					
					System.out.println("profile: " + SequenceUtilities.getStrFromToks(profileToks));
					
					/*
					if (msa.getTotalRows() < msaMinRows) {
						System.out.println("Filtered! rows: " + msa.getTotalRows());
						continue;
					}
					*/
					
					AnnotationSequenceGrid profileGrid = genGrid.toAnnotSeqGrid(profileToks, false);
					
					if (requireTarget) {
						int targetTokIndex = profileToks.indexOf(":target");
						if (targetTokIndex == -1)
							System.out.println("here");
						
						int[] targetCoords = new int[2];
						targetCoords[0] = targetTokIndex;
						targetCoords[1] = profileGrid.get(targetTokIndex).size()-1;
						profileGrid.setTargetCoords(targetCoords);
					}
					
					
					profileSeqGridList.add(profileGrid);
					msaRowList.add(msa.getTotalRows());
					
					//System.out.println("profile: " + SequenceUtilities.getStrFromToks(profileToks));
				}
				
				System.out.println("profile total = " + profileSeqGridList.size());
					
					
				//target MSAs
					
				if (requireTarget) {
					System.out.println("\n\nTarget Profiles");
					List<AnnotationSequenceGrid> targetGridList = MSAUtils.getTargetGridList(gridList);
					
					
					//print target grids 
					for (AnnotationSequenceGrid targetGrid : targetGridList) {
						//targetGrid.removeRow(targetGrid.get);
						System.out.println(targetGrid.toString());
					}
								
	
					genMSA.setMinSize(1);
					genMSA.setRequireTarget(false);
					genMSA.setMatchSize(true);
					genMSA.setGridList(targetGridList);
					genMSA.setMsaMinRows(targetMinRows);
					List<MultipleSequenceAlignment> targetMSAList = genMSA.genMSA();
					
					//get grids from target MSAs
					targetProfileGridList = new ArrayList<AnnotationSequenceGrid>();
					for (MultipleSequenceAlignment msa : targetMSAList) {
						List<String> profileToks = SequenceUtilities.getToksFromStr(msa.toProfileString(false));				
						AnnotationSequenceGrid profileGrid = genGrid.toAnnotSeqGrid(profileToks, false);
						
						targetProfileGridList.add(profileGrid);
						
						//System.out.println("target profile: " + SequenceUtilities.getStrFromToks(profileToks));
					}
				
				}
				
				
				//generate MSAProfile objects
				profileList = new ArrayList<MSAProfile>();
				targetProfileList = new ArrayList<MSAProfile>();
	
				
				for (int i=0; i<profileSeqGridList.size(); i++) {
					AnnotationSequenceGrid grid = profileSeqGridList.get(i);
					int rows = msaRowList.get(i);
					List<String> toks = grid.getSequence().getToks();
					String profileGridStr = gson.toJson(toks);
					MSAProfile profile = new MSAProfile(profileGridStr, targetType, group, 0, toks, 0.0, rows);
					profileList.add(profile);
				}
				
				
				if (requireTarget) {
					for (AnnotationSequenceGrid grid : targetProfileGridList) {
						List<String> toks = grid.getSequence().getToks();
						String profileGridStr = gson.toJson(toks);
						MSAProfile profile = new MSAProfile(profileGridStr, targetType, targetGroup, 1, toks, 1.0, 0);
						targetProfileList.add(profile);
					}
				}
				
				//Add empty profile in case a target is self-sufficient and doesn't need outside context
				List<String> toks = new ArrayList<String>();
				MSAProfile emptyProfile = new MSAProfile("[\":start|start\",\":target\",\":end|end\"]", targetType, group, 0, toks, 0.0, 1);
				profileList.add(emptyProfile);
				
				//write profiles
				if (write) {
					ProfileWriter writer = new ProfileWriter();
					writer.setMsaTable(schema + "." + profileTable);
					writer.init(user, password, host, dbType, dbName);
				
					System.out.println("writing... ");
					
					writer.write(profileList, false);
					writer.write(targetProfileList, false);
					
					System.out.println("wrote " + profileList.size() + " patterns");
					System.out.println("wrote " + targetProfileList.size() + " targets");
					writer.close();
				}
				
				cluster++;

				totalDocList.addAll(docList);
				lastDocIndex = totalDocList.size();
			}
			
			
			genMSA.close();
			db.close();
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args)
	{
		if (args.length != 5) {
			System.out.println("usage: user password docUser docPassword config");
			System.exit(0);
		}
		
		GenMSADriver genMSA = new GenMSADriver();
		genMSA.init(args[4]);
		genMSA.run(args[0], args[1], args[2], args[3]);
	}
}
