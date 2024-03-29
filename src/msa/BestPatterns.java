package msa;

import java.sql.*;
import java.util.*;

import com.google.gson.Gson;

import java.io.*;

import utils.db.DBConnection;

public class BestPatterns
{
	private Connection conn;
	private Connection annotConn;
	private PreparedStatement pstmt;
	private List<Long> docIDList;
	private Map<String, String> ansMap;
	
	private String host;
	private String dbName;
	//private String msaKeyspace;
	private String dbType;
	private String annotType;
	//private String annotKeyspace;
	private String finalTable;
	private String indexTable;
	private String profileTable;
	private String provenance;
	private double negThreshold;
	private int negMinCount;
	private double posThreshold;
	private int posMinCount;
	private boolean cleanTables = false;
	private String docQuery;
	private String group;
	private String annotTable;
	private String ansType;
	
	private Boolean negAnsFlag;
	
	private List<String> groupList;
	private List<String> targetGroupList;
	
	private List<String> annotTypeList;
	private List<String> profileTableList;
	private List<String> indexTableList;
	private List<String> finalTableList;
	
	private Map<String, Integer> posMap;
	private Map<String, Integer> negMap;
	private Map<String, Boolean> inactiveMap;
	private Map<String, Boolean> preloadMap;
	

	private int projID = -1;
	
	private boolean write;
	
	
	private String rq;
	
	private String schema = "";
	
	private boolean filterFlag;
	private Map<String, Boolean> profileFilterMap;
	
	private int minToks;
	private int maxToks;
	
	private Gson gson;

	
	
	public BestPatterns()
	{
		gson = new Gson();
	}
	
	public void init(Properties props) throws ClassNotFoundException, SQLException
	{
		
		host = props.getProperty("host");
		dbName = props.getProperty("dbName");
		//msaKeyspace = props.getProperty("msaKeyspace");
		dbType = props.getProperty("dbType");
		annotType = props.getProperty("annotType");
		//annotKeyspace = props.getProperty("annotKeyspace");
		finalTable = props.getProperty("finalTable");
		indexTable = props.getProperty("indexTable");
		profileTable = props.getProperty("profileTable");
		provenance = props.getProperty("provenance");
		negThreshold = Double.parseDouble(props.getProperty("negThreshold"));
		negMinCount = Integer.parseInt(props.getProperty("negMinCount"));
		posThreshold = Double.parseDouble(props.getProperty("posThreshold"));
		posMinCount = Integer.parseInt(props.getProperty("posMinCount"));
		schema = props.getProperty("schema") + ".";
		annotTable = props.getProperty("annotTable");
		ansType = props.getProperty("ansType");
		String negAnsFlagStr = props.getProperty("negAnsFlag");
		if (negAnsFlagStr != null)
			negAnsFlag = Boolean.parseBoolean(negAnsFlagStr);
		
		
		String projIDStr = props.getProperty("projID");
		if (projIDStr != null)
			projID = Integer.parseInt(projIDStr);
		
		
		String groupListStr = props.getProperty("groupList");
		groupList = new ArrayList<String>();
		groupList = gson.fromJson(groupListStr, groupList.getClass());
		
		String targetGroupListStr = props.getProperty("targetGroupListStr");
		targetGroupList = new ArrayList<String>();
		targetGroupList = gson.fromJson(targetGroupListStr, targetGroupList.getClass());
		
		String cleanTablesStr = props.getProperty("cleanTables");
		if (cleanTablesStr != null)
			cleanTables = Boolean.parseBoolean(cleanTablesStr);
		
		filterFlag = false;
		String filterFlagStr = props.getProperty("filterFlag");
		if (filterFlagStr != null)
			filterFlag = Boolean.parseBoolean(filterFlagStr);
		
		docQuery = props.getProperty("docQuery");
		group = props.getProperty("group");
		
		String writeStr = props.getProperty("write");
		if (writeStr != null)
			write = Boolean.parseBoolean(writeStr);
		
		
		minToks = Integer.parseInt(props.getProperty("minToks"));
		maxToks = Integer.parseInt(props.getProperty("maxToks"));
		
		

	}
	
	public void close() throws SQLException
	{
		conn.close();
		annotConn.close();
	}
	
	public List<String> getAnnotTypeList() {
		return annotTypeList;
	}

	public void setAnnotTypeList(List<String> annotTypeList) {
		this.annotTypeList = annotTypeList;
	}

	public List<String> getProfileTableList() {
		return profileTableList;
	}

	public void setProfileTableList(List<String> profileTableList) {
		this.profileTableList = profileTableList;
	}

	public List<String> getIndexTableList() {
		return indexTableList;
	}

	public void setIndexTableList(List<String> indexTableList) {
		this.indexTableList = indexTableList;
	}

	public List<String> getFinalTableList() {
		return finalTableList;
	}

	public void setFinalTableList(List<String> finalTableList) {
		this.finalTableList = finalTableList;
	}
	
	public void setFilterFlag(boolean filterFlag)
	{
		this.filterFlag = filterFlag;
	}
	
	public void getBestPatterns(String msaUser, String msaPassword, String annotUser, String annotPassword)
	{
		try {
			
			Map<String, Boolean> profileTargetMap = new HashMap<String, Boolean>();
			
			//conn = DBConnection.dbConnection(msaUser, msaPassword, host, dbName, dbType);
			//annotConn = DBConnection.dbConnection(annotUser, annotPassword, host, dbName, dbType);
			conn = DBConnection.dbConnection(msaUser, msaPassword, host, dbName, dbType);
			annotConn = DBConnection.dbConnection(annotUser, annotPassword, host, dbName, dbType);
			rq = DBConnection.reservedQuote;
			
			
			if (annotType != null) {
				annotTypeList = new ArrayList<String>();
				annotTypeList.add(annotType);
				
				profileTableList = new ArrayList<String>();
				profileTableList.add(profileTable);
				
				indexTableList = new ArrayList<String>();
				indexTableList.add(indexTable);
				
				finalTableList = new ArrayList<String>();
				finalTableList.add(finalTable);
			}
			
			

			
			Map<String, Boolean> finalTableMap = new HashMap<String, Boolean>();
			for (int index=0; index<annotTypeList.size(); index++) {
				String annotType = annotTypeList.get(index);
				profileTable = profileTableList.get(index);
				indexTable = indexTableList.get(index);
				finalTable = finalTableList.get(index);
				
				
				System.out.println("best annot type: " + annotType);
				
				
				PreparedStatement pstmt = conn.prepareStatement("insert into " + schema + finalTable + " (profile_id, target_id, total, prec, true_pos, false_pos, disabled) values (?,?,?,?,?,?,?)");
				PreparedStatement pstmtUpdateFinal = conn.prepareStatement("update " + schema + finalTable + " set true_pos = ?, false_pos = ?, total = ? where profile_id = ? and target_id = ?");
				PreparedStatement pstmtUpdateProfile = conn.prepareStatement("update " + schema + profileTable + " set score = ? where profile_id = ?");
				PreparedStatement pstmtUpdateProfileCounts = conn.prepareStatement("update " + schema + profileTable + " set true_pos = ?, false_pos = ? where profile_id = ?");
				
				/*
				PreparedStatement pstmtGetIndexCounts = conn.prepareStatement("select a.profile_id, a.target_id, a.start, a." + rq + "end" + rq + ", b.profile_type, b.profile, count(*) "
					+ "from " + schema + rq + indexTable + rq + " a, " + schema + profileTable + " b "
					+ "where b.annotation_type = '" + annotType + "' and a.profile_id = b.profile_id and a.document_id = ? "
					+ "group by a.profile_id, a.target_id, a.start, a." + rq + "end" + rq + ", b.profile_type, b.profile");
					*/
				
				PreparedStatement pstmtGetIndexCounts = conn.prepareStatement("select a.profile_id, a.target_id, a.start, a." + rq + "end" + rq + ", b.profile_type, b.profile "
						+ "from " + schema + rq + indexTable + rq + " a, " + schema + profileTable + " b "
						+ "where b.annotation_type = '" + annotType + "' and a.profile_id = b.profile_id and a.document_id = ?");
				
				
				PreparedStatement pstmtGetIndexCounts2 = conn.prepareStatement("select a.profile_id, a.target_id, a.start, a." + rq + "end" + rq + ", b.profile_type, count(*) "
						+ "from " + schema + rq + indexTable + rq + " a, " + schema + profileTable + " b "
						+ "where b.annotation_type = '" + annotType + "' and a.profile_id = b.profile_id and a.document_id = ? and b." + rq + "group" + rq + " = '" + group + "' "
						+ "group by a.profile_id, a.target_id, a.start, a." + rq + "end" + rq);
				
				PreparedStatement pstmtSetProfileDisabled = conn.prepareStatement("update " + schema + "profile set score = -1.0 where profile_id = ?");
				
				Statement stmt = conn.createStatement();
				
				//delete from final table for this annot type
				//stmt.execute("delete from " + schema + "final where profile_id in ("
				//	+ "select a.profile_id from " + schema + "profile a where a.annotation_type = '" + annotType + "')");
				

			
				System.out.println("getting doc IDs...");
				docIDList = new ArrayList<Long>();
				//ResultSet rs = stmt.executeQuery("select distinct document_id from " + rq + indexTable + rq + " order by document_id");
				if (docQuery == null) {
					if (projID == -1) {
						docQuery = "select document_id from " + schema + "document_status where status = 1 or status = 2 order by document_id";
					}
					else {
						docQuery = "select document_id from " + schema + "document_status" + " where (status = 1 or status = 2) "
							+ "and document_id in "
							+ "(select b.document_id from " + schema + "project_frame_instance a, " + schema + "frame_instance_document b "
							+ "where a.frame_instance_id = b.frame_instance_id and a.project_id = " + projID + ") "
							+ "order by document_id";
					}
				}
				
				List<Integer> statusList = new ArrayList<Integer>();
				ResultSet rs = stmt.executeQuery(docQuery);
				while (rs.next()) {
					docIDList.add(rs.getLong(1));
					//statusList.add(rs.getInt(2));
				}
				
				
				System.out.println("reading answers...");
				ansMap = new HashMap<String, String>();
				for (long docID : docIDList)
					readAnswers(docID, ansType, provenance);
				
				System.out.println("ansMap size: " + ansMap.size());
				
					//+ " where profile_id in "
					//+ "(select distinct profile_id from msa_profile where annotation_type = '" + annotType + "')");
				//stmt.execute("delete from msa_profile_final where annotation_type = '" + annotType + "' and `group` = '" + group + "'");
				
				//map targetIDs
				/*
				ResultSet rs = stmt.executeQuery("select distinct profile_id, profile from msa_profile where profile_type = 1 order by profile_id");
				while (rs.next()) {
					long targetID = rs.getLong(1);
					String profileStr = rs.getString(2);
	
					Long targetID2 = targetStrMap.get(profileStr);
					if (targetID2 == null) {
						targetStrMap.put(profileStr, targetID);
						targetIDMap.put(targetID, targetID);
					}
					else
						targetIDMap.put(targetID, targetID2);
				}
				*/
				
				System.out.println("retrieving profile matches...");
				//ResultSet rs = stmt.executeQuery("select distinct a.profile_id, a.target_id, a.document_id, a.start, a.end, b.profile, c.profile from msa_profile_match_index a, msa_profile b, msa_profile c "
				//	+ "where b.annotation_type = '" + annotType + "' and a.profile_id = b.profile_id and a.target_id = c.profile_id "
				//	+ "order by a.profile_id");
				
				
				
				posMap = new HashMap<String, Integer>();
				negMap = new HashMap<String, Integer>();
				
				//read all profiles if negAnsFlag is set
				if (negAnsFlag)
					readProfiles();
				

				Map<String, Integer> docCountMap = new HashMap<String, Integer>();
				inactiveMap = new HashMap<String, Boolean>();
				String targetStr = "";
				
				System.out.println("preloading counts...");
				preloadCounts();
				System.out.println("finished preloading counts...");
				
				
				
				long currProfileID = -1;
				
				Map<Long, List<Long>> docMap = new HashMap<Long, List<Long>>();
				Map<Long, List<Long>> invDocMap = new HashMap<Long, List<Long>>();
				Map<Long, Integer> profileTotals = new HashMap<Long, Integer>();
				
				//rs = stmt.executeQuery("select distinct a.profile_id, a.target_id, a.document_id, a.start, a." + rq + "end" + rq + " from " + schema + rq + indexTable + rq + " a, " + schema + profileTable + " b "
				//	+ "where b.annotation_type = '" + annotType + "' and a.profile_id = b.profile_id");
				
				Map<String, Integer> profileTypeMap = new HashMap<String, Integer>();
				
				for (int i=0; i<docIDList.size(); i++) {
					long docID = docIDList.get(i);
					//int status = statusList.get(i);
					
					System.out.println("best docID: " + docID);
					
					//if (status == 1) {
						pstmtGetIndexCounts.setLong(1, docID);
						//System.out.println(pstmtGetIndexCounts.toString());
						rs = pstmtGetIndexCounts.executeQuery();
					//}
					//else {
						//pstmtGetIndexCounts2.setLong(1, docID);
						
						//rs = pstmtGetIndexCounts2.executeQuery();
					//}

					while (rs.next()) {
						/*
						long profileID = rs.getLong(1);
						Long targetID = rs.getLong(2);
						long docID = rs.getLong(3);
						long start = rs.getLong(4);
						long end = rs.getLong(5);
						String profileStr = rs.getString(6);
						String targetProfileStr = rs.getString(7);
						*/
						
						long profileID = rs.getLong(1);
						long targetID = rs.getLong(2);
						long start = rs.getLong(3);
						long end = rs.getLong(4);
						int profileType = rs.getInt(5);
						//int matchCount = rs.getInt(6);
						String profileStr = rs.getString(6);
						
						List<String> tokList = new ArrayList<String>();
						tokList = gson.fromJson(profileStr, tokList.getClass());
						
						int tokCount = 0;
						for (String tok : tokList) {
							if (tok.equals(":start") || tok.equals(":end") || tok.equals(":target"))
								continue;
							
							
							if (tok.indexOf("|string") > 0 || tok.indexOf("|root") > 0)
								tokCount++;
						}
						
						if (tokCount < minToks || tokCount > maxToks)
							continue;
						
						//System.out.println("profileID: " + profileID + " targetID: " + targetID + " start:" + start + " profileType: " + profileType + " matchCount: " + matchCount);
						
						
						/*
						if (matchCount > 1)
							continue;
						*/
						
						
						/*
						int index = profileStr.indexOf(":target");
						if (index < 0) {
							System.out.println("ERROR: " + profileID + ", " + profileStr);
						}
						*/
						
						
						
			
						String key = profileID + "|" + targetID;
						if (inactiveMap.get(key) != null)
							continue;
						
						String docKey = profileID + "|" + targetID + "|" + docID;
						String ansProv = ansMap.get(docID + "|" + start + "|" + end);
						
						System.out.println(docID + "|" + start + "|" + end + "|" + profileID + "|" + targetID);
						
						
						if (profileType == 3) {
							profileTypeMap.put(key, profileType);
							System.out.println("profile type 3: " + key);
						}
						
						
						long profileID2 = profileID;
						if (ansProv != null && !ansProv.equals("validation-tool-unlabeled")) {
							
							boolean ansFlag = true;
							boolean inc = true;
							Integer docCount = docCountMap.get(docKey + "|" + ansFlag);
							if (docCount == null)
								docCount = 0;
							if (docCount == 50)
								inc = false;
							else 
								docCountMap.put(docKey + "|" + ansFlag, ++docCount);
							
							
							if (inc) {
								Integer count = posMap.get(key);
								if (count == null)
									count = 0;
								
								posMap.put(key, ++count);
							}
							
							Integer profileCount = profileTotals.get(profileID);
							if (profileCount == null)
								profileCount = 0;
							profileTotals.put(profileID, ++profileCount);
							
							
						}
						else if (ansProv == null) {							
							boolean inc = true;
							boolean ansFlag = false;
							Integer docCount = docCountMap.get(docKey + "|" + ansFlag);
							if (docCount == null)
								docCount = 0;
							if (docCount == 50)
								inc = false;
							else 
								docCountMap.put(docKey + "|" + ansFlag, ++docCount);
							
							if (inc) {
								Integer count = negMap.get(key);
								if (count == null)
									count = 0;
								
								negMap.put(key, ++count);
							}
							
							profileID2 = -profileID;
							
						}
						
						
						
						//profile conditionals
						List<Long> docList = docMap.get(profileID2);
						if (docList == null) {
							docList = new ArrayList<Long>();
							docMap.put(profileID2, docList);
						}
						
						docList.add(docID);
						
						List<Long> profileList = invDocMap.get(docID);
						if (profileList == null) {
							profileList = new ArrayList<Long>();
							invDocMap.put(docID, profileList);
						}
						
						profileList.add(profileID);
						
						
						
						
					}
				}
				
				
				conn.setAutoCommit(false);
				int count = 0;
				Map<Long, Boolean> profileActiveMap = new HashMap<Long, Boolean>();
				
				for (String key : posMap.keySet()) {
					int posCount = posMap.get(key);
					Integer negCount = negMap.get(key);
					String[] parts = key.split("\\|");
					Long profileID = Long.parseLong(parts[0]);
					long targetID = Long.parseLong(parts[1]);
					
					if (negCount == null)
						negCount = 0;
					
					//subtract one from negcount to correct for human errors
					//or unannotated retrospective cases (status 1)
					
					if (negCount > 0)
						negCount--;
					
					double prec = ((double) posCount) / ((double) (posCount + negCount));
					
					if (negAnsFlag)
						prec = 1.0 - prec;
						
					
						
	
					//boolean write = true;
					int valence = 0;
					
					double score = 0.0;
					
					if (prec >= posThreshold && (posCount + negCount) >= posMinCount) {
						//write = true;
						score = 1.0;
					}
					
					if (prec < negThreshold && (posCount + negCount) >= negMinCount) {
						score = 1.0;
					}
					
					
					
					if (profileTypeMap.get(key) != null) {
						posCount = posMinCount + 1;
						negCount = 0;
						prec = 1.0;
					}
					
					Boolean active = profileActiveMap.get(profileID);
					if (prec > 0.8)
						profileActiveMap.put(profileID, true);
					else if (prec <= 0.8 && active == null && (posCount + negCount) >= negMinCount)
						profileActiveMap.put(profileID, false);
					
					
					
					System.out.println(key + " : " + posCount + ", " + negCount + ", " + prec + ", " + score);
					
					//pstmtUpdateProfile.setDouble(1, score);					
					//pstmtUpdateProfile.setLong(2, profileID);
					//pstmtUpdateProfile.execute();
					
					if (write) {
						
						if (preloadMap.get(key) == null) {
							int disabled = 1;
							if (score == 1.0)
								disabled = 0;
							
							pstmt.setLong(1, profileID);
							pstmt.setLong(2, targetID);
							pstmt.setInt(3, posCount+negCount);
							pstmt.setDouble(4, prec);
							//pstmt.setInt(5, valence);
							pstmt.setInt(5, posCount);
							pstmt.setInt(6, negCount);
							pstmt.setInt(7, disabled);
							pstmt.addBatch();
							
						}
						else {
							pstmtUpdateFinal.setInt(1, posCount);
							pstmtUpdateFinal.setInt(2, negCount);
							pstmtUpdateFinal.setInt(3, posCount + negCount);
							pstmtUpdateFinal.setLong(4, profileID);
							pstmtUpdateFinal.setLong(5, targetID);
							pstmtUpdateFinal.addBatch();
						}
						
						count++;
						if (count % 1000 == 0) {
							pstmt.executeBatch();
							conn.commit();
						}
					}
				
				}
				
				//profile has no target combo that exceeds 80%
				for (long profileID : profileActiveMap.keySet()) {
					Boolean active = profileActiveMap.get(profileID);
					if (!active) {
						if (write) {
							pstmtSetProfileDisabled.setLong(1, profileID);
							pstmtSetProfileDisabled.execute();
						}
					}
				}
				
				
				pstmt.executeBatch();
				conn.commit();
				
				stmt.close();
				pstmt.close();
				pstmtUpdateProfile.close();
			
		
				if (filterFlag) {
					System.out.println("filter overlap...");
					filterOverlapping(annotType);
					
					/*
					int count = 0;
					PreparedStatement pstmt = conn.prepareStatement("update " + schema + finalTable + " set prec = -1.0, true_pos = 0, false_pos = 0, total = 0 where profile_id = ? and target_id = ?");
					for (String key : posMap.keySet()) {
						Boolean flag = profileFilterMap.get(key);
						if (flag == null) {
							String[] parts = key.split("\\|");
							int profileID = Integer.parseInt(parts[0]);
							int targetID = Integer.parseInt(parts[1]);
							
							System.out.println("filtered: " + profileID + "|" + targetID);
							
							pstmt.setInt(1, profileID);
							pstmt.setInt(2, targetID);
							pstmt.addBatch();
							//conn.commit();
							
							count++;
							if (count % 1000 == 0) {
								pstmt.executeBatch();
								conn.commit();
							}
						}
					}
					
					pstmt.executeBatch();
					conn.commit();
					*/
				}

			
			//cleanIndexTable();
			}
			
			
			conn.close();
			annotConn.close();
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	public void filterOverlapping(String msaUser, String msaPassword, String annotUser, String annotPassword) throws SQLException, ClassNotFoundException
	{
		conn = DBConnection.dbConnection(msaUser, msaPassword, host, dbName, dbType);
		annotConn = DBConnection.dbConnection(annotUser, annotPassword, host, dbName, dbType);
		rq = DBConnection.reservedQuote;
		
		filterOverlapping(annotType);
	}
	
	public void filterOverlapping(String annotType) throws SQLException, ClassNotFoundException
	{
		conn.setAutoCommit(false);
		
		if (posMap == null) {
			posMap = new HashMap<String, Integer>();
			negMap = new HashMap<String, Integer>();
			inactiveMap = new HashMap<String, Boolean>();
		}
		
		//preloadCounts();
		
		System.out.println("get profile lengths...");
		Map<Integer, Integer> profileLengthMap = new HashMap<Integer, Integer>();
		Map<Integer, String> profileStringMap = new HashMap<Integer, String>();
		Map<Integer, Integer> profileSkewMap = new HashMap<Integer, Integer>();
		Map<Integer, Integer> profileTypeMap = new HashMap<Integer, Integer>();
		Statement stmt = conn.createStatement();
		
		
		if (docIDList == null) {
			System.out.println("getting doc IDs...");
			docIDList = new ArrayList<Long>();
			//ResultSet rs = stmt.executeQuery("select distinct document_id from " + rq + indexTable + rq + " order by document_id");
			if (docQuery == null) {
				docQuery = "select document_id, status from " + schema + "document_status" + " where status = 1 or status = 2 order by document_id";
			}
			
			//statusList = new ArrayList<Integer>();
			ResultSet rs = stmt.executeQuery(docQuery);
			while (rs.next()) {
				docIDList.add(rs.getLong(1));
				//statusList.add(rs.getInt(2));
			}
		}
		
		
		if (ansMap == null) {
			System.out.println("reading answers...");
			ansMap = new HashMap<String, String>();
			for (long docID : docIDList)
				readAnswers(docID, annotType, provenance);
		}
		
		
		int maxTargetLen = 0;
		
		ResultSet rs = stmt.executeQuery("select distinct b.profile_id, b.profile, b.profile_type from " + schema + finalTable + " a, " + schema + profileTable + " b where "
			+ "a.profile_id = b.profile_id or a.target_id = b.profile_id");
		while (rs.next()) {
			int profileID = rs.getInt(1);
			String profileStr = rs.getString(2);
			int profileType = rs.getInt(3);
			
			List<String> profileToks = new ArrayList<String>();
			
			profileStr = profileStr.replaceAll("\"\"", "\\\\\"\"");
			profileToks = gson.fromJson(profileStr, profileToks.getClass());
			int len = profileToks.size();
			profileLengthMap.put(profileID, len);
			
			if (profileStr.indexOf(":" + annotType.toLowerCase()) >= 0)
				profileTypeMap.put(profileID, 1);
			else profileTypeMap.put(profileID, 0);
			
			if (profileType == 1 && len > maxTargetLen)
				maxTargetLen = len;
			
			profileStringMap.put(profileID, profileStr);
			
			/*
			int index = profileStr.indexOf("[\":target");
			int skew = 0;
			if (profileStr.endsWith(":target\"]"))
				skew = 2;
			else if (profileStr.startsWith("[\":target"))
				skew = 1;
			
			profileSkewMap.put(profileID, skew);
			*/
			
			
		}

		
		//System.out.println("original size: " + profileLengthMap.size());
		
		System.out.println("get profile frequencies...");
		double maxFreq = 0.0;
		Map<Integer, Double> profileScoreMap = new HashMap<Integer, Double>();
		
		/*
		rs = stmt.executeQuery("select a.profile_id, a.target_id, count(*) from " + schema + indexTable + " a, " + schema + finalTable + " b where a.profile_id = b.profile_id and a.target_id = b.target_id group by a.profile_id, a.target_id");
		while (rs.next()) {
			int profileID = rs.getInt(1);
			int targetID = rs.getInt(2);
			double freq = rs.getDouble(3);
			
			profileScoreMap.put(profileID + "|" + targetID, freq);
			
			if (freq > maxFreq)
				maxFreq = freq;
		}
		*/
		
		double targetBase = Math.floor(Math.log10(maxTargetLen)) + 1.0;
		targetBase = Math.pow(10.0, targetBase);
		
		
		for (String key : posMap.keySet()) {
			String[] parts = key.split("\\|");
			int profileID = Integer.parseInt(parts[0]);
			int targetID = Integer.parseInt(parts[1]);
			Integer len = profileLengthMap.get(profileID);
			if (len == null)
				continue;
			
			int count = posMap.get(key);
			String targetStr = profileStringMap.get(targetID);
			if (targetStr == null)
				continue;
			
			double targetLen = ((double) targetStr.length()) / targetBase;
			//profileScoreMap.put(profileID, ((double) count + targetLen));
			profileScoreMap.put(profileID, ((double) count));
			
			if (count > maxFreq)
				maxFreq = count;
		}
		
		
		/*
		for (String key : negMap.keySet()) {
			String[] parts = key.split("\\|");
			int profileID = Integer.parseInt(parts[0]);
			int targetID = Integer.parseInt(parts[1]);
			Integer len = profileLengthMap.get(profileID);
			if (len == null)
				continue;
			
			int count = negMap.get(key);
			String targetStr = profileStringMap.get(targetID);
			if (targetStr == null)
				continue;
			
			int targetLen = targetStr.length();
			Double score = profileScoreMap.get(key);
			
			if (score == null) {
				score = 0.0;
				profileScoreMap.put(profileID, ((double) count + targetLen));
			}
			else
				profileScoreMap.put(profileID, score + count);
			
			if (score + count > maxFreq)
				maxFreq = score + count;
		}
		*/
		
		
		double exp = Math.floor(Math.log10(maxFreq)) + 1.0;
		double base = Math.pow(10.0, exp);
		
		for (int profileID : profileScoreMap.keySet()) {
			
			Integer len = profileLengthMap.get(profileID);
			if (len == null)
				continue;
			
			double score = profileScoreMap.get(profileID);
			score = ((double) len) - (score / base);
			profileScoreMap.put(profileID, score);
			System.out.println(profileID + " score: " + score);
		}
		
		
		System.out.println("get overlapping profiles...");
		Map<String, String> profileUseMap = new HashMap<String, String>();
		Map<String, Double> profileUseScoreMap = new HashMap<String, Double>();
		profileFilterMap = new HashMap<String, Boolean>();
		
		Map<String, Integer> useTotalMap = new HashMap<String, Integer>();
		
		
		rs = stmt.executeQuery("select b.document_id, b.start, a.profile_id, a.target_id, a.total from " + schema + finalTable + " a, " + schema + rq + indexTable + rq + " b, " + schema + "profile c "
			+ "where a.profile_id = b.profile_id and a.target_id = b.target_id and a.prec >= " + posThreshold + " and a.total >= " + posMinCount + " and c.annotation_type =  '" + annotType + "'"
			+ " and a.profile_id = c.profile_id"
			+ " order by b.profile_id, b.target_id");
			
		
		/*
		rs = stmt.executeQuery("select b.document_id, b.start, a.profile_id, a.target_id, a.total from " + schema + finalTable + " a, " + schema + rq + indexTable + rq + " b "
				+ "where a.profile_id = b.profile_id and a.target_id = b.target_id"
				+ " order by b.profile_id, b.target_id");
				*/
		
		while (rs.next()) {
			long docID = rs.getLong(1);
			long start = rs.getLong(2);
			int profileID = rs.getInt(3);
			int targetID = rs.getInt(4);
			int total = rs.getInt(5);
			//int skew = profileSkewMap.get(profileID);
			int profileType = profileTypeMap.get(profileID);
			if (profileType == 3)
				profileType = 0;
			
			
			Double score = profileScoreMap.get(profileID);
			if (score == null)
				System.out.println("score null! " + profileID);
			//String key = docID + "|" + start + "|" + skew;
			//String key = docID + "|" + start + "|" + targetID + "|" + profileType;
			String key = docID + "|" + start + "|" + profileType;
			
			Double useScore = profileUseScoreMap.get(key);
			Integer useTotal = useTotalMap.get(key);

			String oldProfile = profileUseMap.get(key);
			if (oldProfile == null)
				oldProfile = "";

			if (useScore == null) 
				useScore = 1000000.0;
			
			if (useTotal == null)
				useTotal = 0;

			if (score <= useScore && total > useTotal) {
				//String oldProfile = profileUseMap.get(key);
				if (oldProfile == null)
					oldProfile = "";
				
				profileUseScoreMap.put(key, score);
				profileUseMap.put(key, profileID + "|" + targetID);
				useTotalMap.put(key, total);
				
				System.out.println("filter: " + key + " " + profileID + "|" + targetID);

				//profileFilterMap.put(profileID + "|" + targetID, true);
			}
		}
		
		for (String key : profileUseMap.keySet()) {
			String pKey = profileUseMap.get(key);
			profileFilterMap.put(pKey, true);
		}
		
		System.out.println("filtered size: " + profileFilterMap.size());
		
		//filter
		
		/*
		for (String key : profileFilterMap.keySet()) {
			String[] parts = key.split("\\|");
			int profileID = Integer.parseInt(parts[0]);
			String profileStr = profileStringMap.get(profileID);
			//System.out.println(key + ": " + profileStr);
		}
		*/
		
		
		int count = 0;
		if (write) {
			stmt.execute("update " + schema + finalTable + " set disabled = 1 where profile_id in "
				+ "(select a.profile_id from " + schema + "profile a where a.annotation_type = '" + annotType + "' and profile_type = 0)");
		}
		
		PreparedStatement pstmt = conn.prepareStatement("update " + schema + finalTable + " set disabled = 0 where profile_id = ? and target_id = ?");
		for (String key : profileFilterMap.keySet()) {
			String[] parts = key.split("\\|");
			int profileID = Integer.parseInt(parts[0]);
			int targetID = Integer.parseInt(parts[1]);
			
			System.out.println("filtered: " + profileID + "|" + targetID);
			
			if (write) {
				pstmt.setInt(1, profileID);
				pstmt.setInt(2, targetID);
				pstmt.addBatch();
				//conn.commit();
			}
			
			count++;
			if (count % 1000 == 0) {
				pstmt.executeBatch();
				conn.commit();
			}
		}
		
		pstmt.executeBatch();
		conn.commit();

		//pstmt.close();
		stmt.close();
		
	}
	
	private void preloadCounts() throws SQLException
	{
		//preload pos and neg counts
		//preload inactive profile/target pairs
		
		preloadMap = new HashMap<String, Boolean>();
		
		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery("select a.profile_id, a.target_id, a.true_pos, a.false_pos, a.disabled from " + schema + finalTable + " a, " + schema + profileTable + " b "
			+ "where b.annotation_type = '" + annotType + "' and a.profile_id = b.profile_id");
		while (rs.next()) {
			long profileID = rs.getLong(1);
			long targetID = rs.getLong(2);
			int posCount = rs.getInt(3);
			int negCount = rs.getInt(4);
			Integer disabled = rs.getInt(5);
			
			/*
			if (disabled != null && disabled == 1) {
				inactiveMap.put(profileID + "|" + targetID, true);
				continue;
			}
			*/
			
			preloadMap.put(profileID + "|" + targetID, true);
			
			if (posCount > 0) {
				posMap.put(profileID + "|" + targetID, posCount);
			}
			
			if (negCount > 0)
				negMap.put(profileID + "|" + targetID, negCount);
		}
	}

	
	
	private void realignAnnotations() throws SQLException
	{
		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery("select b.profile from " + schema + finalTable + " b, " + schema + profileTable + " b where a.prec > " + posThreshold + " and a.target_id = b.profile_id");
		
		List<String> annotTypeList = new ArrayList<String>();
		while (rs.next()) {
			String profileStr = rs.getString(1);
			String[] parts = profileStr.split("\\!");
			
			for (int i=0; i<parts.length; i++) {
				String parts2[] = parts[i].split("\\|");
				
				//filter out syntax level annotations
				if (!parts2[0].equals("token") && !parts2[0].equals("syntaxtreenode"))
					annotTypeList.add(parts2[0]);
			}
			
		}
		
		StringBuilder strBlder = new StringBuilder();
		for (String annotType : annotTypeList) {
			if (strBlder.length() > 0)
				strBlder.append(" or ");
			strBlder.append("annotation_type = '" + annotType + "'");
		}
		
		
		
		stmt.close();
	}
	
	
	private void readProfiles() throws SQLException
	{
		System.out.println("read all profiles...");
		
		List<Integer> basicTargetIDList = getBasicTargetList();
		List<String> basicList = new ArrayList<String>();
		
		Statement stmt = conn.createStatement();
		
		ResultSet rs = stmt.executeQuery("select profile_id, profile from " + schema + "profile where annotation_type = '" + annotType + "' and profile_type = 0");
		while (rs.next()) {
			int profileID = rs.getInt(1);
			String profileStr = rs.getString(2);
			
			
			List<String> tokList = new ArrayList<String>();
			tokList = gson.fromJson(profileStr, tokList.getClass());
			
			
			int tokCount = 0;
			for (String tok : tokList) {
				if (tok.equals(":start") || tok.equals(":end") || tok.equals(":target"))
					continue;
				
				
				if (tok.indexOf("|string") < 0)
					continue;
					
				
				tokCount++;
			}
			
			if (tokCount < minToks || tokCount > maxToks)
				continue;
			
			
			
			String basicProfile = basicProfile(tokList);
			System.out.println("orig: " + profileStr);
			System.out.println("basic: " + basicProfile);

			
			boolean insert = true;
			for (int i=0; i<basicList.size(); i++) {
				String basicProfile2 = basicList.get(i);
				
				if (basicProfile.indexOf(basicProfile2) >= 0) {
					System.out.println(basicProfile2 + "\nis sub to\n" + basicProfile + "\n");
					insert = false;
					break;
				}
				else if (basicProfile2.indexOf(basicProfile) >= 0) {
					basicList.remove(i);
					//basicList.add(basicProfile);
					System.out.println(basicProfile + "\nis sub to\n" + basicProfile2 + "\n");
					i--;
					
				}
			}
			
			if (insert) {
				basicList.add(basicProfile);
			}
			else
				continue;
			
			
			
			
			for (int targetID : basicTargetIDList) {
				posMap.put(profileID + "|" + targetID, 0);
				negMap.put(profileID + "|" + targetID, 2);
			}
			
			
		}
		
		stmt.close();
	}
	
	
	private List<Integer> getBasicTargetList() throws SQLException
	{
		List<Integer> targetIDList = new ArrayList<Integer>();
		
		Map<String, Integer> indexMap = new HashMap<String, Integer>();
		Map<List<Boolean>, Boolean> activeMap = new HashMap<List<Boolean>, Boolean>();
		Map<List<Boolean>, Integer> targetIDMap = new HashMap<List<Boolean>, Integer>();
		
		Statement stmt = conn.createStatement();
		
		int currIndex = 0;
		
		ResultSet rs = stmt.executeQuery("select profile_id, profile from " + schema + "profile where annotation_type = '" + annotType
			+ "' and profile_type = 1");
		
		while (rs.next()) {
			List<Boolean> vec = new ArrayList<Boolean>();
			for (int i=0; i<currIndex; i++)
				vec.add(false);
			

			int targetID = rs.getInt(1);
			String profileStr = rs.getString(2);
			List<String> toks = new ArrayList<String>();
			toks = gson.fromJson(profileStr, toks.getClass());
			
			System.out.println("targetID: " + targetID + ", profile: " + profileStr);

			//TODO: this only works for single token targets now
			for (String tok : toks) {
				String[] parts = tok.split("!");
				
				for (int i=0; i<parts.length; i++) {
					
					Integer index = indexMap.get(parts[i]);
					if (index == null) {
						index = currIndex++;
						indexMap.put(parts[i], index);
						vec.add(true);
					}
					else
						vec.set(index, true);
				}
				
				int bits = countBits(vec);
				
				//determine overlap with other targets
				boolean flag = true;
				for (List<Boolean> vec2 : activeMap.keySet()) {
					if (activeMap.get(vec2) == null || !activeMap.get(vec2))
						continue;
					
					int bits2 = countBits(vec2);
					int targetID2 = targetIDMap.get(vec2);
					
					List<Boolean> or = vecOr(vec, vec2);
					if (vecEqual(or, vec) || vecEqual(or, vec2)) {
						if (bits < bits2) {
							activeMap.put(vec2, false);
							System.out.println(targetID2 + " overlapped by " + targetID);
						}
						else {
							flag = false;
							System.out.println(targetID + " overlapped by " + targetID2);
							break;
						}
					}
				}
				
				activeMap.put(vec, flag);
				targetIDMap.put(vec, targetID);
			}
		}
		
		//remove overlapped codes		
		for (List<Boolean> vec : activeMap.keySet()) {
			if (activeMap.get(vec)) {
				int targetID = targetIDMap.get(vec);
				targetIDList.add(targetID);
			}
		}
		
		
		stmt.close();
		
		System.out.println("target list size: " + targetIDList.size());
		
		return targetIDList;
	}
	
	
	private int countBits(List<Boolean> v)
	{
		int count=0;
		for (int i=0; i<v.size(); i++) {
			if (v.get(i))
				count++;
		}
		
		return count;
	}
	
	
	private boolean vecEqual(List<Boolean> v1, List<Boolean> v2)
	{
		int size = v1.size();
		List<Boolean> v3 = v2;
		
		if (size > v2.size()) {
			size = v2.size();
			v3 = v1;
		}
		
		for (int i=0; i<size; i++) {
			if (v1.get(i) != v2.get(i))
				return false;
		}
		
		for (int i=size; i<v3.size(); i++) {
			if (v3.get(i))
				return false;
		}
		
		return true;
	}
	
	private List<Boolean> vecOr(List<Boolean> v1, List<Boolean> v2)
	{
		List<Boolean> v = new ArrayList<Boolean>();
		List<Boolean> v3 = v2;
		int size = v1.size();
		if (size > v2.size()) {
			size = v2.size();
			v3 = v1;
		}
		
		for (int i=0; i<size; i++) {
			if (v1.get(i) || v2.get(i))
				v.add(true);
			else
				v.add(false);
		}	
		
		for (int i = size; i<v3.size(); i++) {
			v.add(v3.get(i));
		}
		
		return v;
	}

	
	private void readAnswers(long docID, String annotType, String provenance) throws SQLException
	{
		
		/*
		PreparedStatement pstmt = annotConn.prepareStatement("select a.start, a.end from annotation a, annotation b where a.document_id = ? and a.annotation_type = 'Token' and "
			+ "a.start >= b.start and a.end <= b.end and b.annotation_type = '" + annotType + "' and a.document_id = b.document_id order by start");

		if (docIDList == null) {
			docIDList = new ArrayList<Long>();
			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery("select distinct document_id from annotation where provenance = '" + provenance + " and annotation_type = '" + annotType + " order by document_id");
			while (rs.next()) {
				docIDList.add(rs.getLong(1));
			}
			
			stmt.close();
		}
		
		for (long docID : docIDList) {
			pstmt.setLong(1, docID);
			
			ResultSet rs = pstmt.executeQuery();
			while (rs.next()) {
				int start = rs.getInt(1);
				int end = rs.getInt(2);
				//String value = rs.getString(3);
				ansMap.put(docID + "|" + start + "|" + end, true);
				//System.out.println(docID + "|" + start2 + "|" + end2 + ", value: " + value);
			}
			
		}
		*/
		
		
		
		Statement stmt = annotConn.createStatement();
		/*
		ResultSet rs = stmt.executeQuery("select distinct a.document_id, a.start, a.end from annotation a, annotation b where a.annotation_type = 'Token' and "
			+ "a.start >= b.start and a.end <= b.end and b.annotation_type = '" + annotType + "' and "
			+ "b.provenance = '" + provenance + "' and a.document_id = b.document_id and a.document_id < 1163 order by start");
			*/
		
		//adjust user annotations to align with Token boundaries
		PreparedStatement pstmt = annotConn.prepareStatement("select start, " + rq + "end" + rq + " from " + schema + annotTable + " where annotation_type = 'Token' and "
			+ "start < ? and " + rq + "end" + rq + " > ? and document_id = " + docID);
		
		
		ResultSet rs = stmt.executeQuery("select distinct document_id, start, " + rq + "end" + rq + ", value, provenance from " + schema + annotTable + " where document_id = " + docID + " and annotation_type = '" + annotType + 
			"' and provenance like '" + provenance + "' order by document_id, start");
		
		while (rs.next()) {
			int start = rs.getInt(2);
			int end = rs.getInt(3);
			String value = rs.getString(4);
			String provenance2 = rs.getString(5);
			
			
			pstmt.setInt(1, start);
			pstmt.setInt(2, start);
			ResultSet rs2 = pstmt.executeQuery();
			if (rs2.next()) {
				start = rs2.getInt(1);
			}
			
			pstmt.setInt(1, end);
			pstmt.setInt(2, end);
			rs2 = pstmt.executeQuery();
			if (rs2.next()) {
				end = rs2.getInt(2);
			}
			
			
			//System.out.println("ans: " + docID + "|" + start + "|" + end + "|" + value + "|" + provenance2);
			
			String provenance3 = ansMap.get(docID + "|" + start + "|" + end);
			if (provenance3 != null) { 
				if (provenance3.equals("validation-tool"))
					continue;
				else if (provenance3.equals("validation-tool-unlabeled")) {
					ansMap.put(docID + "|" + start + "|" + end, provenance2);
				}
			}
			else
				ansMap.put(docID + "|" + start + "|" + end, provenance2);
			
			provenance2 = ansMap.get(docID + "|" + start + "|" + end);
			System.out.println("ans: " + docID + "|" + start + "|" + end + "|" + value + "|" + provenance2);
		}
		
		stmt.close();
		
	}
	
	
	public void removeDupes()
	{
		try {
			Connection conn = DBConnection.dbConnection("fmeng", "fmeng", "10.9.94.203", "msa", "mysql");
			Statement stmt = conn.createStatement();
			PreparedStatement pstmt = conn.prepareStatement("delete from msa_profile where profile_id = ?");
			
			Map<String, Boolean> map = new HashMap<String, Boolean>();
			ResultSet rs = stmt.executeQuery("select profile_id, profile from msa_profile where profile_type = 1 order by profile_id");
			while (rs.next()) {
				long profileID = rs.getLong(1);
				String profile = rs.getString(2);
				
				Boolean flag = map.get(profile);
				if (flag == null) {
					map.put(profile, true);
				}
				else {
					System.out.println("Dupe: " + profileID + ", " + profile);
					pstmt.setLong(1, profileID);
					pstmt.execute();
				}
			}
			
			stmt.close();
			conn.close();
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	private void removeSubpatterns()
	{
		
	}
	
	private String basicProfile(List<String> toks)
	{
		StringBuilder strBlder = new StringBuilder();
		
		for (String tok : toks) {
			String partName = "|category";
			if (tok.indexOf("|string|") >= 0)
				partName = "|string|";
			else if (tok.indexOf("|root|") >= 0)
				partName = "|root|";
			else if (tok.startsWith(":"))
				partName = ":";
			
			String[] parts = tok.split("!");
			
			String tok2 = "";
			for (int i=0; i<parts.length; i++) {
				if (partName.equals(":") && parts[i].startsWith(":")) {
					tok2 = parts[i];
					break;
				}
				else if (partName.equals("|string|") && parts[i].indexOf("|string|") >= 0) {
					tok2 = parts[i];
					break;
				}
				else if (partName.equals("|root|") && parts[i].indexOf("|root|") >= 0) {
					tok2 = parts[i];
					break;
				}
				else if (partName.equals("|category|") && parts[i].indexOf("|category|") >= 0){
					tok2 = parts[i];
					break;
				}
				else
					tok2 = parts[i];
			}
			
			strBlder.append(tok2 + " ");
		}
		
		return strBlder.toString().trim();
	}
	
	public static void main(String[] args)
	{
		
		if (args.length != 6) {
			System.out.println("usage: msaUser msaPassword annotUser annotPassword config [best/filter]");
			System.exit(0);
		}
		
		
		BestPatterns best = new BestPatterns();
		//filter.filter(args[0], args[1], args[2], args[3], args[4], args[5]);
		//filter.getBestPatterns("fmeng", "fmeng", "fmeng", "fmeng", "10.9.94.203", "ner", "msa", "mysql", "I-NOT-PER", "final_not_per", 
		//	"index_not_per", "profile_not_per", "conll2003-not-per-token", 0.9, 20);
		//filter.filter("fmeng", "fmeng", "10.9.94.203", "msa", "mysql", "I-PER", "msa_profile_target_final_per", "msa_profile_match_index_per", "msa_profile_per", "conll2003-token");
		//filter.removeDupes();
		
		try {
			Properties props = new Properties();
			props.load(new FileReader(args[4]));
			best.init(props);
			
			if (args[5].equals("best"))
				best.getBestPatterns(args[0], args[1], args[2], args[3]);
			else if (args[5].equals("filter"))
				best.filterOverlapping(args[0], args[1], args[2], args[3]);
			
			best.close();
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
}
