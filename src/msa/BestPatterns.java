package msa;

import java.sql.*;
import java.util.*;
import java.io.*;

import utils.db.DBConnection;

public class BestPatterns
{
	private Connection conn;
	private Connection annotConn;
	private PreparedStatement pstmt;
	private List<Long> docIDList;
	
	private String host;
	private String msaKeyspace;
	private String dbType;
	private String annotType;
	private String annotKeyspace;
	private String finalTable;
	private String indexTable;
	private String profileTable;
	private String provenance;
	private double negThreshold;
	private int negMinCount;
	private double posThreshold;
	private int posMinCount;
	
	private List<String> annotTypeList;
	private List<String> profileTableList;
	private List<String> indexTableList;
	private List<String> finalTableList;

	
	
	public BestPatterns()
	{
	}
	
	public void init(Properties props)
	{
		host = props.getProperty("host");
		msaKeyspace = props.getProperty("msaKeyspace");
		dbType = props.getProperty("dbType");
		annotType = props.getProperty("annotType");
		annotKeyspace = props.getProperty("annotKeyspace");
		finalTable = props.getProperty("finalTable");
		indexTable = props.getProperty("indexTable");
		profileTable = props.getProperty("profileTable");
		provenance = props.getProperty("provenance");
		negThreshold = Double.parseDouble(props.getProperty("negThreshold"));
		negMinCount = Integer.parseInt(props.getProperty("negMinCount"));
		posThreshold = Double.parseDouble(props.getProperty("posThreshold"));
		posMinCount = Integer.parseInt(props.getProperty("posMinCount"));
	}
	
	public void close()
	{
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
	
	public void getBestPatterns(String msaUser, String msaPassword, String annotUser, String annotPassword)
	{
		try {			
			conn = DBConnection.dbConnection(msaUser, msaPassword, host, msaKeyspace, dbType);
			annotConn = DBConnection.dbConnection(annotUser, annotPassword, host, annotKeyspace, dbType);
			
			
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
				
				
				PreparedStatement pstmt = conn.prepareStatement("insert into " + finalTable + " (profile_id, target_id, total, prec, valence) values (?,?,?,?,?)");
				PreparedStatement pstmtUpdateProfile = conn.prepareStatement("update " + profileTable + " set score = ? where profile_id = ?");
				
				Statement stmt = conn.createStatement();
				
				Boolean flag = finalTableMap.get(finalTable);
				if (flag == null) {
					System.out.println("deleting...");
					stmt.execute("delete from " + finalTable);
					finalTableMap.put(finalTable, true);
				}

			
				System.out.println("getting doc IDs...");
				docIDList = new ArrayList<Long>();
				ResultSet rs = stmt.executeQuery("select distinct document_id from " + indexTable + " order by document_id");
				while (rs.next()) {
					docIDList.add(rs.getLong(1));
				}
				
				
				System.out.println("reading answers...");
				Map<String, Boolean> ansMap = readAnswers(annotType, provenance);
				
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
				
				rs = stmt.executeQuery("select distinct a.profile_id, a.target_id, a.document_id, a.start, a.end from " + indexTable + " a, " + profileTable + " b "
					+ "where b.annotation_type = '" + annotType + "' and a.profile_id = b.profile_id");
				
				Map<String, Integer> posMap = new HashMap<String, Integer>();
				Map<String, Integer> negMap = new HashMap<String, Integer>();
				String targetStr = "";
				
				long currProfileID = -1;
				
				Map<Long, List<Long>> docMap = new HashMap<Long, List<Long>>();
				Map<Long, List<Long>> invDocMap = new HashMap<Long, List<Long>>();
				Map<Long, Integer> profileTotals = new HashMap<Long, Integer>();
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
					long docID = rs.getLong(3);
					long start = rs.getLong(4);
					long end = rs.getLong(5);
					
					
					
					/*
					int index = profileStr.indexOf(":target");
					if (index < 0) {
						System.out.println("ERROR: " + profileID + ", " + profileStr);
					}
					*/
					
					
					
		
					String key = profileID + "|" + targetID;
					Boolean ansFlag = ansMap.get(docID + "|" + start + "|" + end);
					
	
					long profileID2 = profileID;
					if (ansFlag != null) {
						//System.out.println(profileID + "|" + targetID + "|" + docID + "|" + start + "|" + end + "|" + targetStr + "|" + ansFlag);
	
						Integer count = posMap.get(key);
						if (count == null)
							count = 0;
						
						posMap.put(key, ++count);
						
						Integer profileCount = profileTotals.get(profileID);
						if (profileCount == null)
							profileCount = 0;
						profileTotals.put(profileID, ++profileCount);
						
						
					}
					else {
						Integer count = negMap.get(key);
						if (count == null)
							count = 0;
						
						negMap.put(key, ++count);
						
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
				
				
				conn.setAutoCommit(false);
				int count = 0;
				for (String key : posMap.keySet()) {
					int posCount = posMap.get(key);
					Integer negCount = negMap.get(key);
					
					if (negCount == null)
						negCount = 0;
					
					double prec = ((double) posCount) / ((double) (posCount + negCount));
	
					boolean write = false;
					int valence = 0;
					
					double score = 0.0;
					
					if (prec >= posThreshold && (posCount + negCount) >= posMinCount) {
						write = true;
						score = 1.0;
					}
					
					if (prec < negThreshold && (posCount + negCount) >= negMinCount) {
						score = 1.0;
					}
					
					System.out.println(key + " : " + posCount + ", " + negCount + ", " + prec + ", " + score);
					
					String[] parts = key.split("\\|");
					long profileID = Long.parseLong(parts[0]);
					long targetID = Long.parseLong(parts[1]);
					
					pstmtUpdateProfile.setDouble(1, score);					
					pstmtUpdateProfile.setLong(2, profileID);
					pstmtUpdateProfile.execute();
					
					if (write) {
						pstmt.setLong(1, profileID);
						pstmt.setLong(2, targetID);
						pstmt.setInt(3, posCount+negCount);
						pstmt.setDouble(4, prec);
						pstmt.setInt(5, valence);
						pstmt.addBatch();
						count++;
						
						if (count % 1000 == 0) {
							pstmt.executeBatch();
							conn.commit();
						}
					}
				}
				
				pstmt.executeBatch();
				conn.commit();
				
				stmt.close();
				pstmt.close();
				pstmtUpdateProfile.close();
			}
			
		
			
			System.out.println("filter overlap...");
			//filterOverlapping(annotType);
			//filterOverlapping(annotType, group, false);
			
			
			/*
			//check conditionals
			Iterator<Long> iter = profileTotals.keySet().iterator();
			for (;iter.hasNext();) {
				Long profileID = iter.next();
				//int total = profileTotals.get(profileID);
				
				Map<Long, Integer> posProfileMap = new HashMap<Long, Integer>();
				Map<Long, Integer> negProfileMap = new HashMap<Long, Integer>();
				List<Long> docList = docMap.get(profileID);
				
				if (docList == null)
					continue;
				
				for (long docID : docList) {
					List<Long> profileList = invDocMap.get(docID);
					
					for (long profileID2 : profileList) {
						Integer count = posProfileMap.get(profileID2);
						if (count == null)
							count = 0;
						posProfileMap.put(profileID2, ++count);
					}
				}
				
				docList = docMap.get(-profileID);
				
				if (docList != null) {
					for (long docID : docList) {
						List<Long> profileList = invDocMap.get(docID);
						
						for (long profileID2 : profileList) {
							Integer count = negProfileMap.get(profileID2);
							if (count == null)
								count = 0;
							negProfileMap.put(profileID2, ++count);
						}
					}
				}
				
				Iterator<Long> iter2 = posProfileMap.keySet().iterator();
				for (;iter2.hasNext();) {
					long profileID2 = iter2.next();
					int pos = posProfileMap.get(profileID2);
					Integer neg = negProfileMap.get(profileID2);
					if (neg == null)
						neg = 0;
					
					if (pos + neg > 20) {
						double condProb = pos / (pos + neg);
						if (condProb > 0.9)
							System.out.println(profileID + ", " + profileID2 + ", pos:" + pos + ", neg:" + neg + ", prob:" + condProb);
					}
				}
			}
			*/
			
			
			conn.close();
			annotConn.close();
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	public void filterOverlapping(String annotType) throws SQLException, ClassNotFoundException
	{
		PreparedStatement pstmt = conn.prepareStatement("delete from msa_profile_target_final where profile_id = ? and target_id = ?");
		
		String queryStr = "select distinct a.profile_id, a.target_id, a.document_id, a.start, a.end from msa_profile_match_index a, msa_profile b, msa_profile_target_final c "
			+ "where a.profile_id = b.profile_id and a.profile_id = c.profile_id and b.annotation_type = '" + annotType + "'";
		filterLoop(queryStr, pstmt);
		pstmt.close();
	}
	
	private void filterLoop(String queryStr, PreparedStatement pstmt) throws SQLException
	{
		Map<String, List<String>> profileMap = new HashMap<String, List<String>>();
		Map<String, List<String>> profileIDMap = new HashMap<String, List<String>>();
		
		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery(queryStr);
			
		while (rs.next()) {
			long profileID = rs.getLong(1);
			long targetID = rs.getLong(2);
			long docID = rs.getLong(3);
			int start = rs.getInt(4);
			int end = rs.getInt(5);
			
			List<String> profileIndexList = profileMap.get(profileID + "|" + targetID);
			if (profileIndexList == null) {
				profileIndexList = new ArrayList<String>();
				profileMap.put(profileID + "|" + targetID, profileIndexList);
			}
			
			String index = docID + "|" + start + "|" + end;
			profileIndexList.add(index);
			
			List<String> profileIDList = profileIDMap.get(index);
			if (profileIDList == null) {
				profileIDList = new ArrayList<String>();
				profileIDMap.put(index, profileIDList);
			}
			
			profileIDList.add(profileID + "|" + targetID);
		}
		
		int count = 0;
		for (String profileTargetID : profileMap.keySet()) {
			List<String> profileIndexList = profileMap.get(profileTargetID);
			int size = profileIndexList.size();
			
			for (int i=0; i<profileIndexList.size(); i++) {
				String index = profileIndexList.get(i);
				List<String> profileIDList = profileIDMap.get(index);
				
				for (String profileID2 : profileIDList) {
					if (!profileID2.equals(profileTargetID)) {
						List<String> profileIndexList2 = profileMap.get(profileID2);
						if (profileIndexList2.size() > size) {
							profileIndexList.remove(i);
							i--;
							break;
						}
					}
				}
			}
			
			if (profileIndexList.size() == 0) {
				System.out.println("overlapped: " + profileTargetID);
				String[] parts = profileTargetID.split("\\|");
				long profileID = Long.parseLong(parts[0]);
				long targetID = Long.parseLong(parts[1]);
				
				//stmt.execute("update msa_profile set score = 0.0 where profile_id = " + profileID);
				pstmt.setLong(1, profileID);
				pstmt.setLong(2, targetID);
				pstmt.addBatch();
				count++;
				
				if (count % 1000 == 0) {
					pstmt.executeBatch();
					conn.commit();
				}
			}
		}
		
		pstmt.executeBatch();
		conn.commit();
		
		stmt.close();
	}
	
	private Map<String, Boolean> readAnswers(String annotType, String provenance) throws SQLException
	{
		Map<String, Boolean> ansMap = new HashMap<String, Boolean>();
		
		
		
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
		
		
		ResultSet rs = stmt.executeQuery("select distinct document_id, start, end from annotation where annotation_type = '" + annotType + "' and "
				+ "provenance = '" + provenance + "' order by start");
		
		while (rs.next()) {
			long docID = rs.getLong(1);
			int start = rs.getInt(2);
			int end = rs.getInt(3);
			ansMap.put(docID + "|" + start + "|" + end, true);
		}
		
		stmt.close();
		
		return ansMap;
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
	
	public static void main(String[] args)
	{
		
		if (args.length != 5) {
			System.out.println("usage: msaUser msaPassword annotUser annotPassword config");
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
			best.getBestPatterns(args[0], args[1], args[2], args[3]);
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
}
