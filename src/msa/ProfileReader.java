package msa;

import java.sql.*;
import java.util.*;

import com.google.gson.Gson;

import utils.db.DBConnection;

public class ProfileReader
{
	private Gson gson;
	private Connection conn;
	private String rq;
	private String dbType;
	private String docNamespace;
	private String docTable;
	//private com.datastax.driver.core.PreparedStatement pstmtCass;
	private PreparedStatement pstmtSQL;
	private Order order = Order.NONE;
	private double minScore = 0.0;
	private double maxScore = 1.0;
	
	private Map<Long, MSAProfile> profileIDMap;
	
	public enum Order {NONE, ASC, DSC};
	

	
	public ProfileReader()
	{
		gson = new Gson();
	}
	
	public void setOrder(Order order)
	{
		this.order = order;
	}
	
	public void setMinScore(double minScore)
	{
		this.minScore = minScore;
	}
	
	public void setMaxScore(double maxScore)
	{
		this.maxScore = maxScore;
	}
	
	public Map<Long, MSAProfile> getProfileIDMap()
	{
		return profileIDMap;
	}
	
	public void init(String user, String password, String host, String dbType, String keyspace) throws SQLException, ClassNotFoundException
	{
		this.dbType = dbType;
		//this.docNamespace = docNamespace;
		//this.docTable = docTable;
		
		String queryStr = "select profile_id, profile, profile_type, score, true_pos, false_pos from msa_profile where annotation_type = ? and `group` = ? and profile_type = ?";
		
		if (dbType.equals("mysql")) {
			conn = DBConnection.dbConnection(user, password, host, keyspace, dbType);
			rq = DBConnection.reservedQuote;
			pstmtSQL = conn.prepareStatement(queryStr);
		}
	}
	
	public void close()
	{
		try {
			if (conn != null)
				conn.close();
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	public List<MSAProfile> read(String annotType, int profileType, String msaTable) throws SQLException, ClassNotFoundException
	{
		List<String> groupList = new ArrayList<String>();
		
		return read(annotType, groupList, 0, Integer.MAX_VALUE, profileType, msaTable);
	}
	
	public List<MSAProfile> read(String annotType, String group, int start, int clusterSize, int profileType, String msaTable) throws SQLException, ClassNotFoundException
	{
		List<String> groupList = new ArrayList<String>();
		groupList.add(group);
		
		return read(annotType, groupList, start, clusterSize, profileType, msaTable);
	}
	
	public List<MSAProfile> read(String annotType, List<String> groupList, int start, int clusterSize, int profileType, String msaTable) throws SQLException, ClassNotFoundException
	{
		List<MSAProfile> profileList= new ArrayList<MSAProfile>();
		profileIDMap = new HashMap<Long, MSAProfile>();

		if (dbType.equals("cassandra")) {
			/*
			com.datastax.driver.core.ResultSet rs = session.execute(pstmtCass.bind(annotType, group, profileType));
			Iterator<Row> iter = rs.iterator();
			for (;iter.hasNext();) {
				Row row = iter.next();
				int msaID = row.getInt(0);
				
				String profileStr = row.getString(1);
				int type = row.getInt(2);
				double score = row.getDouble(3);
				
				
				if (score >= minScore) {
					List<String> toks = new ArrayList<String>();
					toks = gson.fromJson(profileStr, toks.getClass());
					MSAProfile profile = new MSAProfile(profileStr, annotType, group, type, toks, score);
					
					if (order == Order.NONE)
						profileList.add(profile);
					else
						insertIntoProfileList(profileList, profile);
				}
			}
			*/
		}
		else if (dbType.equals("mysql")) {
			/*
			pstmtSQL.setString(1, annotType);
			pstmtSQL.setString(2, group);
			pstmtSQL.setInt(3, profileType);
			*/
			
			Statement stmt = conn.createStatement();
			StringBuilder strBlder = new StringBuilder();
			
			if (groupList.size() > 0) {
				
				for (String group : groupList) {
					if (strBlder.length() > 0)
						strBlder.append(",");
					strBlder.append("'" + group + "'");
				}
				
				strBlder.insert(0, " and `group` in (");
				strBlder.append(")");
			}

			ResultSet rs = stmt.executeQuery("select profile_id, profile, profile_type, score, true_pos, false_pos, `group` from " + msaTable
				+ " where annotation_type = '" + annotType + "' and profile_type = " + profileType + strBlder.toString() + " order by profile_id "
				+ "limit " + start + ", " + clusterSize);

			while (rs.next()) {
				long profileID = rs.getLong(1);
				
				
				/*
				String profileJSONStr = row.getString(1);
				
				Map<String, String> profileMap = new HashMap<String, String>();
				profileMap = (Map<String, String>) gson.fromJson(profileJSONStr, profileMap.getClass());
				String profileStr = profileMap.get("profile");
				String contextStr = profileMap.get("context");
				*/
				
				String profileStr = rs.getString(2);
				int type = rs.getInt(3);	
				double score = rs.getDouble(4);
				int truePos = rs.getInt(5);
				int falsePos = rs.getInt(6);
				String group = rs.getString(7);
				

				//System.out.println(profileStr);
				
				
				//remove targets with multiple elements
				//if (profileType == 1 && (profileStr.indexOf("\",\"") >= 0 || ((profileStr.startsWith("[\":token|string") || profileStr.startsWith("[\":token|root")) && profileStr.indexOf("token|string|.") < 0)))
				if (profileType == 1 && (profileStr.indexOf("\",\"") >= 0 || profileStr.indexOf(":" + annotType.toLowerCase()) >= 0))
					continue;
				
				
				if (score >= minScore && score <= maxScore) {
					List<String> toks = new ArrayList<String>();
					toks = gson.fromJson(profileStr, toks.getClass());
					
					MSAProfile profile = new MSAProfile(profileStr, annotType, group, type, toks, score, truePos, falsePos);
					profile.setProfileID(profileID);
					
					profileIDMap.put(profileID, profile);
					
					if (order == Order.NONE)
						profileList.add(profile);
					else
						insertIntoProfileList(profileList, profile);
				}
			}
			
			stmt.close();
		}
		
		return profileList;
			
	}
	
	public Map<MSAProfile, List<MSAProfile>> readFinal(String annotType, int total, double prec, String finalProfileTable, String profileTable) throws SQLException
	{	
		Map<MSAProfile, List<MSAProfile>> profileMap = new HashMap<MSAProfile, List<MSAProfile>>();
		Map<Long, MSAProfile> targetMap = new HashMap<Long, MSAProfile>();
		
		Statement stmt = conn.createStatement();
		
		ResultSet rs = stmt.executeQuery("select a.profile_id, a.target_id, b.profile, b.`group`, c.profile "
			+ "from " + finalProfileTable + " a, " + profileTable + " b, " + profileTable + " c "
			+ "where b.annotation_type = '" + annotType + "' and a.profile_id = b.profile_id and a.target_id = c.profile_id and "
			+ "a.total >= " + total + " and a.prec >= " + prec);
		
		Map<Long, MSAProfile> profileMap2 = new HashMap<Long, MSAProfile>();
		

		while (rs.next()) {
			long profileID = rs.getLong(1);
			long targetID = rs.getLong(2);
			String profileStr = rs.getString(3);
			String group = rs.getString(4);
			String targetStr = rs.getString(5);
			
			//remove targets with multiple elements
			if (targetStr.indexOf("\",\"") >= 0)
				continue;

			
			List<String> toks = new ArrayList<String>();
			toks = gson.fromJson(profileStr, toks.getClass());
			
			MSAProfile profile = profileMap2.get(profileID);
			if (profile == null) {
				profile = new MSAProfile(profileID, profileStr, annotType, group, 0, toks);
				List<MSAProfile> targetList = new ArrayList<MSAProfile>();
				profileMap.put(profile, targetList);
				profileMap2.put(profileID, profile);
			}
			
			List<MSAProfile> targetList = profileMap.get(profile);

			MSAProfile target = targetMap.get(targetID);
			if (target == null) {
				List<String> toks2 = new ArrayList<String>();
				toks2 = gson.fromJson(targetStr, toks2.getClass());
				target = new MSAProfile(targetID, targetStr, annotType, group, 1, toks2);
				targetMap.put(targetID, target);
			}
			
			boolean inserted = false;
			for (int i=0; i<targetList.size(); i++) {
				MSAProfile target2 = targetList.get(i);
				if (target2.getProfileStr().length() > target.getProfileStr().length()) {
					targetList.add(i, target);
					inserted = true;
					break;
				}
			}
			
			if (!inserted)
				targetList.add(target);				
		}
		
		stmt.close();
		
		return profileMap;
	}
	
	private void insertIntoProfileList(List<MSAProfile> profileList, MSAProfile profile)
	{
		boolean inserted = false;
		//int size1 = profile.getProfileToks().size();
		int size1 = profile.size();
		
		for (int i=0; i<profileList.size(); i++) {
			MSAProfile profile2 = profileList.get(i);
			//int size2 = profile2.getProfileToks().size();
			int size2 = profile2.size();
			
			if (order == Order.DSC) {
				if (size2 < size1) {
					profileList.add(i, profile);
					inserted = true;
					break;
				}
			}
			else if (order == Order.ASC) {
				if (size2 > size1) {
					profileList.add(i, profile);
					inserted = true;
					break;
				}
			}
		}
		
		if (!inserted)
			profileList.add(profile);
	}
	
	public Map<MSAProfile, List<MSAProfile>> readProfileTargets(String annotType) throws SQLException
	{
		Map<MSAProfile, List<MSAProfile>> targetProfileMap = new HashMap<MSAProfile, List<MSAProfile>>();
		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery("select a.profile_id, a.target_id from msa_profile_match_index_final a, msa_profile_final b "
			+ "where a.profile_id = b.profile_id and b.annotation_type = '" + annotType + "'");
		
		while (rs.next()) {
			long profileID = rs.getLong(1);			
			long targetID = rs.getLong(2);
			MSAProfile profile = profileIDMap.get(profileID);
			List<MSAProfile> targetList = targetProfileMap.get(profile);
			if (targetList == null) {
				targetList = new ArrayList<MSAProfile>();
				targetProfileMap.put(profile, targetList);
			}
						
			MSAProfile targetProfile = profileIDMap.get(targetID);
			targetList.add(targetProfile);
		}
		
		stmt.close();
		
		return targetProfileMap;
	}
	
	public static void main(String[] args)
	{
		try {
			ProfileReader reader = new ProfileReader();
			reader.init("cassandra", "cassandra", "192.99.100.31", "cassandra", "msa");
			List<MSAProfile> profileList = reader.read("lungrads-patient-age", "msa-lungrads", 0, 100, 0, "msa_profile");
			for (MSAProfile profile : profileList) {
				System.out.println(profile.getProfileStr());
			}
			reader.close();
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
}
