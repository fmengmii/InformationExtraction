package analysis;

import java.sql.*;
import java.util.*;
import java.io.*;

import com.google.gson.Gson;

import utils.db.DBConnection;

public class PatternCoverage
{
	
	private Connection conn;
	private Gson gson;


	public PatternCoverage()
	{
		gson = new Gson();
	}
	
	public void getPatternCoverage(String user, String password, String config) throws SQLException, ClassNotFoundException, IOException
	{
		Properties props = new Properties();
		props.load(new FileInputStream(config));
		
		String host = props.getProperty("host");
		String dbName = props.getProperty("dbName");
		String annotType = props.getProperty("annotType");
		String provenance = props.getProperty("provenance");
		String autoProvenance = props.getProperty("autoProvenance");
		String profileTable = props.getProperty("profileTable");
		
		
		conn = DBConnection.dbConnection(user, password, host, dbName, "mysql");
		
		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery("select distinct b.document_id, b.start, b.value, b.features from annotation a, annotation b "
			+ "where a.document_id = b.document_id and a.start = b.start and a.annotation_type = '" + annotType + "' and a.provenance = '" + provenance + "' "
			+ "and b.annotation_type = '" + annotType + "' and b.provenance = '" + autoProvenance + "' and b.features is not null order by document_id, start;");
		//ResultSet rs = stmt.executeQuery(profileQuery);
		PreparedStatement pstmt = conn.prepareStatement("select profile from " + profileTable + " where profile_id = ?");
		
		List<String> profileList = new ArrayList<String>();
		List<Integer> totalList = new ArrayList<Integer>();
		Map<String, Integer> profileMap = new HashMap<String, Integer>();
		
		int count = 0;
		int covered = 0;
		
		while (rs.next()) {
			String features = rs.getString(4);
			Map<String, Object> featureMap = new HashMap<String, Object>();
			featureMap = gson.fromJson(features, featureMap.getClass());
			long profileID =  ((Double) featureMap.get("profileID")).longValue();
			long targetID = ((Double) featureMap.get("targetID")).longValue();
			
			String key = profileID + "|" + targetID;
			Integer total = profileMap.get(key);
			if (total == null) {
				total = 0;
			}
			profileMap.put(key, ++total);
			
			count++;
			covered++;
		}
		
		for (String key : profileMap.keySet()) {
			int total = profileMap.get(key);
			
			String[] parts = key.split("\\|");
			
			boolean inserted = false;
			for (int i=0; i<totalList.size(); i++) {
				int total2 = totalList.get(i);
				if (total > total2) {
					inserted = true;
					profileList.add(i, key);
					totalList.add(i, total);
					break;
				}
			}
			
			if (!inserted) {
				profileList.add(key);
				totalList.add(total);
			}
		}
		
		
		for (int i=0; i<profileList.size(); i++) {
			String key = profileList.get(i);
			int total = totalList.get(i);
			
			String[] parts = key.split("\\|");
			long profileID = Long.parseLong(parts[0]);
			long targetID = Long.parseLong(parts[1]);
			
			pstmt.setLong(1, profileID);
			rs = pstmt.executeQuery();
			String profileStr = "";

			if (rs.next())
				profileStr = rs.getString(1);
			
			String targetStr = "";
			pstmt.setLong(1, targetID);
			rs = pstmt.executeQuery();

			if (rs.next())
				targetStr = rs.getString(1);
			
			
			System.out.println("profileID: " + profileID + ", targetID: " + targetID + ", total: " + total + ", profileStr: " + profileStr + ", targetStr: " + targetStr);
		}
		
		rs = stmt.executeQuery("select a.document_id, a.start, a.end, a.value from annotation a "
			+ "where annotation_type = '" + annotType + "' and provenance = '" + provenance + "' and (document_id, start) not in "
			+ "(select distinct b.document_id, b.start from annotation b where annotation_type = '" + annotType + "' and provenance = '" + autoProvenance + "')");
		//rs = stmt.executeQuery(negQuery);
		
		
		while (rs.next()) {
			long docID = rs.getLong(1);
			long start = rs.getLong(2);
			long end = rs.getLong(3);
			String value = rs.getString(4);
			
			String sentStr = getSentence(docID, start, end);
			System.out.println("not found: " + docID + "|" + start + "|" + value + ", " + sentStr);
			count++;
		}
		
		System.out.println("recall: " + (((double) covered) / ((double) count)));
		
		
		stmt.close();
		conn.close();
	}
	
	private String getSentence(long docID, long start, long end) throws SQLException
	{
		StringBuilder strBlder = new StringBuilder();
		Statement stmt = conn.createStatement();
		
		ResultSet rs = stmt.executeQuery("select value from annotation where document_id = " + docID + " and start >= " + (start-50) + " and start < " + start + " and annotation_type = 'Token' order by start");
		while (rs.next()) {
			strBlder.append(rs.getString(1) + " ");
		}
		
		strBlder.append("  TARGET  ");
		
		rs = stmt.executeQuery("select value from annotation where document_id = " + docID + " and start > " + end + " and start < " + (end+50) + " and annotation_type = 'Token' order by start");
		while (rs.next()) {
			strBlder.append(rs.getString(1) + " ");
		}
		
		stmt.close();
		
		return strBlder.toString();
	}
	
	public static void main(String[] args)
	{
		if (args.length != 3) {
			System.out.println("usage: user password config");
			System.exit(0);;
		}
		
		try {
			PatternCoverage pattCov = new PatternCoverage();
			pattCov.getPatternCoverage(args[0], args[1], args[2]);
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
}
