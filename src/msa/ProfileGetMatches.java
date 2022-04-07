package msa;

import java.io.*;
import java.sql.*;
import java.util.*;

import utils.db.DBConnection;

public class ProfileGetMatches 
{
	private Connection conn;
	private String rq;
	private String schema;
	
	private String indexTable;
	private String annotTable;
	private PreparedStatement pstmt;
	private PreparedStatement pstmtProfile;
	private PreparedStatement pstmtTarget;
	private PreparedStatement pstmtSents;
	
	public ProfileGetMatches()
	{
	}
	
	public void getMatches(String user, String password, String config)
	{
		try {
			Properties props = new Properties();
			props.load(new FileReader(config));
		
			String host = props.getProperty("host");
			String dbName = props.getProperty("dbName");
			String dbType = props.getProperty("dbType");
			schema = props.getProperty("schema") + ".";
			String profileQuery = props.getProperty("profileQuery");
			String profileTable = props.getProperty("profileTable");
			indexTable = props.getProperty("indexTable");
			annotTable = props.getProperty("annotTable");
			
			List<Integer> profileIDList = new ArrayList<Integer>();
			List<Integer> targetIDList = new ArrayList<Integer>();
			
			conn = DBConnection.dbConnection(user, password, host, dbName, dbType);
			rq = DBConnection.reservedQuote;
			
			Statement stmt = conn.createStatement();
			pstmt = conn.prepareStatement("select document_id, start, + " + rq + "end" + rq + " from " + schema + indexTable 
				+ " where profile_id = ? and target_id = ?");
			pstmtProfile = conn.prepareStatement("select profile from " + schema + "profile where profile_id = ?");
			pstmtTarget = conn.prepareStatement("select profile from " + schema + "profile where target_id = ?");
			
			
			ResultSet rs = stmt.executeQuery(profileQuery);
			while (rs.next()) {
				int profileID = rs.getInt(1);
				int targetID = rs.getInt(2);
				
				pstmtProfile.setInt(1, profileID);
				ResultSet rs2 = pstmtProfile.executeQuery();
				String profileStr = "";
				
				if (rs2.next())
					profileStr = rs2.getString(1);
				
				String targetStr = "";				
				pstmtTarget.setInt(1, targetID);
				rs2 = pstmtTarget.executeQuery();
				if (rs2.next())
					targetStr = rs2.getString(1);
								
				System.out.println("\n\n" + profileID + " | " + profileStr);
				System.out.println(targetID + " | " + targetStr);
				
				readMatches(profileID, targetID);
			}
			
			
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	private void readMatches(int profileID, int targetID) throws SQLException
	{
		pstmt.setInt(1, profileID);
		pstmt.setInt(2, targetID);
		
		ResultSet rs = pstmt.executeQuery();
		while (rs.next()) {
			long docID = rs.getLong(1);
			long start = rs.getLong(2);
			long end = rs.getLong(3);
			
			getSentences(docID, start, end, 3);
		}
	}
	
	private String getSentences(long docID, long start, long end, int windowSize) throws SQLException
	{
		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery("select start from " + schema + annotTable 
			+ " where document_id = " + docID + " and annotation_type = 'Sentence' and start <= " + start 
			+ " order by start desc");
		
		long minStart = -1;
		int count = 0;
		while (rs.next()) {
			minStart = rs.getLong(1);
			count++;
			if (count >= windowSize)
				break;
		}
		
		rs = stmt.executeQuery("select " + rq + "end" + rq + " from " + schema + annotTable
			+ " where document_id = " + docID + " and annotation_type = 'Sentence' and " + rq + "end" + rq + " >= " + end 
			+ " order by " + rq + "end" + rq);
		
		long maxEnd = -1;
		count = 0;
		while (rs.next()) {
			maxEnd = rs.getLong(1);
			count++;
			if (count >= windowSize)
				break;
		}
		
		rs = stmt.executeQuery("select value, start, " + rq + "end"	+ rq 
			+ " where document_id = " + docID + " and annotation_type = 'Token' "
			+ "and start >= " + minStart + " and " + rq + "end"	 + rq + " <= " + maxEnd
			+ " order by start");
		
		StringBuffer strBuf = new StringBuffer();
		long prevStart = -1;
		while (rs.next()) {
			String val = rs.getString(1);
			long currStart = rs.getLong(2);
			long currEnd = rs.getLong(3);
			
			if (start >= prevStart && start <= currStart)
				strBuf.append(" [ ");
			else if (end >= prevStart && end <= currStart)
				strBuf.append(" ] ");
			
			strBuf.append(val + " ");
			
			prevStart = currStart;
		}
		
		System.out.println(docID + "|" + start + "|" + end + "|" + minStart + "|" + maxEnd + "|" + strBuf.toString());
		
		return strBuf.toString();
	}
	
	
	static public void main(String[] args)
	{
		if (args.length != 3) {
			System.out.println("usage: user password config");
			System.exit(0);
		}
		
		ProfileGetMatches get = new ProfileGetMatches();
		get.getMatches(args[0], args[1], args[2]);
	}
}
