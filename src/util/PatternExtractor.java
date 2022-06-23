package util;

import java.io.*;
import java.sql.*;
import java.util.*;

import com.google.gson.Gson;

import utils.db.DBConnection;

public class PatternExtractor 
{
	private Connection conn;
	private String annotTable;
	private String schema;
	private PreparedStatement pstmt;
	private PreparedStatement pstmt2;
	private PreparedStatement pstmt3;
	
	private Map<Integer, int[]> pattRangeMap;
	private Gson gson;

	public PatternExtractor()
	{
		gson = new Gson();
	}
	
	public void extract(String user, String password, String config)
	{
		try {
			Properties props = new Properties();
			props.load(new FileReader(config));
			
			String host = props.getProperty("host");
			String dbName = props.getProperty("dbName");
			String dbType = props.getProperty("dbType");
			schema = props.getProperty("schema") + ".";
			String indexTable = props.getProperty("indexTable");
			String finalTable = props.getProperty("finalTable");
			String annotType = props.getProperty("annotType");
			annotTable = props.getProperty("annotTable");
			String profileTable = props.getProperty("profileTable");
			
			conn = DBConnection.dbConnection(user, password, host, dbName, dbType);
			String rq = DBConnection.reservedQuote;
			
			pstmt = conn.prepareStatement("select value from " + schema + annotTable + " where document_id = ? "
				+ "and start >= ? and " + rq + "end" + rq + " <= ? and annotation_type = 'Token' "
				+ "order by start");
			
			pstmt2 = conn.prepareStatement("select top(?) value from " + schema + annotTable + " where document_id = ? and start < ? and annotation_type = 'Token' order by start desc");
			pstmt3 = conn.prepareStatement("select top(?) value from " + schema + annotTable + " where document_id = ? and start > ? and annotation_type = 'Token' order by start");

			Statement stmt = conn.createStatement();
			
			Map<String, Integer> countMap = new HashMap<String, Integer>();
			pattRangeMap = new HashMap<Integer, int[]>();
			
			ResultSet rs = stmt.executeQuery("select a.profile_id, a.document_id, a.start, a." + rq + "end" + rq + ", b.profile "
				+ " from " + schema + indexTable + " a, " + schema + "profile b "
				+ "where a.profile_id = b.profile_id and a.profile_id in "
				+ "(select b.profile_id from " + schema + finalTable + " b, " + schema + profileTable + " c "
				+ "where c.annotation_type = '" + annotType + "' and b.disabled = 0 "
				+ "and b.profile_id = c.profile_id)");
			
			while (rs.next()) {
				int profileID = rs.getInt(1);
				long docID = rs.getLong(2);
				long start = rs.getLong(3);
				long end = rs.getLong(4);
				String profileStr = rs.getString(5);
				
				if (pattRangeMap.get(profileID) == null)
					setPatternRange(profileID, profileStr);
				
				String pattern = getPatternStr(docID, start, end, profileID);
				Integer count = countMap.get(pattern);
				
				if (count == null) {
					count = 0;
				}
				
				countMap.put(pattern, ++count);
			}
			
			List<String> pattList = new ArrayList<String>();
			List<Integer> countList = new ArrayList<Integer>();
			
			for (String patt : countMap.keySet()) {
				int count = countMap.get(patt);
				
				boolean inserted = false;
				for (int i=0; i<countList.size(); i++) {
					if (count > countList.get(i)) {
						countList.add(i, count);
						pattList.add(i, patt);
						inserted = true;
						break;
					}
				}
				
				if (!inserted) {
					countList.add(count);
					pattList.add(patt);
				}
			}
			
			for (int i=0; i<pattList.size(); i++) {
				System.out.println(pattList.get(i) + "\ncount: " + countList.get(i) + "\n\n");
			}
			
			
			conn.close();
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	private String getPatternStr(long docID, long start, long end, int profileID) throws SQLException
	{
		StringBuilder strBlder = new StringBuilder();
		Statement stmt = conn.createStatement();
		
		int[] ranges = pattRangeMap.get(profileID);
		
		//tokens before target
		pstmt2.setInt(1, ranges[0]);
		pstmt2.setLong(2, start);
		ResultSet rs = pstmt2.executeQuery();
		while (rs.next()) {
			String val = rs.getString(1);
			strBlder.insert(0, val + " ");
		}
		
		//target
		pstmt.setLong(1, docID);
		pstmt.setLong(2, start);
		pstmt.setLong(3, end);
		rs = pstmt.executeQuery();
		
		while (rs.next()) {
			String val = rs.getString(1);
			
			strBlder.append(val + " ");
		}
		
		//tokens after target
		pstmt3.setInt(1, ranges[1]);
		pstmt3.setLong(2, end);
		rs = pstmt3.executeQuery();
		while (rs.next()) {
			String val = rs.getString(1);
			strBlder.append(val + " ");
		}
		
		
		
		
		stmt.close();
		
		return strBlder.toString().trim();
	}
	
	private void setPatternRange(int profileID, String profileStr)
	{
		int[] ranges = new int[2];
		
		List<String> toks = new ArrayList<String>();
		toks = gson.fromJson(profileStr, toks.getClass());
		
		int tokIndex = 0;
		for (int i=0; i<toks.size(); i++) {
			if (toks.get(i).equals(":target")) {
				tokIndex = i;
				break;
			}
		}
		
		ranges[0] = tokIndex;
		ranges[1] = toks.size() - tokIndex - 1;
		
		pattRangeMap.put(profileID, ranges);
	}
	
	public static void main(String[] args)
	{
		if (args.length != 3) {
			System.out.println("usage: user password config");
			System.exit(0);
		}
		
		PatternExtractor pattExtract = new PatternExtractor();
		pattExtract.extract(args[0], args[1], args[2]);
	}
}
