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
	private String annotType;
	private String schema;
	private PreparedStatement pstmt;
	private PreparedStatement pstmt2;
	private PreparedStatement pstmt3;
	
	private Map<Integer, int[]> pattRangeMap;
	private Gson gson;
	
	private String rq;
	
	private String indexTable;
	private String finalTable;
	private String profileTable;

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
			indexTable = props.getProperty("indexTable");
			finalTable = props.getProperty("finalTable");
			annotType = props.getProperty("annotType");
			annotTable = props.getProperty("annotTable");
			profileTable = props.getProperty("profileTable");
			
			conn = DBConnection.dbConnection(user, password, host, dbName, dbType);
			rq = DBConnection.reservedQuote;
			
			pstmt = conn.prepareStatement("select value from " + schema + annotTable + " where document_id = ? "
				+ "and start >= ? and " + rq + "end" + rq + " <= ? and annotation_type = 'Token' "
				+ "order by start");
			
			pstmt2 = conn.prepareStatement("select value from " + schema + annotTable + " where document_id = ? and start >= ? and start < ? and annotation_type = 'Token' order by start desc");
			//pstmt3 = conn.prepareStatement("select value from " + schema + annotTable + " where document_id = ? and start > ? and annotation_type = 'Token' order by start");

			Statement stmt = conn.createStatement();
			
			Map<String, Integer> countMap = new HashMap<String, Integer>();
			Map<String, List<String>> sentMap = new HashMap<String, List<String>>();
			pattRangeMap = new HashMap<Integer, int[]>();
			
			ResultSet rs = stmt.executeQuery("select distinct a.profile_id, a.document_id, a.start, a." + rq + "end" + rq + ", b.profile "
				+ " from " + schema + indexTable + " a, " + schema + profileTable + " b, " + schema + annotTable + " c, " + schema + finalTable + " d "
				+ "where a.profile_id = b.profile_id and a.document_id = c.document_id and a.start <= c.start and a." + rq + "end" + rq + " >= c." + rq + "end" + rq
				+ " and c.annotation_type = '" + annotType + "' and d.profile_id = a.profile_id and d.disabled = 0");
			
			while (rs.next()) {
				int profileID = rs.getInt(1);
				long docID = rs.getLong(2);
				long start = rs.getLong(3);
				long end = rs.getLong(4);
				String profileStr = rs.getString(5);
				
				if (pattRangeMap.get(profileID) == null)
					setPatternRange(profileID, profileStr);
				
				String pattern = getPatternStr(docID, start, end);
				
			
				if (pattern.length() == 0)
					continue;
				
				System.out.println("docID: " + docID + " start: " + start + " end: " + end + " profileID : " + profileID + " profileStr: " + profileStr + " pattern: " + pattern);
				
				
				Integer count = countMap.get(pattern);
				
				if (count == null) {
					count = 0;
				}
				
				countMap.put(pattern, ++count);
				
				List<String> sentList = sentMap.get(pattern);
				if (sentList == null) {
					sentList = new ArrayList<String>();
					sentMap.put(pattern, sentList);
				}
				
				String sentStr = getSentStr(docID, start, end);
				sentList.add(sentStr);
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
				System.out.println(pattList.get(i) + "\ncount: " + countList.get(i) + "\n");
				List<String> sentList = sentMap.get(pattList.get(i));
				for (String sentStr : sentList) {
					System.out.println(sentStr);
				}
				
				System.out.println("\n\n");
			}
			
			
			System.out.println("\n\n\n\nNEGATIVES");
			printNegatives();
			
			conn.close();
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	private void printNegatives() throws SQLException
	{
		Statement stmt = conn.createStatement();
		
		ResultSet rs = stmt.executeQuery("select distinct d.document_id, d.start, d." + rq + "end" + rq 
			+ " from " + schema + indexTable + " d where not exists ("
			+ "select distinct a.document_id, a.start, a." + rq + "end" + rq
			+ " where " + schema + indexTable + " a, " + schema + finalTable + " b, " + schema + profileTable + " c "
			+ "where c.annotation_type = '" + annotType + "' and b.disabled = 0 and b.profile_id = c.profile_id and a.profile_id = b.profile_id "
			+ "and a.document_id = d.document_id and a.start = d.start and a." + rq + "end" + rq + " = d." + rq + "end" + rq
			+ ")");
		
		while (rs.next()) {
			long docID = rs.getLong(1);
			long start = rs.getLong(2);
			long end = rs.getLong(3);
			
			String patternStr = getPatternStr(docID, start, end);
			System.out.println("NEG: " + patternStr);
		}
		
		stmt.close();
	}
	
	private String getPatternStr(long docID, long start, long end) throws SQLException
	{
		StringBuilder strBlder = new StringBuilder();
		//Statement stmt = conn.createStatement();
		
		//int[] ranges = pattRangeMap.get(profileID);
		
		//tokens before target
		pstmt2.setLong(1, docID);
		pstmt2.setLong(2, start);
		pstmt2.setLong(3, end);
		ResultSet rs = pstmt2.executeQuery();
		while (rs.next()) {
			String val = rs.getString(1);
			strBlder.insert(0, val + " ");
		}
		
		//stmt.close();
		
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
	
	private String getSentStr(long docID, long start, long end) throws SQLException
	{
		Statement stmt = conn.createStatement();
		
		ResultSet rs = stmt.executeQuery("select value, start, " + rq + "end" + rq + " from " + schema + annotTable + " where annotation_type = 'Token' "
			+ "and document_id = " + docID + " and start > " + (start - 200) + " and start < " + (start + 200) + " order by start");
		
		StringBuilder strBlder = new StringBuilder();
		
		boolean inAnnot = false;
		while (rs.next()) {
			String tokStr = rs.getString(1);
			long start2 = rs.getLong(2);
			
			if (start2 >= start && start2 < end && !inAnnot) {
				strBlder.append("[");
				inAnnot = true;
			}
			else if (start2 > end && inAnnot) {
				strBlder.append("]");
				inAnnot = false;
			}
				
			
			strBlder.append(tokStr + " ");
		}
		
		
		stmt.close();
		
		return strBlder.toString();
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
