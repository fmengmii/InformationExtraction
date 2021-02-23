package util;

import java.io.*;
import java.sql.*;
import java.util.*;

import com.google.gson.Gson;

import utils.db.DBConnection;


public class DisabledDocStats
{
	private Connection conn;
	private PreparedStatement pstmt;
	private String targetType;
	private Map<String, Integer> countMap;
	private int targetTotal;
	private Gson gson;

	public DisabledDocStats()
	{
		gson = new Gson();
	}
	
	public void run(String user, String password, String config)
	{
		try {
			Properties props = new Properties();
			props.load(new FileReader(config));
			
			String host = props.getProperty("host");
			String dbName = props.getProperty("dbName");
			String dbType = props.getProperty("dbType");
			String project = props.getProperty("project");
			String docNamespace = props.getProperty("docNamespace");
			String docTable = props.getProperty("docTable");
			String schema = props.getProperty("schema") + ".";
			List<String> annotTypeList = new ArrayList<String>();
			targetType = props.getProperty("targetType");
			annotTypeList = gson.fromJson(props.getProperty("annotTypeList"), annotTypeList.getClass());
			
			countMap = new HashMap<String, Integer>();
			targetTotal = 0;
			
			conn = DBConnection.dbConnection(user, password, host, dbName, dbType);
			String rq = DBConnection.reservedQuote;
			
			StringBuffer strBuf = new StringBuffer();
			for (String annotType : annotTypeList) {
				if (strBuf.length() > 0)
					strBuf.append(", ");
				strBuf.append("'" + annotType + "'");
			}
			
			strBuf.insert(0, "(");
			strBuf.append(", '" + targetType + "')");
			
			pstmt = conn.prepareStatement("select start, " + rq + "end" + rq + ", annotation_type from " + schema + "annotation where document_namespace = ? and document_table = ? "
				+ "and document_id = ? and annotation_type in " + strBuf.toString() + " order by start");
			pstmt.setString(1, docNamespace);
			pstmt.setString(2, docTable);
			
			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery("select distinct a.document_id, a.disabled from " + schema + "frame_instance_document a, " + schema + "project_frame_instance b, "
				+ schema + "project c, " + schema + "annotation d "
				+ "where a.document_id = b.document_id and a.document_table = b.document_table and a.document_namespace = b.document_namespace "
				+ "and a.document_namespace = '" + docNamespace + "' and a.document_table = '" + docTable + "' and a.disabled = 1 "
				+ "and c.name = '" + project + "' and c.project_id = b.project_id and d.annotation_type = '" + targetType + "' "
				+ "and d.document_id = a.document_id and d.document_namespace = a.document_namespace and d.document_table = a.document_table "
				+ "order by a.document_id");
			
			while (rs.next()) {
				long docID = rs.getLong(1);
				processDoc(docID);
			}
			
			for (String annotTypes : countMap.keySet()) {
				int count = countMap.get(annotTypes);
				System.out.println(annotTypes + ": " + count + ", " + ((double) count) / ((double) targetTotal));
			}
			
			System.out.println("Total targets: " + targetTotal);
			
			stmt.close();
			conn.close();
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	private void processDoc(long docID) throws SQLException
	{
		pstmt.setLong(3, docID);
		
		Map<String, Long> startMap = new HashMap<String, Long>();
		Map<String, Long> endMap = new HashMap<String, Long>();
		Map<Long, List<String>> targetPosMap = new HashMap<Long, List<String>>();
		
		List<Long> targetPosList = new ArrayList<Long>();
		
		//int targetTotal = 0;
		
		ResultSet rs = pstmt.executeQuery();
		while (rs.next()) {
			long start = rs.getLong(1);
			long end = rs.getLong(2);
			String annotType = rs.getString(3);
			
			Long currStart = startMap.get(annotType);
			Long currEnd = endMap.get(annotType);
			
			if (currStart == null)
				currStart = (long) -1;
			if (currEnd == null)
				currEnd = (long) -1;
			
			if (start > currEnd + 1) {
				startMap.put(annotType, start);
				currStart = start;
			}

			if (end > currEnd) {
				endMap.put(annotType, end);
				currEnd = end;
			}
			
			if (annotType.equals(targetType)) {
				targetTotal++;
				targetPosList.add(start);
				
				for (String annotType2 : startMap.keySet()) {
					if (annotType2.equals(targetType))
						continue;
					
					Long start2 = startMap.get(annotType2);
					Long end2 = endMap.get(annotType2);
					
					if (currStart >= start2 && currEnd <= end2) {
						List<String> annotList = targetPosMap.get(start);
						if (annotList == null) {
							annotList = new ArrayList<String>();
							targetPosMap.put(start, annotList);
						}
						
						if (!annotList.contains(annotType2)); {	
							annotList.add(annotType2);
							
							/*
							Integer count = countMap.get(annotType2);
							if (count == null)
								count = 0;
							countMap.put(annotType2, ++count);
							*/
						}
					}
				}
			}
			else {
				Long start2 = startMap.get(targetType);
				Long end2 = endMap.get(targetType);
				
				if (currStart <= start2 && currEnd >= end2) {
					List<String> annotList = targetPosMap.get(start2);
					if (annotList == null) {
						annotList = new ArrayList<String>();
						targetPosMap.put(start2, annotList);
					}
					
					if (!annotList.contains(annotType)) {
						annotList.add(annotType);
						
						/*
						Integer count = countMap.get(annotType);
						if (count == null)
							count = 0;
						countMap.put(annotType, ++count);
						*/
					}
				}
			}
		}
		
		//tally stats
		boolean empty = false;
		for (long pos : targetPosList) {
			List<String> annotTypeList = targetPosMap.get(pos);
			if (annotTypeList == null) {
				empty = true;
			}
			else {
				annotTypeList.sort(null);;
				StringBuffer strBuf = new StringBuffer();
				for (String annotType : annotTypeList) {
					strBuf.append(annotType + "|");
				}
				
				Integer count = countMap.get(strBuf.toString());
				if (count == null) {
					count = 0;
				}
				
				countMap.put(strBuf.toString(), ++count);
			}
		}
	}
	
	public static void main(String[] args)
	{
		if (args.length != 3) {
			System.out.println("usage: user password config");
			System.exit(0);
		}
		
		DisabledDocStats docStats = new DisabledDocStats();
		docStats.run(args[0], args[1], args[2]);
	}
}
