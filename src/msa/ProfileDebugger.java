package msa;

import java.sql.*;
import java.util.*;
import java.io.*;

import utils.db.DBConnection;


public class ProfileDebugger
{
	private Connection conn;
	private String profileStr;
	private String targetStr;
	private long profileID;
	private String value;
	private String profileTable;
	private String indexTable;
	private String provenance;
	
	public ProfileDebugger()
	{
	}
	
	public void init(String user, String password, String config)
	{
		try {
			Properties props = new Properties();
			props.load(new FileReader(config));
			
			String host = props.getProperty("host");
			String dbName = props.getProperty("dbName");
			String dbType = props.getProperty("dbType");
			
			profileStr = props.getProperty("profileStr");
			targetStr = props.getProperty("targetStr");
			profileID = -1;
			String profileIDStr = props.getProperty("profileID");
			if (profileIDStr != null)
				profileID = Long.parseLong(profileIDStr);
			
			value = props.getProperty("value");
			profileTable = props.getProperty("profileTable");
			indexTable = props.getProperty("indexTable");
			provenance = props.getProperty("provenance");
			
			conn = DBConnection.dbConnection(user, password, host, dbName, dbType);	
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	public void debug()
	{
		try {
			//conn = DBConnection.dbConnection(user, password, host, dbName, dbType);

			PreparedStatement pstmtTarget = conn.prepareStatement("select profile from " + profileTable + " where profile_id = ?");
			PreparedStatement pstmtSent = conn.prepareStatement("select value, start, end from annotation where document_id = ? and "
				+ "start >= ?  and end <= ? and annotation_type = 'Token' order by start");
			PreparedStatement pstmtAns = conn.prepareStatement("select annotation_type from annotation where document_id = ? and start = ? and end = ? and provenance = '" + provenance + "'");
			
			Statement stmt = conn.createStatement();
			
			if (profileID >= 0) {
				ResultSet rs = stmt.executeQuery("select profile from " + profileTable + " where profile_id = " + profileID);
				if (rs.next()) {
					profileStr = rs.getString(1);
				}
			}
			
			
			System.out.println("profile: " + profileStr);
			
			if (profileID == -1) {
				profileID = -1;
				ResultSet rs = stmt.executeQuery("select profile_id from " + profileTable + " where profile like '" + profileStr + "' and profile_type = 0");
				if (rs.next()) {
					profileID = rs.getLong(1);
				}
			}
			
			long targetID = -1;
			if (targetStr != null) {
				ResultSet rs = stmt.executeQuery("select profile_id from " + profileTable + " where profile like '" + targetStr + "' and profile_type = 1");
				if (rs.next()) {
					targetID = rs.getLong(1);
				}
			}
			
			Map<String, Integer> annotTypeMap = new HashMap<String, Integer>();
			String queryStr = "select target_id, document_id, start, end from " + indexTable + " where profile_id = " + profileID;
			if (targetID >= 0)
				queryStr += " and target_id = " + targetID;
			ResultSet rs = stmt.executeQuery(queryStr);
			while (rs.next()) {
				targetID = rs.getLong(1);
				long docID = rs.getLong(2);
				long start = rs.getLong(3);
				long end = rs.getLong(4);
				
				targetStr = "";
				pstmtTarget.setLong(1, targetID);
				ResultSet rs2 = pstmtTarget.executeQuery();
				if (rs2.next()) {
					targetStr = rs2.getString(1);
				}
				
				
				pstmtSent.setLong(1, docID);
				pstmtSent.setLong(2, start);
				pstmtSent.setLong(3, end);
				rs2 = pstmtSent.executeQuery();
				if (rs2.next()) {
					String value2 = rs2.getString(1);
					System.out.println(value2);
					if (value != null && !value2.toLowerCase().equals(value.toLowerCase()))
						continue;
				}
				
				System.out.println("\n\n" + targetStr + "|" + docID + "|" + start + "|" + end);
				
				pstmtSent.setLong(2, start - 50);
				pstmtSent.setLong(3, end + 50);
				rs2 = pstmtSent.executeQuery();
				
				while (rs2.next()) {
					String value2 = rs2.getString(1);
					long start2 = rs2.getLong(2);
					long end2 = rs2.getLong(3);
					

					
					if (start2 == start) {
						System.out.print(" [ ");
					}
					
					System.out.print(start2 + "|" + end2 + "|" + value2);
					
					if (start2 == start)
						System.out.print(" ] ");
					
					System.out.print(" ");
				}
				
				System.out.println();
				
				String annotType = "";
				pstmtAns.setLong(1, docID);
				pstmtAns.setLong(2, start);
				pstmtAns.setLong(3, end);
				rs2 = pstmtAns.executeQuery();
				if (rs2.next())
					annotType = rs2.getString(1);
				
				String key = targetStr + "|" + annotType;
				Integer count = annotTypeMap.get(key);
				if (count == null)
					count = 0;
				annotTypeMap.put(key, ++count);
			}
			
			for (String key : annotTypeMap.keySet()) {
				int count = annotTypeMap.get(key);
				System.out.println(key + " : " + count);
			
			}
			
			stmt.close();
			pstmtTarget.close();
			pstmtSent.close();
			conn.close();
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args)
	{
		if (args.length != 3) {
			System.out.println("usage: user password config");
			System.exit(0);
		}
		
		
		ProfileDebugger debug = new ProfileDebugger();
		debug.init(args[0], args[1], args[2]);
		//debug.debug("[\":target\",\":i-per\",\":i-per\"]");
		//debug.debug("[\":token|string|the!:token|root|the!:token|orth|lowercase!:token|category|dt!:syntaxtreenode|cat|dt\",\":target\"]");
		
		//debug.debug("[\":start|start\",\":token|orth|upperinitial!:syntaxtreenode|cat|np\",\":token|string|-!:token|root|-!:token|category|:!:syntaxtreenode|cat|:\",\":target\"]");
		
		//debug.debug("[\":token|orth|lowercase!:token|category|nn!:lookup|majortype|jobtitle\",\":token|orth|upperinitial!:token|category|nnp!:syntaxtreenode|cat|nnp\",\":target\"]");
		
		//debug.debug("[\":token|string|m!:token|root|m!:token|orth|upperinitial!:token|category|nnp!:syntaxtreenode|cat|nnp\",\":token|string|.!:token|root|.!:token|category|.!:syntaxtreenode|cat|.\",\":target\"]");
		//debug.debug("[\":target\",\":i-per\",\":i-per\"]", "", -1, "general");
		//debug.debug("", "", 15, null, "msa_profile_not_per", "msa_profile_match_index_not_per", "conll2003-not-per-token");
		debug.debug();

		//debug.debug("[\":target\",\":token|string|0!:token|root|0!:token|category|cd!:number|value|0.0!:number|number!:syntaxtreenode|cat|cd\",\":end|end\"]");
		//debug.debug("[\":syntaxtreenode|cat|np\",\":token|orth|lowercase!:syntaxtreenode|cat|vbd\",\":target\",\":i-per\"]", "fmeng", "fmeng", "10.9.94.203", "msa", "mysql");
		//debug.debug("[\":token|string|(!:token|root|(!:token|category|(!:syntaxtreenode|cat|-lrb-\",\":syntaxtreenode|cat|np\",\":token|string|,!:token|root|,!:token|category|,!:syntaxtreenode|cat|,\",\":target\"]");
	}
}
