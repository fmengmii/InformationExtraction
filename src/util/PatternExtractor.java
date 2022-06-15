package util;

import java.io.*;
import java.sql.*;
import java.util.*;

import utils.db.DBConnection;

public class PatternExtractor 
{
	private Connection conn;
	
	private PreparedStatement pstmt;
	

	public PatternExtractor()
	{
	}
	
	public void extract(String user, String password, String config)
	{
		try {
			List<String> totalPhrases = new ArrayList<String>();
			
			Properties props = new Properties();
			props.load(new FileReader(config));
			String host = props.getProperty("host");
			String dbName = props.getProperty("dbName");
			String dbType = props.getProperty("dbType");
			String schema = props.getProperty("schema");
			String finalTable = props.getProperty("finalTable");
			String annotType = props.getProperty("annotType");
			
			conn = DBConnection.dbConnection(user, password, host, dbName, dbType);
			
			pstmt = conn.prepareStatement("select ");
			
			Statement stmt = conn.createStatement();
			
			ResultSet rs = stmt.executeQuery("select a.profile_id, b.profile from " + schema + finalTable + "a, " + schema + "profile b "
				+ "where a.annotation_type = '" + annotType + "' and a.disabled = 0");
			
			while (rs.next()) {
				int profileID = rs.getInt(1);
				String profileStr = rs.getString(2);
				
				List<String> phrases = getPhrases(profileID);
				totalPhrases.addAll(phrases);
			}
			
			for (String phrase: totalPhrases)
				System.out.println(phrase);
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	private List<String> getPhraess(int profileID) throws SQLException
	{
		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery("");
		
		stmt.close();
	}
}
