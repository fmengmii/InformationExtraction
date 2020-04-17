package util;

import java.io.*;
import java.sql.*;
import java.util.*;

import utils.db.DBConnection;

public class CleanDocs
{

	public CleanDocs()
	{
	}
	
	public void clean(String user, String password, String config)
	{
		try {
			Properties props = new Properties();
			props.load(new FileReader(config));
			String host = props.getProperty("host");
			String dbName = props.getProperty("dbName");
			String dbType = props.getProperty("dbType");
			String idCol = props.getProperty("idCol");
			String textCol = props.getProperty("textCol");
			String docTable = props.getProperty("docTable");
			
			Connection conn = DBConnection.dbConnection(user, password, host, dbName, dbType);
			Connection conn2 = DBConnection.dbConnection(user, password, host, dbName, dbType);
			Statement stmt = conn.createStatement();
			PreparedStatement pstmt = conn2.prepareStatement("update " + docTable + " set " + textCol + " = ? where document_id = ?");
			
			conn2.setAutoCommit(false);
			
			int count = 0;
			
			ResultSet rs = stmt.executeQuery("select " + idCol + ", " + textCol + " from " + docTable);
			while (rs.next()) {
				long docID = rs.getLong(1);
				String text = rs.getString(2);
				String text2 = text.trim();
				
				System.out.println("docID: " + docID);
				
				if (!text.equals(text2)) {
					System.out.println("trimmed!");
					pstmt.setString(1, text);
					pstmt.setLong(2, docID);
					pstmt.addBatch();
					count++;
					
					if (count == 100) {
						pstmt.executeBatch();
						conn2.commit();
						count = 0;
					}
				}
			}
			
			pstmt.executeBatch();
			conn2.commit();
			
			conn.close();
			conn2.close();
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
		
		CleanDocs clean = new CleanDocs();
		clean.clean(args[0], args[1], args[2]);
	}
}
