package util;

import java.io.*;
import java.sql.*;

import utils.db.DBConnection;

public class ImportTextFiles
{
	private int currentDocID = 1;
	private Connection conn;
	
	public ImportTextFiles()
	{
	}
	
	public void importFiles(String user, String password, String host, String dbName, String dbType, String folderName, String tableName, String insertSQLString)
	{
		try {
			conn = DBConnection.dbConnection(user, password, host, dbName, dbType);
			conn.setAutoCommit(false);
			PreparedStatement pstmt = conn.prepareStatement(insertSQLString);
			
			getLastDocID(tableName);
			
			File folder = new File(folderName);
			String[] files = folder.list();
			int count = 0;
			for (int i=0; i<files.length; i++) {
				if (!files[i].endsWith(".txt"))
					continue;
				
				BufferedReader reader = new BufferedReader(new FileReader(folderName + "/" + files[i]));
				
				String line = "";
				StringBuilder strBlder = new StringBuilder();
				while ((line = reader.readLine()) != null) {
					strBlder.append(line + "\n");
				}
				
				
				reader.close();
				
				int docID = currentDocID;
				try {
					int index = files[i].indexOf(".");
					String rootName = files[i].substring(0, index);
					docID = Integer.parseInt(rootName);
				}
				catch(NumberFormatException e)
				{
					currentDocID++;
				}
				
				String docText = strBlder.toString().trim().replaceAll("\\r", "");
				
				pstmt.setInt(1, docID);
				pstmt.setString(2, files[i]);
				//pstmt.setString(3, "smith");
				//pstmt.setInt(4, i+54);
				//pstmt.setString(3, "testb");
				pstmt.setString(3, docText);
				pstmt.addBatch();
				
				System.out.println("Importing " + files[i]);
				
				if (count % 1000 == 0) {
					pstmt.executeBatch();
					conn.commit();
				}
				
				count++;
			}
			
			pstmt.executeBatch();
			conn.commit();
			pstmt.close();
			conn.close();			
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	private void getLastDocID(String tableName) throws SQLException
	{
		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery("select max(document_id) from " + tableName);
		if (rs.next())
			currentDocID = rs.getInt(1) + 1;
			
		stmt.close();
	}
	
	public static void main(String[] args)
	{
		if (args.length != 8) {
			System.out.println("usage: <user> <password> <host> <dbname> <dbtype> <folder name> <table name> <insert SQL string>");
			System.exit(0);
		}
		
		ImportTextFiles importText = new ImportTextFiles();
		importText.importFiles(args[0], args[1], args[2], args[3], args[4], args[5], args[6], args[7]);
		//importText.importFiles("fmeng", "fmeng", "localhost", "nlp", "/Users/frankmeng/Documents/Research/i2b2/i2b2_obesity/docs", "insert into i2b2_obesity_documents (document_id, name, text) values (?,?,?)");
	}
}
