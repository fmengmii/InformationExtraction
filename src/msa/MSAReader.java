package msa;

import java.util.*;
import java.sql.*;

import nlputils.sequence.SequenceUtilities;
import utils.db.DBConnection;



import com.google.gson.Gson;

public class MSAReader
{
	private Gson gson;
	private Connection conn;
	private String rq;
	private String dbType;

	public MSAReader()
	{
		gson = new Gson();
	}
	
	public List<MultipleSequenceAlignment> read(String msaTable) throws SQLException
	{
		return read(msaTable, null, null, null, -1);
	}
	
	public void close() throws SQLException
	{		
		if (conn != null)
			conn.close();
	}
	
	public List<MultipleSequenceAlignment> read(String user, String password, String host, String dbType, String keyspace, String msaTable, String group, 
		String docNamespace, String docTable, int userMSAID) throws SQLException, ClassNotFoundException
	{
		this.dbType = dbType;
		
		if (dbType.equals("mysql")) {
			conn = DBConnection.dbConnection(user, password, host, keyspace, dbType);
			rq = DBConnection.reservedQuote;
		}
		
		return read(msaTable, group, docNamespace, docTable, userMSAID);
	}
	
	public List<MultipleSequenceAlignment> read(String msaTable, String group, String docNamespace, String docTable, int userMSAID) throws SQLException
	{
		List<MultipleSequenceAlignment> msaList = new ArrayList<MultipleSequenceAlignment>();
		
		List<List<Object>> rowList = new ArrayList<List<Object>>();
		if (dbType.equals("mysql")) {
			String queryStr = "select msa_id, " + rq + "group" + rq + ", base_tokens, filler_tokens, multiplicity from " + msaTable + " where document_namespace = '" + docNamespace + "' and "
					+ "document_table = '" + docTable + "'";
				if (group != null) {
					queryStr += " and group = '" + group + "'";
				}
				
				if (userMSAID >= 0)
					queryStr += " and msa_id = " + userMSAID;
			Statement stmt = conn.createStatement();
			java.sql.ResultSet rs = stmt.executeQuery(queryStr);
			
			while (rs.next()) {
				int msaID = rs.getInt(1);
				String group2 = rs.getString(2);
				String baseToksStr = rs.getString(3);
				String fillerToksStr = rs.getString(4);
				int mult = rs.getInt(5);
				
				List<Object> row = new ArrayList<Object>();
				row.add(msaID);
				row.add(group2);
				row.add(baseToksStr);
				row.add(fillerToksStr);
				row.add(mult);
				rowList.add(row);
			}
			
			stmt.close();
			conn.close();
		}
		
		int currMSAID = -1;
		String currGroup = "";
		MultipleSequenceAlignment msa = null;

		for (List<Object> row  : rowList) {
			int msaID = (int) row.get(0);
			String group2 = (String) row.get(1);
			String baseToksStr = (String) row.get(2);
			String fillerToksStr = (String) row.get(3);
			int mult = (int) row.get(4);
			
			List<Map<String, Object>> baseToks = new ArrayList<Map<String, Object>>();
			baseToks = gson.fromJson(baseToksStr, baseToks.getClass());
			List<Map<String, Object>> fillerToks = new ArrayList<Map<String, Object>>();
			fillerToks = gson.fromJson(fillerToksStr, fillerToks.getClass());
			
			List<String> baseTokens = new ArrayList<String>();
			for (Map<String, Object> map : baseToks) {
				List<String> toksList = (List<String>) map.get("tokens");
				StringBuilder strBlder = new StringBuilder();
				for (String tok : toksList) {
					strBlder.append(tok + " ");
				}
				
				baseTokens.add(strBlder.toString().trim());
			}
			
			List<String> fillerTokens = new ArrayList<String>();
			for (Map<String, Object> map : fillerToks) {
				List<String> toksList = (List<String>) map.get("tokens");
				StringBuilder strBlder = new StringBuilder();
				for (String tok : toksList) {
					strBlder.append(tok + " ");
				}
				
				fillerTokens.add(strBlder.toString().trim());
			}
			
			
			MSARowBean rowBean = new MSARowBean(baseTokens, fillerTokens);
			
			if (msaID != currMSAID || !group.equals(currGroup)) {
				currMSAID = msaID;
				currGroup = group2;
				//System.out.println("msaID: " + msaID + ", group: " + group2);
				
				if (msa != null) {
					msaList.add(msa);
				}
				
				msa = new MultipleSequenceAlignment();
				msa.setID(msaID);
			}
			
			msa.addRow(rowBean, mult);
		}
		
		if (msa != null) {
			msaList.add(msa);
		}
		
		return msaList;
	}
	
	public List<MSAProfile> readProfiles(String docNamespace, String docTable, String group) throws SQLException
	{
		
		List<MSAProfile> profileList = new ArrayList<MSAProfile>();
		
		//Statement stmt = conn.createStatement();
		

		/*
		ResultSet rs = stmt.executeQuery("select msa_id, profile from msa_profile where document_namespace = '" + docNamespace + "' and document_table = '" + docTable + "'"
				+ " and group = '" + group);
		//Iterator<Row> iter = rs.iterator();
		//for (;iter.hasNext();) {
		while (rs.next()) {
			//Row row = iter.next();
			//int msaID = row.getInt(0);
			//String profileStr = row.getString(1);
			
			int msaID = rs.getInt(1);
			String profileStr = rs.getString(2);
			
			MSAProfile profile = new MSAProfile(msaID, profileStr);
			profileList.add(profile);
		}
		*/
		
		return profileList;
	}
	
	public static void main(String[] args)
	{
		try {
			MSAReader reader = new MSAReader();
			List<MultipleSequenceAlignment> msaList = reader.read("cassandra", "cassandra", "192.99.100.31", "cassandra", "msa", "msa_row", "lungrads-target-number", 
				"radiology", "lung_cancer_screening_report", -1);
			//List<MultipleSequenceAlignment> msaList = reader.read("fmeng", "fmeng", "localhost", "mysql", "msa", "msa_row", -1);
			
			int count = 0;
			for (MultipleSequenceAlignment msa : msaList) {
				String profileStr = msa.toProfileString(true);
				List<String> toks = SequenceUtilities.getToksFromStr(profileStr);
				/*
				if ((toks.contains("mass") || toks.contains("masses") || toks.contains("nodule") || toks.contains("nodules") ||
						toks.contains("micronodule") || toks.contains("micronodules") || 
						toks.contains("macronodule") || toks.contains("macronodules") ||
						toks.contains("lesion") || toks.contains("lesions")) && toks.contains("#{\"AnnotationType\":\"TumorLocationAuto\"}#")) {
						*/
				//if (toks.contains("#{\"AnnotationType\":\"TumorLocationAuto\"}#")) {
				
					System.out.println("MSA ID: " + msa.getID());
					System.out.println(msa.toProfileString(true));
					System.out.println(msa.toString() + "\n");
					count++;
				//}
				
					/*
					if (count > 100)
						break;
						*/
			}
			
			System.out.println("count=" + count);
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
}
