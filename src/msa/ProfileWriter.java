package msa;

import java.util.*;

import utils.db.DBConnection;
import java.sql.*;

public class ProfileWriter
{
	private Connection conn;
	private Connection conn2;
	private PreparedStatement pstmtInsert;
	private PreparedStatement pstmtUpdate;
	private PreparedStatement pstmtExists;
	private String msaTable = "msa_profile";
	
	private Map<String, Boolean> profileMap;

	
	public ProfileWriter()
	{
		profileMap = new HashMap<String, Boolean>();
	}
	
	public void setMsaTable(String msaTable)
	{
		this.msaTable = msaTable;
	}
	
	public void init(String user, String password, String host, String dbType, String keyspace, String annotType) throws SQLException, ClassNotFoundException
	{
		conn = DBConnection.dbConnection(user, password, host, keyspace, dbType);
		conn2 = DBConnection.dbConnection(user, password, host, keyspace, dbType);
		String rq = DBConnection.reservedQuote;
		pstmtInsert = conn2.prepareStatement("insert into " + msaTable + " (profile, annotation_type, " + rq + "group" + rq + ", profile_type, score, true_pos, false_pos, " + rq + "rows" + rq + ") "
			+ "values (?,?,?,?,?,?,?,?)");
		pstmtUpdate = conn2.prepareStatement("update " + msaTable + " set score=?,true_pos=?,false_pos=?," + rq + "rows" + rq + "=? where profile=? and annotation_type=? "
			+ "and " + rq + "group" + rq + "=? and profile_type=?");
		pstmtExists = conn.prepareStatement("select count(*) from " + msaTable + " where profile = ? and annotation_type = ? "
				+ "and profile_type = ?");

		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery("select distinct profile from " + msaTable 
			+ " where annotation_type = '" + annotType + "'");
		
		while (rs.next()) {
			String profileStr = rs.getString(1);
			Boolean flag = profileMap.get(profileStr);
			if (flag == null)
				profileMap.put(profileStr, true);
		}
	}
	
	public void close() throws SQLException
	{
		pstmtInsert.close();
		pstmtUpdate.close();
		pstmtExists.close();
		conn.close();
		conn2.close();
	}
	
	public void write(List<MSAProfile> profileList, boolean duplicates) throws SQLException
	{
		//for (MSAProfile profile : profileList) {
		conn2.setAutoCommit(false);
		int queryCount = 0;
		Map<String, Boolean> usedMap = new HashMap<String, Boolean>();
		
		for (int i=0; i<profileList.size(); i++) {
			MSAProfile profile = profileList.get(i);
			
			/*
			pstmtExists.setString(1, profile.getProfileStr());
			pstmtExists.setString(2, profile.getAnnotType());
			//pstmtExists.setString(3, profile.getGroup());
			pstmtExists.setInt(3, profile.getType());
			int count = 0;
			ResultSet rs = pstmtExists.executeQuery();
			if (rs.next()) {
				count = rs.getInt(1);
			}
			*/
			
			Boolean flag = profileMap.get(profile.getProfileStr());
			if (flag != null)
				continue;
			else
				profileMap.put(profile.getProfileStr(), true);
			
			flag = usedMap.get(profile.getProfileStr());
			if (flag != null)
				continue;
			
			usedMap.put(profile.getProfileStr(), true);
			
			//insert
			if (flag == null || duplicates) {
				pstmtInsert.setString(1, profile.getProfileStr());
				pstmtInsert.setString(2, profile.getAnnotType());
				pstmtInsert.setString(3, profile.getGroup());
				pstmtInsert.setInt(4, profile.getType());
				pstmtInsert.setDouble(5, profile.getScore());
				pstmtInsert.setInt(6, profile.getTruePos());
				pstmtInsert.setInt(7, profile.getFalsePos());
				pstmtInsert.setInt(8, profile.getRows());
				
				//System.out.println("query: " + pstmtInsert.toString());
				
				//pstmtInsert.execute();
				pstmtInsert.addBatch();
				queryCount++;
				if (queryCount == 100) {
					pstmtInsert.executeBatch();
					conn2.commit();
					queryCount = 0;
				}
					
				
			}
			//update
			/*
			else {
				pstmtUpdate.setDouble(1, profile.getScore());
				pstmtUpdate.setInt(2, profile.getTruePos());
				pstmtUpdate.setInt(3, profile.getFalsePos());
				pstmtUpdate.setInt(4, profile.getRows());
				pstmtUpdate.setString(5, profile.getProfileStr());
				pstmtUpdate.setString(6, profile.getAnnotType());
				pstmtUpdate.setString(7, profile.getGroup());
				pstmtUpdate.setInt(8, profile.getType());
				
				pstmtUpdate.addBatch();
				queryCount++;
				if (queryCount == 100) {
					pstmtUpdate.executeBatch();
					conn2.commit();
					queryCount = 0;
				}
				
				//pstmtUpdate.execute();
				
				profileList.remove(i);
				i--;
			}
			*/
		}
		
		pstmtInsert.executeBatch();
		//pstmtUpdate.executeBatch();
		conn2.commit();
	}
	
	private String toSQL(String str)
	{
		StringBuilder strBlder = new StringBuilder();
		for (int i=0; i<str.length(); i++) {
			if (str.charAt(i) == '\'')
				strBlder.append("''");
			else
				strBlder.append(str.charAt(i));
		}
		
		return strBlder.toString();
	}
}
