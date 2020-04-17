package msa;

import java.util.*;

import utils.db.DBConnection;
import java.sql.*;

public class ProfileWriter
{
	private Connection conn;
	private PreparedStatement pstmtInsert;
	private PreparedStatement pstmtUpdate;
	private PreparedStatement pstmtExists;
	private String msaTable = "msa_profile";

	
	public ProfileWriter()
	{
	}
	
	public void setMsaTable(String msaTable)
	{
		this.msaTable = msaTable;
	}
	
	public void init(String user, String password, String host, String dbType, String keyspace) throws SQLException, ClassNotFoundException
	{
		conn = DBConnection.dbConnection(user, password, host, keyspace, dbType);
		String rq = DBConnection.reservedQuote;
		pstmtInsert = conn.prepareStatement("insert into " + msaTable + " (profile, annotation_type, " + rq + "group" + rq + ", profile_type, score, true_pos, false_pos, " + rq + "rows" + rq + ") "
			+ "values (?,?,?,?,?,?,?,?)");
		pstmtUpdate = conn.prepareStatement("update " + msaTable + " set score=?,true_pos=?,false_pos=?," + rq + "rows" + rq + "=? where profile=? and annotation_type=? "
			+ "and " + rq + "group" + rq + "=? and profile_type=?");
		pstmtExists = conn.prepareStatement("select count(*) from " + msaTable + " where profile = ? and annotation_type = ? "
				+ "and profile_type = ?");		
	}
	
	public void close() throws SQLException
	{
		pstmtInsert.close();
		pstmtUpdate.close();
		pstmtExists.close();
		conn.close();
	}
	
	public void write(List<MSAProfile> profileList, boolean duplicates) throws SQLException
	{
		//for (MSAProfile profile : profileList) {
		conn.setAutoCommit(false);
		int queryCount = 0;
		for (int i=0; i<profileList.size(); i++) {
			MSAProfile profile = profileList.get(i);
			pstmtExists.setString(1, profile.getProfileStr());
			pstmtExists.setString(2, profile.getAnnotType());
			//pstmtExists.setString(3, profile.getGroup());
			pstmtExists.setInt(3, profile.getType());
			int count = 0;
			ResultSet rs = pstmtExists.executeQuery();
			if (rs.next()) {
				count = rs.getInt(1);
			}
			
			//insert
			if (count == 0 || duplicates) {
				pstmtInsert.setString(1, toSQL(profile.getProfileStr()));
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
					conn.commit();
					queryCount = 0;
				}
					
				
			}
			//update
			else {
				pstmtUpdate.setDouble(1, profile.getScore());
				pstmtUpdate.setInt(2, profile.getTruePos());
				pstmtUpdate.setInt(3, profile.getFalsePos());
				pstmtUpdate.setInt(4, profile.getRows());
				pstmtUpdate.setString(5, toSQL(profile.getProfileStr()));
				pstmtUpdate.setString(6, profile.getAnnotType());
				pstmtUpdate.setString(7, profile.getGroup());
				pstmtUpdate.setInt(8, profile.getType());
				
				pstmtUpdate.addBatch();
				queryCount++;
				if (queryCount == 100) {
					pstmtUpdate.executeBatch();
					conn.commit();
					queryCount = 0;
				}
				
				//pstmtUpdate.execute();
				
				profileList.remove(i);
				i--;
			}
		}
		
		pstmtInsert.executeBatch();
		pstmtUpdate.executeBatch();
		conn.commit();
		conn.setAutoCommit(true);
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
