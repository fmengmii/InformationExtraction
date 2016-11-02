package msa;

import java.util.*;

import align.AnnotationSequenceGrid;
import utils.db.DBConnection;
import java.sql.*;


public class MatchWriter
{
	private Connection conn;
	private PreparedStatement pstmtTarget;
	private int batchSize = 1000;
	
	public MatchWriter()
	{
	}
	
	public void init(String user, String password, String host, String dbName, String dbType, String tableName) throws SQLException, ClassNotFoundException
	{
		conn = DBConnection.dbConnection(user, password, host, dbName, dbType);
		conn.setAutoCommit(false);
		pstmtTarget = conn.prepareStatement("insert into " + tableName + " (profile_id, document_id, start, end, target_id) values (?,?,?,?,?)");
	}
	
	public void write(List<ProfileMatch> matchList) throws SQLException
	{
		int count = 0;
		for (ProfileMatch match : matchList) {
			long docID = match.getSequence().getDocID();
			int[] targetIndexes = match.getTargetIndexes();
			MSAProfile profile = match.getProfile();
			List<MSAProfile> targetList = match.getTargetList();
			
			long profileID = profile.getProfileID();
			
			pstmtTarget.setLong(1, profileID);
			pstmtTarget.setLong(2, docID);
			pstmtTarget.setInt(3, targetIndexes[0]);
			pstmtTarget.setInt(4, targetIndexes[1]);
			
			for (MSAProfile target : targetList) {
				long targetID = target.getProfileID();
				pstmtTarget.setLong(5, targetID);
				pstmtTarget.addBatch();
				count++;
			}
			
			if (count == batchSize) {
				pstmtTarget.executeBatch();
				count = 0;
			}
		}
		
		pstmtTarget.executeBatch();
		conn.commit();
	}
	
	public void close() throws SQLException
	{
		pstmtTarget.close();
		conn.close();
	}
}
