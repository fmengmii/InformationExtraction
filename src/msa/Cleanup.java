package msa;

import java.sql.*;
import java.util.*;
import java.io.*;

import utils.db.DBConnection;

public class Cleanup
{

	private Connection conn;
	private String schema;
	private String user;
	private String password;
	private String host;
	private String dbName;
	private String dbType;
	
	private String profileTable;
	private String indexTable;
	private String finalTable;
	private int lowFreqDocCount;
	private int minFreqCount;

	
	public Cleanup()
	{
	}
	
	public void init(String config)
	{
		try {
			Properties props = new Properties();
			props.load(new FileReader(config));
			init(props);
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	public void init(Properties props)
	{
		try {
			host = props.getProperty("host");
			dbName = props.getProperty("dbName");
			dbType = props.getProperty("dbType");
			schema = props.getProperty("schema") + ".";
			
			profileTable = props.getProperty("profileTable");
			indexTable = props.getProperty("indexTable");
			finalTable = props.getProperty("finalTable");
			
			minFreqCount = Integer.parseInt(props.getProperty("minFreqCount"));
			lowFreqDocCount = Integer.parseInt(props.getProperty("lowFreqDocCount"));
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	public void cleanup() throws SQLException, ClassNotFoundException
	{
		conn = DBConnection.dbConnection(user, password, host, dbName, dbType);
		Statement stmt = conn.createStatement();
		
		String rq = DBConnection.reservedQuote;
		
		
		ResultSet rs = stmt.executeQuery("select count(distinct document_id) from " + schema + indexTable);
		int docNum = 0;
		if (rs.next())
			docNum = rs.getInt(1);
		
		if (docNum >= lowFreqDocCount) {
			rs = stmt.executeQuery("select distinct profile_id from " + schema + indexTable);
			
			//inactivate low performing and low freqency profiles
			stmt.execute("update " + schema + profileTable + " a, set a.score = -1.0 where a.profile_id in (select b.profile_id from " + schema + finalTable + " b where b.score = 2.0 group by b.profile_id having sum(b.true_pos) < " + minFreqCount + ")");
			stmt.execute("update " + schema + profileTable + " set score = 1.0 where score = 2.0");
			stmt.execute("update " + schema + profileTable + " set score = 2.0 where score = 0.0");
			
			//delete from index table
			stmt.execute("delete from " + schema + indexTable);
		}

		
		stmt.close();
		conn.close();
	}
}
