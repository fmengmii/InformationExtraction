package msa.pipeline;

import java.sql.*;

import com.google.gson.Gson;

import utils.db.DBConnection;

abstract public class MSAModule
{
	protected Connection conn;
	protected Connection connDoc;
	protected String schema;
	protected String rq;
	protected Gson gson;
	
	public MSAModule()
	{
		gson = new Gson();
	}
	
	protected void initDB(String user, String password, String host, String dbName, String dbType) throws SQLException, ClassNotFoundException
	{
		conn = DBConnection.dbConnection(user, password, host, dbName, dbType);
		rq = DBConnection.reservedQuote;
	}
	
	protected void initDocDB(String user, String password, String host, String dbName, String dbType) throws SQLException, ClassNotFoundException
	{
		connDoc = DBConnection.dbConnection(user, password, host, dbName, dbType);
	}
	
	abstract public void run();
}
