package msa.db;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.google.gson.Gson;

import msa.Annotation;
import msa.AnnotationSequence;
import msa.MSAProfile;
import msa.MSARow;
import msa.MSAToken;
import msa.MultipleSequenceAlignment;
import utils.db.DBConnection;

public class MySQLDBInterface implements MSADBInterface
{
	private Connection conn;
	private Connection msaConn;
	private Gson gson;
	
	private String docNamespace;
	private String docTable;
	private String group;
	private String provenance;
	private String rq;
	
	private PreparedStatement pstmtMSAInsert;
	private PreparedStatement pstmtMSARowInsert;
	private PreparedStatement pstmtProfileInsert;
	private PreparedStatement pstmtSents;
	private PreparedStatement pstmtSentAnnots;
	private PreparedStatement pstmtAnnotText;
	private PreparedStatement pstmtAnnotInsert;
	private PreparedStatement pstmtDeleteProfiles;
	private PreparedStatement pstmtReadProfile;
	private PreparedStatement pstmtUpdateProfileScore;
	private String dbType = "mysql";
	private String schema = "";
	
	
	public MySQLDBInterface()
	{
		gson = new Gson();
	}
	
	public void setGroup(String group)
	{
		this.group = group;
	}
	
	public void setDBType(String dbType)
	{
		this.dbType = dbType;
	}
	
	public void setSchema(String schema)
	{
		this.schema = schema + ".";
	}
	
	public void init(String user, String password, String host, String keyspace, String msaKeyspace) throws MSADBException
	{
		try {
			conn = DBConnection.dbConnection(user, password, host, keyspace, dbType);
			
			if (msaKeyspace != null)
				msaConn = DBConnection.dbConnection(user, password, host, msaKeyspace, dbType);
			
			rq = DBConnection.reservedQuote;
			
			/*
			docNamespace = props.getProperty("docNamespace");
			docTable = props.getProperty("docTable");
			group = props.getProperty("group");
			String provenance = props.getProperty("provenance");
			*/
			
			if (msaKeyspace != null) {
				pstmtMSAInsert = msaConn.prepareStatement("insert into msa (msa_id, document_namespace, document_table, " + rq + "group" + rq + ") values (?,?,?,?)");
				pstmtMSARowInsert = msaConn.prepareStatement("insert into msa_row (msa_id, document_namespace, document_table, " + rq + "group" + rq + ", row_id, base_tokens, filler_tokens, multiplicity, sentences) values (?,?,?,?,?,?,?,?,?)");
				pstmtProfileInsert = msaConn.prepareStatement("insert into msa_profile (msa_id, document_namespace, document_table, annotation_type, `group`, profile_type, name, profile, score) values (?,?,?,?,?,?,?,?,?)");
				pstmtDeleteProfiles = msaConn.prepareStatement("delete from msa_profile where annotation_type = ? and `group` = ?");
				pstmtReadProfile = msaConn.prepareStatement("select score from msa_profile where " + rq + "group" + rq + " = ? and profile = ? and profile_type = ? and annotation_type = ?");
				pstmtUpdateProfileScore = msaConn.prepareStatement("update msa_profile set score = ? where document_namespace = ? and document_table = ? and " + rq + "group" + rq + " = ? and profile = ? and profile_type = ? and annotation_type = ?");

			}

			pstmtSents = conn.prepareStatement("select a.start, a." + rq + "end" + rq + ", a.id from " + schema + "annotation a, " + schema + "annotation b "
				+ "where a.document_namespace = ? and a.document_table = ? "
				+ "and a.document_id = ? and a.annotation_type = ? and b.document_namespace = a.document_namespace and b.document_table = a.document_table "
				+ "and b.annotation_type = ? and a.document_id = b.document_id "
				+ "and a.start <= b.start and a." + rq + "end" + rq +" >= b." + rq + "end" + rq
				+ "order by a.start");
			
			pstmtSentAnnots = conn.prepareStatement("select document_namespace, document_table, document_id, id, annotation_type, start, " + rq + "end" + rq + ", value, features, provenance "
					+ "from " + schema + "annotation where document_namespace = ? and document_table = ? and "
					+ "document_id = ? and start >= ? and start < ? order by start");
			
			pstmtAnnotInsert = conn.prepareStatement("insert into " + schema + "annotation (id, document_namespace, document_table, document_id, annotation_type, start, " + rq + "end" + rq + ", features, score, value, provenance) values (?,?,?,?,?,?,?,?,?,?,?)");

			
		}
		catch(SQLException e)
		{
			throw new MSADBException(e);
		}
		catch(ClassNotFoundException e)
		{
			throw new MSADBException(e);
		}
	}
	
	public List<Long> getDocIDList(String docQuery) throws MSADBException
	{
		List<Long> docIDList = new ArrayList<Long>();

		try {
			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(docQuery);
			while (rs.next()) {
				long docID = rs.getLong(1);
				docIDList.add(docID);
			}
			
			stmt.close();
		}
		catch(SQLException e)
		{
			throw new MSADBException(e);
		}
		
		return docIDList;
	}
	
	
	public List<AnnotationSequence> getSentsInDoc(String docNamespace, String docTable, long docID, String targetType) throws MSADBException
	{
		return getSentsInDoc(docNamespace, docTable, docID, "Sentence", targetType);
	}
	
	
	public List<AnnotationSequence> getSentsInDoc(String docNamespace, String docTable, long docID, String sentType, String targetType) throws MSADBException
	{
		System.out.println("Doc: " + docID);

		List<AnnotationSequence> seqList = new ArrayList<AnnotationSequence>();
			
		try {
			pstmtSents.setString(1, docNamespace);
			pstmtSents.setString(2, docTable);
			pstmtSents.setLong(3, docID);
			pstmtSents.setString(4, sentType);
			pstmtSents.setString(5, targetType);
			
			
			System.out.println("select a.start, a." + rq + "end" + rq + ", a.id from " + schema + "annotation a, " + schema + "annotation b "
				+ "where a.document_namespace = '" + docNamespace + "' and a.document_table = '" + docTable + "' "
				+ "and a.document_id = " + docID + " and a.annotation_type = '" + sentType + "' and b.document_namespace = a.document_namespace "
				+ "and b.document_table = a.document_table "
				+ "and b.annotation_type = '" + targetType + "' and a.document_id = b.document_id "
				+ "and a.start <= b.start and a." + rq + "end" + rq +" >= b." + rq + "end" + rq
					+ "order by a.start");
			
			
			ResultSet rs = pstmtSents.executeQuery();
			while (rs.next()) {
				int start = rs.getInt(1);
				int end = rs.getInt(2);
				int sentID = rs.getInt(3);
				
				AnnotationSequence seq = new AnnotationSequence();
				seq.setDocID(docID);
				seq.setSentID(sentID);
				seq.setStart(start);
				seq.setEnd(end);
				
				seqList.add(seq);
			}
		}
		catch(SQLException e)
		{
			throw new MSADBException(e);
		}
		
		return seqList;
	}
	
	public List<Annotation> getSentAnnots(String docNamespace, String docTable, long docID, int start, int end, boolean punct) throws MSADBException
	{
		//System.out.println("start getSentAnnots");
		List<Annotation> annotList = new ArrayList<Annotation>();
		
		try {
			pstmtSentAnnots.setString(1, docNamespace);
			pstmtSentAnnots.setString(2, docTable);
			pstmtSentAnnots.setLong(3, docID);
			pstmtSentAnnots.setInt(4, start);
			pstmtSentAnnots.setInt(5, end);
	
			//System.out.println("start query: " + pstmtSentAnnots.toString());
			ResultSet rs = pstmtSentAnnots.executeQuery();
			//System.out.println("end query");
			
			while (rs.next()) {
				int id = rs.getInt("id");
				String annotType = rs.getString("annotation_type");
				
				
				int annotStart = rs.getInt("start");
				int annotEnd = rs.getInt("end");
				String features = rs.getString("features");
				Map<String, Object> featureMap = new HashMap<String, Object>();
				featureMap = (Map<String, Object>) gson.fromJson(features, featureMap.getClass());
				
				if (!punct) {
					if (annotType.equals("Token") && featureMap.get("kind").equals("punctuation"))
						continue;
				}
				
				String value = rs.getString("value");
				String provenance = rs.getString("provenance");
				Annotation annot = new Annotation(id, annotType, annotStart, annotEnd, value, featureMap, provenance);
				annotList.add(annot);
			}
			
			//System.out.println("end getSentAnnots");
		}
		catch(SQLException e)
		{
			throw new MSADBException(e);
		}
		
		return annotList;
	}
	
	public void writeMSAToDB(MultipleSequenceAlignment msa, String docNamespace, String docTable, String group) throws MSADBException
	{
		try {
			pstmtMSAInsert.setInt(1, msa.getID());
			pstmtMSAInsert.setString(2, docNamespace);
			pstmtMSAInsert.setString(3, docTable);
			pstmtMSAInsert.setString(4, group);
			pstmtMSAInsert.execute();
			
			String profileStr = msa.toProfileString();

			pstmtProfileInsert.setInt(1, msa.getID());
			pstmtProfileInsert.setString(2, docNamespace);
			pstmtProfileInsert.setString(3, docTable);
			pstmtProfileInsert.setString(4, group);
			pstmtProfileInsert.setString(5, profileStr);
			pstmtProfileInsert.execute();

			for (int i=0; i<msa.getNumRows(); i++) {
				MSARow row = msa.getRow(i);
				
				List<MSAToken> baseToks = row.getBaseTokens();
				List<MSAToken> fillerToks = row.getFillerTokens();
				
				String baseToksStr = gson.toJson(baseToks);
				String fillerToksStr = gson.toJson(fillerToks);			
				
				pstmtMSARowInsert.setInt(1, msa.getID());
				pstmtMSARowInsert.setString(2, docNamespace);
				pstmtMSARowInsert.setString(3, docTable);
				pstmtMSARowInsert.setString(4, group);
				pstmtMSARowInsert.setInt(5, i);
				pstmtMSARowInsert.setString(6, baseToksStr);
				pstmtMSARowInsert.setString(7, fillerToksStr);
				pstmtMSARowInsert.setInt(8, msa.getMultiplicity(i));
				pstmtMSARowInsert.setString(9, "");
				pstmtMSARowInsert.execute();
			}
		}
		catch(SQLException e)
		{
			throw new MSADBException(e);
		}
	}
	
	public List<Annotation> getAnnotations(String docNamespace, String docTable, long docID, long start, long end, String annotType, String provenance) throws MSADBException
	{
		List<Annotation> annotList = new ArrayList<Annotation>();
		
		try {
			StringBuilder queryStr = new StringBuilder("select document_id, document_namespace, document_table, id, start, " + rq + "end" + rq + ", value, annotation_type, value, features from annotation where");
			
			queryStr.append(" document_namespace = '" + docNamespace + "'");
			queryStr.append(" and document_table = '" + docTable + "'");
			
			if (docID >= 0)
				queryStr.append(" and document_id = " + docID);
			
			if (provenance != null)
				queryStr.append(" and provenance = '" + provenance + "'");
			if (annotType != null)
				queryStr.append(" and annotation_type = '" + annotType + "'");
			if (start >= 0)
				queryStr.append(" and start >= " + start);
			if (end >= 0)
				queryStr.append(" and start < " + end);
			
			queryStr.append(" order by start");
			
			Statement stmt = conn.createStatement();		
			ResultSet rs = stmt.executeQuery(queryStr.toString());
			while (rs.next()) {
				docID = (long) rs.getInt("document_id");
				int id = rs.getInt("id");
				long start2 = rs.getInt("start");
				long end2 = rs.getInt("end");
				
				//check if end is within range
				if (end >= 0 && end2 > end)
					continue;
				
				annotType = rs.getString("annotation_type");
				String value = rs.getString("value");
				String features = rs.getString("features");
				
				Map<String, Object> featureMap = new HashMap<String, Object>();
				featureMap = gson.fromJson(features, featureMap.getClass());
				Annotation annot = new Annotation(docID, docNamespace, docTable, id, annotType, start2, end2, value, featureMap);
				annotList.add(annot);
			}
			
			stmt.close();
		}
		catch(SQLException e)
		{
			throw new MSADBException(e);
		}
		
		return annotList;
	}

	
	public void close() throws MSADBException
	{
		try {
			conn.close();
			if (msaConn != null)
				msaConn.close();
		}
		catch(SQLException e)
		{
			throw new MSADBException(e);
		}
	}
	
	public void writeAnnotation(Annotation annot, String docNamespace, String docTable, long docID, String provenance) throws MSADBException
	{
		try {
			pstmtAnnotInsert.setInt(1, annot.getId());
			pstmtAnnotInsert.setString(2, docNamespace);
			pstmtAnnotInsert.setString(3, docTable);
			pstmtAnnotInsert.setLong(4, (int) docID);
			pstmtAnnotInsert.setString(5, annot.getAnnotationType());
			pstmtAnnotInsert.setInt(6, (int) annot.getStart());
			pstmtAnnotInsert.setInt(7, (int) annot.getEnd());
			pstmtAnnotInsert.setString(8, annot.getFeatures());
			pstmtAnnotInsert.setDouble(9, annot.getScore());
			pstmtAnnotInsert.setString(10, annot.getValue());
			pstmtAnnotInsert.setString(11, provenance);
			pstmtAnnotInsert.execute();
		}
		catch(SQLException e)
		{
			throw new MSADBException(e);
		}
	}

	public double getProfileScore(String group, String profileStr, int type, String annotType) throws MSADBException
	{
		double score = -1.0;

		try {
			pstmtReadProfile.setString(1, group);
			pstmtReadProfile.setString(2, profileStr);
			pstmtReadProfile.setInt(3, type);
			pstmtReadProfile.setString(4, annotType);
			ResultSet rs = pstmtReadProfile.executeQuery();
			
			if (rs.next())
				score = rs.getDouble(1);
		}
		catch(SQLException e)
		{
			throw new MSADBException(e);
		}
		
		return score;
	}
	
	public void writeProfile(int msaID, String docNamespace, String docTable, String annotType, String group, int type, String profileStr, double score) throws MSADBException
	{
		try {
			String name = annotType + "::" + group;
			pstmtProfileInsert.setInt(1, msaID);
			pstmtProfileInsert.setString(2, docNamespace);
			pstmtProfileInsert.setString(3, docTable);
			pstmtProfileInsert.setString(4, annotType);
			pstmtProfileInsert.setString(5, group);
			pstmtProfileInsert.setInt(6, type);
			pstmtProfileInsert.setString(7, name);
			pstmtProfileInsert.setString(8, profileStr);
			pstmtProfileInsert.setDouble(9, score);
			pstmtProfileInsert.execute();
		}
		catch(SQLException e)
		{
			throw new MSADBException(e);
		}
	}
	
	public void deleteProfiles(String annotType, String group) throws MSADBException
	{
		try {
			pstmtDeleteProfiles.setString(1, annotType);
			pstmtDeleteProfiles.setString(2, group);
			pstmtDeleteProfiles.execute();
		}
		catch(SQLException e)
		{
			throw new MSADBException(e);
		}
	}
	
	public void updateProfileScore(String docNamespace, String docTable, String group, String profileStr, int type, String annotType, double score) throws MSADBException
	{
		try {
			pstmtUpdateProfileScore.setString(1, docNamespace);
			pstmtUpdateProfileScore.setString(2, docTable);
			pstmtUpdateProfileScore.setString(3, group);
			pstmtUpdateProfileScore.setString(4, profileStr);
			pstmtUpdateProfileScore.setInt(5, type);
			pstmtUpdateProfileScore.setString(6, annotType);
			pstmtUpdateProfileScore.execute();
		}
		catch(SQLException e)
		{
			throw new MSADBException(e);
		}
	}
}
