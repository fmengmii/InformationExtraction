package msa;

import java.io.*;
import java.sql.*;
import java.util.*;

import nlputils.sequence.SequenceUtilities;
import utils.db.DBConnection;

public class DuplicateSentences
{
	private Connection conn;
	private Connection conn2;
	
	private PreparedStatement pstmtSent;
	private PreparedStatement pstmtSentAnnots;
	private PreparedStatement pstmtAnnotID;
	
	
	public DuplicateSentences()
	{
	}
	
	public void annotateDuplicateSentences(String user, String password, String config)
	{
		try {
			Properties props = new Properties();
			props.load(new FileReader(config));
			String host = props.getProperty("host");
			String dbName = props.getProperty("dbName");
			String dbType = props.getProperty("dbType");
			String docQuery = props.getProperty("docQuery");
			String schema = props.getProperty("schema");
			String docNamespace = props.getProperty("docNamespace");
			String docTable = props.getProperty("docTable");
			
			conn = DBConnection.dbConnection(user, password, host, dbName, dbType);
			conn2 = DBConnection.dbConnection(user, password, host, dbName, dbType);
			conn2.setAutoCommit(false);
			
			String rq = DBConnection.reservedQuote;
			
			pstmtSent = conn.prepareStatement("select id, start, " + rq + "end" + rq + " from " + schema + "annotation where document_id = ? and annotation_type = 'Sentence' order by start");
			pstmtSentAnnots = conn.prepareStatement("select value from " + schema + "annotation where document_id = ? and start >= ? and " + rq  + "end" + rq + " <= ? and annotation_type = 'Token' order by start");
			PreparedStatement pstmtAnnot = conn2.prepareStatement("insert into " + schema + "annotation (id, document_namespace, document_table, document_id, annotation_type, start, " + rq + "end" + rq + ", value, features, provenance, score) "
				+ "values (?,'" + docNamespace + "','" + docTable + "',?,'SentenceDuplicate',?,?,'','','duplicate-sentences-util',1.0)");
			pstmtAnnotID = conn.prepareStatement("select max(id) from " + schema + "annotation where document_id = ?");
			
			Statement stmt = conn.createStatement();
			
			int currPatientSID = -1;
			Map<String, Boolean> sentMap = new HashMap<String, Boolean>();
			int batchCount = 0;
			
			
			ResultSet rs = stmt.executeQuery(docQuery);
			while (rs.next()) {
				long docID = rs.getLong(1);
				int patientSID = rs.getInt(2);
				
				if (patientSID != currPatientSID) {
					sentMap = new HashMap<String,Boolean>();
					currPatientSID = patientSID;
				}
				
				List<AnnotationSequence> seqList = getSequences(docID);
				
				for (AnnotationSequence seq : seqList) {
					String sentStr = SequenceUtilities.getStrFromToks(seq.getToks());
					
					Boolean flag = sentMap.get(sentStr);
					if (flag == null)
						sentMap.put(sentStr, true);
					else {
						int annotID = getAnnotID(docID);
						pstmtAnnot.setInt(1, annotID);
						pstmtAnnot.setLong(2, docID);
						pstmtAnnot.setLong(3, seq.getStart());
						pstmtAnnot.setLong(4, seq.getEnd());
						pstmtAnnot.addBatch();
						batchCount++;
						
						if (batchCount == 1000) {
							batchCount = 0;
							pstmtAnnot.executeBatch();
							conn.commit();
						}
					}
				}
			}
			
			pstmtAnnot.executeBatch();
			conn.commit();
			
			
			pstmtSent.close();
			pstmtAnnot.close();
			stmt.close();
			conn.close();
			conn2.close();
			
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	private List<AnnotationSequence> getSequences(long docID)
	{
		List<AnnotationSequence> seqList = new ArrayList<AnnotationSequence>();
		try {
			pstmtSent.setLong(1, docID);
			ResultSet rs = pstmtSent.executeQuery();
			while (rs.next()) {
				int sentID = rs.getInt(1);
				int start = rs.getInt(2);
				int end = rs.getInt(3);
				
				pstmtSentAnnots.setLong(1, docID);
				pstmtSentAnnots.setLong(2, start);
				pstmtSentAnnots.setLong(3, end);
				ResultSet rs2 = pstmtSentAnnots.executeQuery();
				
				List<String> toks = new ArrayList<String>();
				while(rs2.next()) {
					String tokStr = rs2.getString(1);
					toks.add(tokStr);
				}
				
				AnnotationSequence seq = new AnnotationSequence(docID, sentID, start, end);
				seqList.add(seq);
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		
		return seqList;
	}
	
	private int getAnnotID(long docID)
	{
		int annotID = -1;
		
		try {
			pstmtAnnotID.setLong(1, docID);
			ResultSet rs = pstmtAnnotID.executeQuery();
			if (rs.next())
				annotID = rs.getInt(1);
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		
		return annotID;
	}
	
	public static void main(String[] args)
	{
		if (args.length != 3) {
			System.out.println("usage: user password config");
			System.exit(0);
		}
		
		DuplicateSentences dupe = new DuplicateSentences();
		dupe.annotateDuplicateSentences(args[0], args[1], args[2]);
	}
}
