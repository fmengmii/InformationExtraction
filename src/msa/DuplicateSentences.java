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
	
	private String schema;
	
	
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
			schema = props.getProperty("schema") + ".";
			String docSchema = props.getProperty("docSchema");
			String docTable = props.getProperty("docTable");
			String docDBName = props.getProperty("docDBName");
			String docEntityCol = props.getProperty("docEntityColumn");
			String docIDCol = props.getProperty("docIDColumn");
			int projID = Integer.parseInt(props.getProperty("projectID"));
			
			conn = DBConnection.dbConnection(user, password, host, dbName, dbType);
			conn2 = DBConnection.dbConnection(user, password, host, dbName, dbType);
			conn2.setAutoCommit(false);
			
			String rq = DBConnection.reservedQuote;
			
			String docNamespace = docSchema;
			
			if (dbType.startsWith("sqlserver") && !dbName.equals(docDBName))
				docNamespace = docDBName + "." + docSchema;
			
			pstmtSent = conn.prepareStatement("select id, start, " + rq + "end" + rq + " from " + schema + "annotation where document_id = ? and annotation_type = 'Sentence' order by start");
			pstmtSentAnnots = conn.prepareStatement("select value from " + schema + "annotation where document_id = ? and start >= ? and " + rq  + "end" + rq + " <= ? and annotation_type = 'Token' order by start");
			PreparedStatement pstmtAnnot = conn2.prepareStatement("insert into " + schema + "annotation (id, document_namespace, document_table, document_id, annotation_type, start, " + rq + "end" + rq + ", value, features, provenance, score) "
				+ "values (?,'" + docNamespace + "','" + docTable + "',?,'SentenceDuplicate',?,?,'','','duplicate-sentences-util',1.0)");
			pstmtAnnotID = conn.prepareStatement("select max(id) from " + schema + "annotation where document_id = ?");
			
			Statement stmt = conn.createStatement();
			
			int currPatientSID = -1;
			Map<String, Integer> sentMap = new HashMap<String, Integer>();
			Map<String, Integer> docMatchCountMap = new HashMap<String, Integer>();
			int batchCount = 0;
			
			docNamespace += ".";
			
			
			ResultSet rs = stmt.executeQuery("select a.document_id, b." + docEntityCol + ", a.frame_instance_id from " + schema + "frame_instance_document a, " + docNamespace + docTable + " b "
				+ "where a.document_id = b." + docIDCol + " and a.document_id in "
				+ "(select c.document_id from " + schema + "frame_instance_document c, " + schema + "project_frame_instance d where c.frame_instance_id = d.frame_instance_id and "
				+ "d.project_id = " + projID + ") "
				+ "order by a.document_id");
			
			long currDocID = -1;
			int annotID = -1;
			while (rs.next()) {
				long docID = rs.getLong(1);
				int patientSID = rs.getInt(2);
				int frameInstanceID = rs.getInt(3);
				
				System.out.println("DocID: " + docID + " PatientSID: " + patientSID);
				
				annotID = getAnnotID(docID) + 1;
				
				if (patientSID != currPatientSID) {
					sentMap = new HashMap<String,Integer>();
					docMatchCountMap = new HashMap<String, Integer>();
					currPatientSID = patientSID;
				}
				
				List<AnnotationSequence> seqList = getSequences(docID);
				
				boolean prevFlag = false;
				for (int i=0; i<seqList.size(); i++) {
					AnnotationSequence seq = seqList.get(i);
					List<String> toks = seq.getToks();
					
					List<String> toks2 = toks;

					if ((toks.size() < 10 && i > 0) || prevFlag) {
						toks2 = new ArrayList<String>();
						for (String tok : seqList.get(i-1).getToks()) {
							toks2.add(tok);
						}
						
						toks2.addAll(toks);
						//adjust the start index
						seq.setStart(seqList.get(i-1).getStart());
						
						if (toks.size() < 10)
							prevFlag = true;
						else
							prevFlag = false;
					}
						

					
					String sentStr = SequenceUtilities.getStrFromToks(toks2).toLowerCase();
					
					Integer prevFrameInstanceID = sentMap.get(sentStr);
					if (prevFrameInstanceID != null) {
						String key = prevFrameInstanceID + "|" + frameInstanceID;
						Integer count = docMatchCountMap.get(key);
						if (count == null)
							count = 0;
						
						docMatchCountMap.put(key, ++count);
						System.out.println("adding: " + key + ": " + count);
						
						//int annotID = getAnnotID(docID);
						pstmtAnnot.setInt(1, annotID);
						pstmtAnnot.setLong(2, docID);
						pstmtAnnot.setLong(3, seq.getStart());
						pstmtAnnot.setLong(4, seq.getEnd());
						pstmtAnnot.addBatch();
						batchCount++;
						
						if (batchCount == 100) {
							batchCount = 0;
							pstmtAnnot.executeBatch();
							conn2.commit();
						}
						
						System.out.println("wrote duplicate: " + sentStr);
						
						annotID++;
					}
					
					sentMap.put(sentStr, frameInstanceID);
				}
			}
			
			pstmtAnnot.executeBatch();
			conn2.commit();
			
			orderDocs(docMatchCountMap, projID);
			
			
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
				seq.setToks(toks);
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
				annotID = rs.getInt(1) + 1;
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		
		return annotID;
	}
	
	private void orderDocs(Map<String, Integer> docMatchCountMap, int projID) throws SQLException
	{
		conn.setAutoCommit(false);
		PreparedStatement pstmt = conn.prepareStatement("insert into " + schema + "frame_instance_order (project_id, order_num, frame_instance_id) values (?,?,?)");
		pstmt.setInt(1, projID);
		
		List<Long> frameInstanceIDList = new ArrayList<Long>();
		List<String> frameInstancePairList = new ArrayList<String>();
		List<Integer> frameInstancePairCountList = new ArrayList<Integer>();
		Map<Integer, List<Integer>> clusterMap = new HashMap<Integer, List<Integer>>();
		
		for (String key : docMatchCountMap.keySet()) {
			int count = docMatchCountMap.get(key);
			
			System.out.println(key + ": " + count);
			
			boolean inserted = false;
			for (int i=0;i<frameInstancePairCountList.size(); i++) {
				if (frameInstancePairCountList.get(i) < count) {
					frameInstancePairCountList.add(i, count);
					frameInstancePairList.add(i, key);
					inserted = true;
					break;
				}
			}
			
			if (!inserted) {
				frameInstancePairCountList.add(count);
				frameInstancePairList.add(key);
			}
		}
		
		int currClusterID = 0;
		for (String key : frameInstancePairList) {
			String[] parts = key.split("\\|");
			int frameInstanceID1 = Integer.parseInt(parts[0]);
			int frameInstanceID2 = Integer.parseInt(parts[1]);
			
			System.out.println("Linking: " + frameInstanceID1 + " and " + frameInstanceID2);
			
			List<Integer> cluster1 = clusterMap.get(frameInstanceID1);
			List<Integer> cluster2 = clusterMap.get(frameInstanceID2);
			
			if (cluster1 != null && cluster2 != null) {
				cluster2.set(0, cluster1.get(0));
			}
			else if (cluster1 == null && cluster2 != null) {
				clusterMap.put(frameInstanceID1, cluster2);
			}
			else if (cluster1 != null && cluster2 == null) {
				clusterMap.put(frameInstanceID2, cluster1);
			}
			else {
				List<Integer> cluster = new ArrayList<Integer>();
				cluster.add(currClusterID++);
				clusterMap.put(frameInstanceID1, cluster);
				clusterMap.put(frameInstanceID2, cluster);
			}
		}
			
		Map<Integer, List<Integer>> inverseClusterMap = new HashMap<Integer, List<Integer>>();
		for (Integer frameInstanceID : clusterMap.keySet()) {
			int cluster = clusterMap.get(frameInstanceID).get(0);
			
			List<Integer> clusterList = inverseClusterMap.get(cluster);
			if (clusterList == null) {
				clusterList = new ArrayList<Integer>();
				inverseClusterMap.put(cluster, clusterList);
			}
			
			boolean inserted = false;
			for (int i=0; i<clusterList.size(); i++) {
				if (clusterList.get(i) > frameInstanceID) {
					clusterList.add(i, frameInstanceID);
					inserted = true;
					break;
				}
			}
			
			if (!inserted)
				clusterList.add(frameInstanceID);
			
			System.out.println("cluster: " + cluster);
			for (int frameInstanceID2 : clusterList)
				System.out.print(frameInstanceID2 + ", ");
			System.out.println();
		}
		
		int batchCount = 0;
		int orderNum = 0;
		for (int cluster : inverseClusterMap.keySet()) {
			List<Integer> clusterList = inverseClusterMap.get(cluster);
			
			for (int frameInstanceID : clusterList) {
				pstmt.setInt(2, orderNum++);
				pstmt.setInt(3, frameInstanceID);
							
				pstmt.addBatch();
				batchCount++;
				
				if (batchCount == 100) {
					pstmt.executeBatch();
					conn.commit();
					batchCount = 0;
				}
			}
		}

		pstmt.executeBatch();
		conn.commit();
		
		conn.setAutoCommit(true);
		
		
		pstmt.close();
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
