package msa;

import java.io.*;
import java.sql.*;
import java.util.*;

import com.google.gson.Gson;

import msa.db.MySQLDBInterface;
import nlputils.sequence.SequenceUtilities;
import utils.db.DBConnection;

public class AnnotateDuplicate
{
	private Connection conn;
	private Connection conn2;
	private PreparedStatement pstmtAnnot;
	private PreparedStatement pstmtWriteAnnot;
	private PreparedStatement pstmtPatientDoc;
	private PreparedStatement pstmtAnnotID;
	private PreparedStatement pstmtSent;
	private PreparedStatement pstmtSentAnnots;
	private String schema;
	private String annotQuery;
	private String patientDocQuery;
	private String rq;
	private String docNamespace;
	private String docTable;
	private Map<Long, List<AnnotationSequence>> seqMap;


	
	public AnnotateDuplicate()
	{
	}
	
	public void init(String user, String password, String config)
	{
		try {
			Properties props = new Properties();
			props.load(new FileReader(config));
			init(user, password, props);
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	public void init(String user, String password, Properties props)
	{
		try {
			String host = props.getProperty("host");
			String dbName = props.getProperty("dbName");
			String dbType = props.getProperty("dbType");
			schema = props.getProperty("schema");
			annotQuery = props.getProperty("annotQuery");
			patientDocQuery = props.getProperty("patientDocQuery");
			docNamespace = props.getProperty("docNamespace");
			docTable = props.getProperty("docTable");
			
			
			conn = DBConnection.dbConnection(user, password, host, dbName, dbType);
			conn2 = DBConnection.dbConnection(user, password, host, dbName, dbType);
			
			rq = DBConnection.reservedQuote;
			
			conn2.setAutoCommit(false);
			
			//System.out.println(annotQuery);

			schema += ".";
			pstmtWriteAnnot = conn2.prepareStatement("insert into " + schema + "annotation (id, document_namespace, document_table, document_id, annotation_type, start, " + rq + "end" + rq +", value, provenance, score) "
					+ "values (?,?,?,?,?,?,?,?,'validation-tool-duplicate',1.0)");
			pstmtAnnot = conn.prepareStatement(annotQuery);
			pstmtAnnotID = conn.prepareStatement("select max(id) from " + schema + "annotation where document_id = ?");
			pstmtPatientDoc = conn.prepareStatement("select document_id from " + schema + "documents where PatientSID = ? and document_id in "
					+ "(select a.document_id from " + schema + "frame_instance_document a, " + schema + "project_frame_instance b where a.frame_instance_id = b.frame_instance_id and "
					+ "b.project_id = ?) "
					+ "order by document_id");
			
			pstmtSent = conn.prepareStatement("select id, start, " + rq + "end" + rq + " from " + schema + "annotation where document_id = ? and annotation_type = 'Sentence' order by start");
			pstmtSentAnnots = conn.prepareStatement("select value, start, " + rq + "end" + rq + " from " + schema + "annotation where document_id = ? and start >= ? and " + rq  + "end" + rq + " <= ? and annotation_type = 'Token' order by start");

		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	public void annotate(int projID)
	{
		try {
			seqMap = new HashMap<Long, List<AnnotationSequence>>();
			
			pstmtPatientDoc.setInt(2, projID);
			
			ResultSet rs = pstmtAnnot.executeQuery();
			
			long currDocID = -1;
			int currDocIndex = 0;
			
			Map<Long, Map<String, List<List<Object>>>> patientProfileMap = new HashMap<Long, Map<String, List<List<Object>>>>();
			List<AnnotationSequence> docSeqList = null;
			Map<Long, Long> patientMinDocMap = new HashMap<Long, Long>();
			
			while (rs.next()) {
				long docID = rs.getLong(1);
				long start = rs.getLong(2);
				long end = rs.getLong(3);
				String annotType = rs.getString(4);
				long patientID = rs.getLong(5);
				
				Long minDocID = patientMinDocMap.get(patientID);
				if (minDocID == null)
					patientMinDocMap.put(patientID, docID);

				
				System.out.println("docID: " + docID + " start: " + start + "|" + patientID + "|" + annotType);
				
				Map<String, List<List<Object>>> profileMap = patientProfileMap.get(patientID);
				if (profileMap == null) {
					profileMap = new HashMap<String, List<List<Object>>>();
					patientProfileMap.put(patientID, profileMap);
				}
				
				if (currDocID != docID) {
					docSeqList = getSequences(docID);
					currDocID = docID;
					currDocIndex = 0;
				}
				
				for (int i=currDocIndex; i<docSeqList.size(); i++) {
					AnnotationSequence seq = docSeqList.get(i);
					
					if (seq.getStart() <= start && seq.getEnd() >= end) {
						List<Annotation> tokAnnotList = seq.getAnnotList();
						int startIndex = -1;
						int endIndex = -1;
						List<Object> indexList= new ArrayList<Object>();
						String startStr = "";
						String endStr = "";
						
						for (int j=0; j<tokAnnotList.size(); j++) {
							Annotation annot = tokAnnotList.get(j);
							
							if (startIndex == -1 && annot.getStart() > start) {
								startIndex = j;
							}
							
							if (endIndex == -1 && annot.getStart() >= end && j > 0)
								endIndex = j-1;
							
							if (annot.getStart() <= start && annot.getEnd() > start) {
								startIndex = j;
								startStr = annot.getValue();
							}
							if (annot.getStart() < end && annot.getEnd() >= end) {
								endIndex = j;
								endStr = annot.getValue();
								break;
							}
						}
						
						if (startIndex == -1 || endIndex == -1)
							break;
						
						String sentStr = SequenceUtilities.getStrFromToks(seq.getToks());
						List<List<Object>> indexListList = profileMap.get(sentStr);
						if (indexListList == null) {
							indexListList = new ArrayList<List<Object>>();
							profileMap.put(sentStr, indexListList);
						}
						
						boolean found = false;
						for (List<Object> indexList2 : indexListList) {
							Integer start2 = (Integer) indexList2.get(0);
							Integer end2 = (Integer) indexList2.get(1);
							String annotType2 = (String) indexList2.get(2);
							
							if (start2 == startIndex && end2 == endIndex && annotType2.equals(annotType)) {
								found = true;
								break;
							}
						}
						
						
						if (!found)	 {
							indexList.add(startIndex);
							indexList.add(endIndex);
							indexList.add(annotType);
							indexListList.add(indexList);
							
							System.out.println("create dup: " + docID + "|" + sentStr + "|" + startStr + " " + endStr + "|" + annotType + "|" + startIndex + "|" + endIndex);
						}
						
						currDocIndex = i;
						break;
					}
				}
			}
				
				
			for (long patientID : patientProfileMap.keySet()) {
				Map<String, List<List<Object>>> profileMap = patientProfileMap.get(patientID);
				matchSequences(patientID, profileMap, patientMinDocMap.get(patientID));
			}

			
			
			conn.close();
			conn2.close();
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	private void matchSequences(long patientID, Map<String, List<List<Object>>> profileMap, long minDocID) throws SQLException
	{
		pstmtPatientDoc.setLong(1, patientID);

		ResultSet rs = pstmtPatientDoc.executeQuery();
		
		int count = 0;

		while (rs.next()) {
			long docID = rs.getLong(1);
			List<AnnotationSequence> docSeqList = getSequences(docID);
			
			int annotID = -1;
			pstmtAnnotID.setLong(1, docID);
			ResultSet rs2 = pstmtAnnotID.executeQuery();
			if (rs2.next()) {
				annotID = rs.getInt(1);
			}
			
			for (AnnotationSequence seq : docSeqList) {
				String seqStr = SequenceUtilities.getStrFromToks(seq.getToks());
				List<List<Object>> indexListList = profileMap.get(seqStr);
				
				if (indexListList != null) {
					
					System.out.println("matched seq! " + seqStr + "|" + seq.getDocID() + "|" + seq.getStart());
					
					for (List<Object> indexList : indexListList) {
						List<Annotation> tokAnnotList = seq.getAnnotList();
						List<String> toks = seq.getToks();
						Integer index1 = (Integer) indexList.get(0);
						Integer index2 = (Integer) indexList.get(1);
						String annotType = (String) indexList.get(2);
						Annotation tokAnnot = tokAnnotList.get(index1);
						Annotation tokAnnot2 = tokAnnotList.get(index2);
						long annotStart = tokAnnot.getStart();
						long annotEnd = tokAnnot2.getEnd();
						
						StringBuilder strBlder = new StringBuilder();
						for (int i=index1; i<=index2; i++) {
							strBlder.append(toks.get(i) + " ");
						}
						
						System.out.println("matched dup: " + docID + "|" + seqStr + "|" + strBlder.toString() + "|" + annotType + "|" + annotStart + "|" + annotEnd);
						
	
						pstmtWriteAnnot.setInt(1, ++annotID);
						pstmtWriteAnnot.setString(2, docNamespace);
						pstmtWriteAnnot.setString(3, docTable);
						pstmtWriteAnnot.setLong(4, docID);
						pstmtWriteAnnot.setString(5, annotType);
						pstmtWriteAnnot.setLong(6, annotStart);
						pstmtWriteAnnot.setLong(7, annotEnd);
						pstmtWriteAnnot.setString(8, strBlder.toString().trim());
						pstmtWriteAnnot.addBatch();
						count++;
						
						if (count == 100) {
							pstmtWriteAnnot.executeBatch();
							conn2.commit();
							count = 0;
						}
					}
				}
			}
		}
		
		pstmtWriteAnnot.executeBatch();
		conn2.commit();
	}
	
	private List<AnnotationSequence> getSequences(long docID)
	{
		List<AnnotationSequence> seqList = seqMap.get(docID);
		if (seqList != null)
			return seqList;
		
		seqList = new ArrayList<AnnotationSequence>();
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
				List<Annotation> annotList = new ArrayList<Annotation>();
				while(rs2.next()) {
					String tokStr = rs2.getString(1);
					long annotStart = rs2.getLong(2);
					long annotEnd = rs2.getLong(3);
					
					Annotation annot = new Annotation(-1, "Token", annotStart, annotEnd, tokStr, null);
					annotList.add(annot);
					toks.add(tokStr);
				}
				
				AnnotationSequence seq = new AnnotationSequence(docID, sentID, start, end);
				seq.setToks(toks);
				seq.setAnnotList(annotList);
				seqList.add(seq);
			}
			
			boolean prevFlag = false;
			AnnotationSequence prevSeq = null;
			for (int i=0; i<seqList.size(); i++) {
				AnnotationSequence seq = seqList.get(i);
				
				List<String> toks = seq.getToks();
				List<Annotation> annotList = seq.getAnnotList();
				
				List<Annotation> annotList2 = new ArrayList<Annotation>();

				if ((toks.size() < 10 && i > 0) || prevFlag) {
					List<String> toks2 = new ArrayList<String>();
					for (String tok : prevSeq.getToks()) {
						toks2.add(tok);
					}
					
					
					toks2.addAll(toks);
					annotList2 = prevSeq.getAnnotList();
					
					prevSeq = new AnnotationSequence();
					prevSeq.setToks(toks);
					prevSeq.setAnnotList(annotList);

					annotList2.addAll(annotList);
					
					
					seq.setToks(toks2);
					seq.setAnnotList(annotList2);
					
					
					if (toks.size() < 10)
						prevFlag = true;
					else
						prevFlag = false;
				}
				else
					prevSeq = seq;
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		
		seqMap.put(docID, seqList);
		
		return seqList;
	}
	
	public static void main(String[] args)
	{
		if (args.length != 4) {
			System.out.println("usage: user password config projID");
			System.exit(0);
		}
		
		AnnotateDuplicate dup = new AnnotateDuplicate();
		dup.init(args[0], args[1], args[2]);
		dup.annotate(Integer.parseInt(args[3]));
	}
}
