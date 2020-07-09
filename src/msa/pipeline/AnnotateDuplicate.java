package msa.pipeline;

import java.io.*;
import java.sql.*;
import java.util.*;

import com.google.gson.Gson;

import msa.Annotation;
import msa.AnnotationSequence;
import msa.db.MySQLDBInterface;
import nlputils.sequence.SequenceUtilities;
import utils.db.DBConnection;

public class AnnotateDuplicate extends MSAModule
{
	private Connection conn;
	private Connection conn2;
	private Connection connDoc;

	private PreparedStatement pstmtAnnot;
	private PreparedStatement pstmtWriteAnnot;
	private PreparedStatement pstmtEntityDoc;
	private PreparedStatement pstmtAnnotID;
	private PreparedStatement pstmtSent;
	private PreparedStatement pstmtSentAnnots;
	private PreparedStatement pstmtUMLS;
	private PreparedStatement pstmtUMLSGet;
	private PreparedStatement pstmtGetDocs;
	private PreparedStatement pstmtGetEntity;
	private PreparedStatement pstmtMatchUMLS;
	
	
	private String schema;
	private String docSchema;
	private String patientDocQuery;
	private String rq;
	private String docNamespace;
	private String docTable;
	private String docIDCol;
	private String docTextCol;
	private String docEntityCol;
	private Map<Long, List<AnnotationSequence>> seqMap;
	private int projID;
	
	private Gson gson;


	
	public AnnotateDuplicate()
	{
		gson = new Gson();
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
			//patientDocQuery = props.getProperty("patientDocQuery");
			//docNamespace = props.getProperty("docNamespace");
			docTable = props.getProperty("docTable");
			//String docUser = props.getProperty("docDBUser");
			//String docPassword = props.getProperty("docDBPassword");
			String docDBHost = props.getProperty("docDBHost");
			String docDBName = props.getProperty("docDBName");
			String docDBType = props.getProperty("docDBType");
			docSchema = props.getProperty("docSchema") + ".";
			docIDCol = props.getProperty("docIDColumn");
			docTextCol = props.getProperty("docTextColumn");
			docEntityCol = props.getProperty("docEntityColumn");
			
			docNamespace = docSchema;
			if (!docDBName.equals(dbName) && docDBType.startsWith("sqlserver"))
				docNamespace = docDBName + "." + docSchema;
			
			
			conn = DBConnection.dbConnection(user, password, host, dbName, dbType);
			conn2 = DBConnection.dbConnection(user, password, host, dbName, dbType);
			connDoc = DBConnection.dbConnection(user, password, docDBHost, docDBName, docDBType);
			
			rq = DBConnection.reservedQuote;
			
			conn2.setAutoCommit(false);
			
			//System.out.println(annotQuery);

			schema += ".";
			pstmtWriteAnnot = conn2.prepareStatement("insert into " + schema + "annotation (id, document_namespace, document_table, document_id, annotation_type, start, " + rq + "end" + rq +", value, provenance, score) "
					+ "values (?,?,?,?,?,?,?,?,?,1.0)");
			pstmtAnnot = conn.prepareStatement("select a.document_id, a.start, a." + rq + "end" + rq + ", a.annotation_type, b." + docEntityCol 
				+ " from " + schema + "annotation a, " + docNamespace + docTable + " b where a.document_id = b." + docIDCol + " and a.provenance = 'validation-tool' "
				+ "and a.document_id in "
				+ "(select a.document_id from " + schema + "frame_instance_document a, " + schema + "project_frame_instance b where a.frame_instance_id = b.frame_instance_id and "
				+ "b.project_id = ?) "
				+ "order by a.document_id");
			pstmtAnnotID = conn.prepareStatement("select max(id) from " + schema + "annotation where document_id = ?");
			pstmtEntityDoc = conn.prepareStatement("select " + docIDCol + " from " + docNamespace + docTable + " where " + docEntityCol + " = ? and " + docIDCol + " in "
				+ "(select a.document_id from " + schema + "frame_instance_document a, " + schema + "project_frame_instance b where a.frame_instance_id = b.frame_instance_id and "
				+ "b.project_id = ?) "
				+ "order by document_id");
			
			pstmtSent = conn.prepareStatement("select id, start, " + rq + "end" + rq + " from " + schema + "annotation where document_id = ? and annotation_type = 'Sentence' order by start");
			pstmtSentAnnots = conn.prepareStatement("select value, start, " + rq + "end" + rq + " from " + schema + "annotation "
				+ "where document_id = ? and start >= ? and " + rq  + "end" + rq + " <= ? and annotation_type = 'Token' order by start");

			//pstmtUMLS = conn.prepareStatement("select features from " + schema + "annotation where annotation_type = 'MetaMap' and ((start >= ? and " + rq + "end" + rq + " <= ?) or "
			//	+ "(start < ? and " + rq + "end" + rq + " > ?) or (start < ? and " + rq + "end" + rq + " > ?) or (start < ? and " + rq + "end" + rq + "> ?))");
			pstmtUMLS = conn.prepareStatement("select features from " + schema + "annotation where annotation_type = 'MetaMap' and start = ? and " + rq + "end" + rq + " = ?");
			
			
			
			pstmtGetDocs = connDoc.prepareStatement("select document_id, " + docTextCol + " from " + docNamespace + docTable + " where " + docEntityCol + " = ?");
			pstmtGetEntity = connDoc.prepareStatement("select " + docEntityCol + " from " + docNamespace + docTable + " where document_id = ?");
			pstmtMatchUMLS = conn.prepareStatement("select start, " + rq + "end" + rq + " from " + schema + "annotation where document_id = ? and "
					+ "annotation_type = 'MetaMap' and features like ?");
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	public void setProjID(int projID)
	{
		this.projID = projID;
	}
	
	
	public void run()
	{
		annotate();
		annotDuplicateTags();
	}
	
	public void annotate()
	{
		try {
			seqMap = new HashMap<Long, List<AnnotationSequence>>();
			
			pstmtEntityDoc.setInt(2, projID);
			
			pstmtAnnot.setInt(1, projID);
			ResultSet rs = pstmtAnnot.executeQuery();
			
			long currDocID = -1;
			int currDocIndex = 0;
			
			Map<String, Map<String, List<List<Object>>>> patientProfileMap = new HashMap<String, Map<String, List<List<Object>>>>();
			List<AnnotationSequence> docSeqList = null;
			Map<String, Long> patientMinDocMap = new HashMap<String, Long>();
			
			while (rs.next()) {
				long docID = rs.getLong(1);
				long start = rs.getLong(2);
				long end = rs.getLong(3);
				String annotType = rs.getString(4);
				String patientID = rs.getString(5);
				
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
				
				
			for (String entity : patientProfileMap.keySet()) {
				Map<String, List<List<Object>>> profileMap = patientProfileMap.get(entity);
				matchSequences(entity, profileMap);
			}

			seqMap = null;
			
			//conn.close();
			//conn2.close();
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	public void close()
	{
		try {
			conn.close();
			conn2.close();
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	private void matchSequences(String entity, Map<String, List<List<Object>>> profileMap) throws SQLException
	{
		pstmtEntityDoc.setString(1, entity);
		
		pstmtWriteAnnot.setString(9, "validation-tool-duplicate");

		ResultSet rs = pstmtEntityDoc.executeQuery();
		
		int count = 0;

		while (rs.next()) {
			long docID = rs.getLong(1);
			List<AnnotationSequence> docSeqList = getSequences(docID);
			
			int annotID = -1;
			pstmtAnnotID.setLong(1, docID);
			ResultSet rs2 = pstmtAnnotID.executeQuery();
			if (rs2.next()) {
				annotID = rs2.getInt(1);
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
	
	public void annotDuplicateTags()
	{
		try {
			String rq = DBConnection.reservedQuote;
			Statement stmt = conn.createStatement();
			Statement stmtDoc = connDoc.createStatement();
			
			pstmtWriteAnnot.setString(9, "validation-tool-unlabeled");
			
			Map<Long, Map<String, Map<String, Boolean>>> docMap = new HashMap<Long, Map<String, Map<String, Boolean>>>();
			Map<Long, Map<String, Map<String, Boolean>>> docMapUMLS = new HashMap<Long, Map<String, Map<String, Boolean>>>();
			ResultSet rs = stmt.executeQuery("select document_id, value, annotation_type, start, " + rq + "end" + rq + " from " + schema + "annotation where provenance = 'validation-tool' "
				+ "and document_id in "
				+ "(select a.document_id from " + schema + "frame_instance_document a, " + schema + "project_frame_instance b where a.frame_instance_id = b.frame_instance_id and "
				+ "b.project_id = " + projID + ") "
				+ "order by document_id");
			
			while (rs.next()) {
				long docID = rs.getLong(1);
				String value = rs.getString(2);
				String annotType = rs.getString(3);
				long start = rs.getLong(4);
				long end = rs.getLong(5);
				
				if (value.length() < 2)
					continue;
				
				String umlsStr = getUMLSConcept(docID, start, end);
				if (umlsStr != null && umlsStr.length() < 2)
					umlsStr = null;
				
				Map<String, Map<String, Boolean>> termMap = docMap.get(docID);
				if (termMap == null) {
					termMap = new HashMap<String, Map<String, Boolean>>();
					docMap.put(docID, termMap);
				}
				
				Map<String, Boolean> termList = termMap.get(annotType);
				if (termList == null) {
					termList = new HashMap<String, Boolean>();
					termMap.put(annotType, termList);
				}
				
				Boolean flag = termList.get(value.toLowerCase());
				if (flag == null)
					termList.put(value.toLowerCase(), true);
				
				
				if (umlsStr != null) {
					Map<String, Map<String, Boolean>> termMapUMLS = docMapUMLS.get(docID);
					if (termMapUMLS == null) {
						termMapUMLS = new HashMap<String, Map<String, Boolean>>();
						docMapUMLS.put(docID, termMapUMLS);
					}
					
					Map<String, Boolean> termListUMLS = termMapUMLS.get(annotType);
					if (termListUMLS == null) {
						termListUMLS = new HashMap<String, Boolean>();
						termMapUMLS.put(annotType, termListUMLS);
					}
					
					flag = termListUMLS.get(umlsStr.toLowerCase());
					if (flag == null)
						termListUMLS.put(umlsStr.toLowerCase(), true);
				}	
			}
			
			
			for (long docID : docMap.keySet()) {
				Map<String, Map<String, Boolean>> termMap = docMap.get(docID);
				pstmtGetEntity.setLong(1, docID);
				rs = pstmtGetEntity.executeQuery();
				String group = "";
				if (rs.next()) {
					group = rs.getString(1);
				}
				
				pstmtGetDocs.setString(1, group);
				rs = pstmtGetDocs.executeQuery();
				
				while (rs.next()) {
					long docID2 = rs.getLong(1);
					String text = rs.getString(2);
					
					for (String annotType : termMap.keySet()) {
						Map<String, Boolean> termList = termMap.get(annotType);
						List<Object[]> matchList = getMatches(docID2, text, termList);
						writeAnnotations(docID2, matchList, annotType);
						docMap.put(docID, null);
					}
				}
			}
			
			//UMLS terms
			
			/*
			for (long docID : docMapUMLS.keySet()) {
				Map<String, Map<String, Boolean>> termMapUMLS = docMapUMLS.get(docID);
				pstmtGetEntity.setLong(1, docID);
				rs = pstmtGetEntity.executeQuery();
				String entity = "";
				if (rs.next()) {
					entity = rs.getString(1);
				}
				
				pstmtGetDocs.setString(1, entity);
				rs = pstmtGetDocs.executeQuery();
				
				while (rs.next()) {
					long docID2 = rs.getLong(1);
					pstmtMatchUMLS.setLong(1, docID2);
					
					for (String annotType : termMapUMLS.keySet()) {
						Map<String, Boolean> termListUMLS = termMapUMLS.get(annotType);
						for (String term : termListUMLS.keySet()) {
							pstmtMatchUMLS.setString(2, "%PreferredName=" + term + "%");
							
							ResultSet rs2 = pstmtMatchUMLS.executeQuery();
							List<Object[]> matchList = new ArrayList<Object[]>();
							while (rs2.next()) {
								Object[] match = new Object[3];
								match[0] = rs2.getLong(1);
								match[1] = rs2.getLong(2);
								match[2] = term;
								matchList.add(match);
							}
							
							writeAnnotations(docID2, matchList, annotType);
							docMapUMLS.put(docID2, null);
						}
					}
				}
			}
			*/
			
			
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	private String getUMLSConcept(long docID, long start, long end) throws SQLException
	{
		pstmtUMLS.setLong(1, start);
		pstmtUMLS.setLong(2, end);
		
		ResultSet rs = pstmtUMLS.executeQuery();
		String umlsConcept = "";
		Map<String, Object> featureMap = new HashMap<String, Object>();
		while (rs.next()) {
			String features = rs.getString(1);
			featureMap = gson.fromJson(features, featureMap.getClass());
			String concept = (String) featureMap.get("PreferredName");
			if (concept == null)
				continue;
			
			if (concept.length() > umlsConcept.length())
				umlsConcept = concept;
		}
		
		return umlsConcept;
	}
	
	private List<Object[]> getMatches(long docID, String text, Map<String, Boolean> termList)
	{
		List<Object[]> matchList = new ArrayList<Object[]>();
		for (String term : termList.keySet()) {
			int index = text.indexOf(term);
			
			int count = 0;

			while (index >= 0) {
				Object[] match = new Object[3];
				match[0] = index;
				match[1] = index + term.length();
				match[2] = term;
				matchList.add(match);
				
				index = text.indexOf(term, index+1);
				
				count++;
				
				if (count > 100) {
					System.out.println("getMatches count exceeded!: " + term + ", " + index);
					System.out.println(text);
					System.exit(0);
				}
			}
		}
		
		return matchList;
	}
	
	private void writeAnnotations(long docID, List<Object[]> matchList, String annotType) throws SQLException
	{
		int annotID = -1;
		pstmtAnnotID.setLong(1, docID);
		ResultSet rs = pstmtAnnotID.executeQuery();
		if (rs.next()) {
			annotID = rs.getInt(1);
		}
		
		int count = 0;
		
		for (Object[] match : matchList) {
			pstmtWriteAnnot.setInt(1, ++annotID);
			pstmtWriteAnnot.setString(2, docNamespace);
			pstmtWriteAnnot.setString(3, docTable);
			pstmtWriteAnnot.setLong(4, docID);
			pstmtWriteAnnot.setString(5, annotType);
			pstmtWriteAnnot.setInt(6, (int) match[0]);
			pstmtWriteAnnot.setInt(7, (int) match[1]);
			pstmtWriteAnnot.setString(8, (String) match[2]);
			pstmtWriteAnnot.addBatch();
			count++;
			
			System.out.println("wrote annotdup: " + docID + "|" + match[0] + "|" + match[1] + "|" + match[2] + "|" + annotType);
			
			if (count == 100) {
				pstmtWriteAnnot.executeBatch();
				conn2.commit();
				count = 0;
			}
		}
		
		pstmtWriteAnnot.executeBatch();
		conn2.commit();
	}
	
	public static void main(String[] args)
	{
		if (args.length != 4) {
			System.out.println("usage: user password config projID");
			System.exit(0);
		}
		
		AnnotateDuplicate dup = new AnnotateDuplicate();
		dup.init(args[0], args[1], args[2]);
		dup.setProjID(Integer.parseInt(args[3]));
		dup.run();
	}
}
