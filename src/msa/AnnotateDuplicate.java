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
	private GenSentences genSent;
	private PreparedStatement pstmtGetSentToks;
	private Map<String, List<Integer>> indexMap;
	private int seqIndex = 0;
	private List<AnnotationSequence> seqList;
	private PreparedStatement pstmtAnnot;
	private PreparedStatement pstmtWriteAnnot;
	private PreparedStatement pstmtAnnotID;
	private String targetType;
	private String schema;
	private String docQuery;
	private String annotQuery;
	private String rq;
	private String docNamespace;
	private String docTable;
	
	private Gson gson;

	
	public AnnotateDuplicate()
	{
		gson = new Gson();
	}
	
	public void setTargetType(String targetType)
	{
		this.targetType = targetType;
	}
	
	public void setGenSent(GenSentences genSent)
	{
		this.genSent = genSent;
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
			targetType = props.getProperty("targetType");
			docQuery = props.getProperty("docQuery");
			annotQuery = props.getProperty("annotQuery");
			docNamespace = props.getProperty("docNamespace");
			docTable = props.getProperty("docTable");
			List<Map<String, Object>> msaAnnotFilterList = new ArrayList<Map<String, Object>>();
			msaAnnotFilterList = gson.fromJson(props.getProperty("msaAnnotFilterList"), msaAnnotFilterList.getClass());
			//String targetProvenance = props.getProperty("targetProvenance");
			String tokType = props.getProperty("tokType");
			Boolean punct = Boolean.parseBoolean(props.getProperty("punctuation"));
			
			conn = DBConnection.dbConnection(user, password, host, dbName, dbType);
			conn2 = DBConnection.dbConnection(user, password, host, dbName, dbType);
			
			rq = DBConnection.reservedQuote;
			
			conn2.setAutoCommit(false);
			
			if (genSent == null || genSent.getDB() == null) {
				MySQLDBInterface db = new MySQLDBInterface();
				db.setDBType(dbType);
				db.setSchema(schema);
				db.init(user, password, host, dbName, dbName);
				
				genSent = new GenSentences();
				genSent.setVerbose(true);
				genSent.setPunct(punct);
				genSent.setTokenType(tokType);
				genSent.init(db, msaAnnotFilterList, "validation-tool");
			}
			
			genSent.setRequireTarget(false);
			
			Statement stmt = conn.createStatement();
			List<Long> docIDList = new ArrayList<Long>();
			ResultSet rs = stmt.executeQuery(docQuery);
			while (rs.next()) {
				docIDList.add(rs.getLong(1));
			}
			
			genSent.setDocIDList(docIDList);
			genSent.genSentenceAnnots(docNamespace, docTable);

			schema += ".";
			pstmtWriteAnnot = conn2.prepareStatement("insert into " + schema + "annotation (id. document_namespace, document_table, document_id, annotation_type, start, " + rq + "end" + rq +", provenance, score) "
					+ "values (?,?,?,?,?,?,?,'validation-tool-duplicate',0.0)");
			pstmtAnnot = conn.prepareStatement(annotQuery);
			pstmtAnnotID = conn.prepareStatement("select max(id) from " + schema + "annotation where document_id = ?");

		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	public void annotate()
	{
		try {
			Statement stmt = conn.createStatement();
			
			//ResultSet rs = stmt.executeQuery("select a.document_id, a.start, a.end from " + schema + "annotation a, " + schema + "document_status b "
			//	+ "where a.provenance = 'validation-tool' and b.status = -4 and a.document_id = b.document_id order by a.document_id, a.start");
			
			pstmtAnnot.setString(1, targetType);
			ResultSet rs = pstmtAnnot.executeQuery();
			
			seqList = genSent.getSeqList();
			
			long currDocID = -1;
			seqIndex = 0;
			
			List<AnnotationSequence> subSeqList = null;
			while (rs.next()) {
				long docID = rs.getLong(1);
				long start = rs.getLong(2);
				long end = rs.getLong(3);
				
				System.out.println("docID: " + docID + " start: " + start);
				
				if (currDocID != docID) {
					if (currDocID >= 0) {
						AnnotationSequence seq = seqList.get(seqIndex);
						while (seq.getDocID() == currDocID) { 
							subSeqList.add(seq);
							seqIndex++;
							seq = seqList.get(seqIndex);
						}
						
						matchSequences(subSeqList, currDocID);
					}

					subSeqList = new ArrayList<AnnotationSequence>();
					indexMap = new HashMap<String, List<Integer>>();
					currDocID = docID;
				}
				
				AnnotationSequence seq = seqList.get(seqIndex);
				while (seq.getDocID() != currDocID || seq.getEnd() <= start) {
					if (indexMap.size() > 0)
						subSeqList.add(seq);
					
					seqIndex++;
					seq = seqList.get(seqIndex);
				}
				
				List<Annotation> tokAnnotList = seq.getAnnotList(":token|string");
				int startIndex = -1;
				int endIndex = -1;
				List<Integer> indexList= new ArrayList<Integer>();
				String startStr = "";
				String endStr = "";
				for (int i=0; i<tokAnnotList.size(); i++) {
					Annotation annot = tokAnnotList.get(i);
					if (annot.getStart() <= start && annot.getEnd() > start) {
						startIndex = i;
						startStr = annot.getValue();
					}
					if (annot.getStart() < end && annot.getEnd() >= end) {
						endIndex = i;
						endStr = annot.getValue();
						break;
					}
				}
				
				indexList.add(startIndex);
				indexList.add(endIndex);
				indexMap.put(SequenceUtilities.getStrFromToks(seq.getToks()), indexList);
				
				System.out.println("create dup: " + docID + "|" + SequenceUtilities.getStrFromToks(seq.getToks()) + "|" + startStr + " " + endStr);
			}
			
			AnnotationSequence seq = seqList.get(seqIndex);
			while (seq.getDocID() == currDocID) { 
				subSeqList.add(seq);
				seqIndex++;
				seq = seqList.get(seqIndex);
			}
			
			matchSequences(subSeqList, currDocID);
			
			
			conn.close();
			conn2.close();
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	private void matchSequences(List<AnnotationSequence> subSeqList, long docID) throws SQLException
	{
		int annotID = -1;
		pstmtAnnotID.setLong(1, docID);
		ResultSet rs = pstmtAnnotID.executeQuery();
		if (rs.next()) {
			annotID = rs.getInt(1);
		}

		int count = 0;
		for (AnnotationSequence seq : subSeqList) {
			List<String> toks = seq.getToks();
			List<Annotation> tokAnnotList = seq.getAnnotList(":token|string");
			String seqStr = SequenceUtilities.getStrFromToks(toks);
			List<Integer> indexList = indexMap.get(seqStr);
			
			if (indexList != null) {
				Annotation tokAnnot = tokAnnotList.get(indexList.get(0));
				Annotation tokAnnot2 = tokAnnotList.get(indexList.get(1));
				long annotStart = tokAnnot.getStart();
				long annotEnd = tokAnnot2.getEnd();
				
				System.out.println("matched dup: " + docID + "|" + seqStr + "|" + tokAnnot.getValue() + "|" + tokAnnot2.getValue());
				

				pstmtWriteAnnot.setInt(1, ++annotID);
				pstmtWriteAnnot.setString(2, docNamespace);
				pstmtWriteAnnot.setString(3, docTable);
				pstmtWriteAnnot.setLong(4, docID);
				pstmtWriteAnnot.setString(5, targetType);
				pstmtWriteAnnot.setLong(6, annotStart);
				pstmtWriteAnnot.setLong(7, annotEnd);
				pstmtWriteAnnot.addBatch();
				count++;
				
				if (count == 100) {
					pstmtWriteAnnot.executeBatch();
					conn2.commit();
					count = 0;
				}
			}
			
			
			seqIndex++;
			seqList.get(seqIndex);
		}
		
		pstmtWriteAnnot.executeBatch();
		conn2.commit();
	}
	
	public static void main(String[] args)
	{
		if (args.length != 3) {
			System.out.println("usage: user password config");
			System.exit(0);
		}
		
		AnnotateDuplicate dup = new AnnotateDuplicate();
		dup.init(args[0], args[1], args[2]);
		dup.annotate();
	}
}
