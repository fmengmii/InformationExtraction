package util;

import java.io.*;
import java.util.*;
import java.sql.*;


import msa.Annotation;
import msa.db.MSADBException;
import msa.db.MSADBInterface;
import msa.db.MySQLDBInterface;
import utils.db.DBConnection;

public class GenCoNLLAnnotations
{
	private MSADBInterface db;
	private PreparedStatement pstmtSentAnnots;
	private PreparedStatement pstmtDocID;
	private PreparedStatement pstmtCheck;
	//private Cluster cluster;
	//private Session session;
	private Connection conn;
	private int annotID = 0;
	
	public GenCoNLLAnnotations()
	{
	}
	
	public void genAnnotations(String user, String password, String host, String keyspace, String fileName)
	{
		try {
			/*
			cluster = Cluster.builder().addContactPoint(host).withCredentials(user, password)
					.withRetryPolicy(DefaultRetryPolicy.INSTANCE)
					.withLoadBalancingPolicy(new TokenAwarePolicy(new DCAwareRoundRobinPolicy()))
					.build();
			
			session = cluster.connect(keyspace);
			*/
			
			conn = DBConnection.dbConnection(user, password, host, keyspace, "mysql");
			
			
			pstmtSentAnnots = conn.prepareStatement("select value, start, end, value, features "
					+ "from annotation where document_namespace = 'ner' and document_table = 'conll203_document' and "
					+ "document_id = ? and start >= ? and start < ? allow filtering");
			
			
			pstmtDocID = conn.prepareStatement("select document_id from conll2003_document where name = ?");
			
			pstmtCheck = conn.prepareStatement("select count(*) from annotation where document_namespace = 'ner' and "
				+ "document_table = 'conll2003_document' and document_id = ? and provenance = 'conll2003-token'");

			
			db = new MySQLDBInterface();
			db.init("fmeng", "fmeng", "10.9.94.203", "ner", null);
			//db.init("fmeng", "fmeng", "localhost", "ner", null);
			
			BufferedReader reader = new BufferedReader(new FileReader(fileName));
			String line = "";
			
			boolean start = false;
			String sourceDocName = "";
			List<String> sourceAnnotList = new ArrayList<String>();
			while ((line = reader.readLine()) != null) {
				//System.out.println(line);
				
				if (line.startsWith("DOC-START")) {
				//if (line.startsWith("DOC-START-25794newsML.txt")) {
					
					if (sourceDocName.length() > 0) {
						System.out.println("\n\nDoc: " + sourceDocName);

						pstmtDocID.setString(1, sourceDocName);
						ResultSet rs = pstmtDocID.executeQuery();

						if (rs.next()) {
							int docID = rs.getInt(1);
							annotID = 0;
							genDocAnnots(docID, sourceAnnotList);
						}
					}
					
					String[] parts = line.split(" ");
					int index = parts[0].indexOf("-");
					index = parts[0].indexOf("-", index+1);
					sourceDocName = parts[0].substring(index+1);
					
					sourceAnnotList = new ArrayList<String>();
				}
				else if (line.length() > 0) {
					String[] parts = line.split(" ");
					String sourceValue = parts[0];
					String sourceAnnot = parts[3];
					sourceAnnotList.add(sourceValue + "|||" + sourceAnnot);
				}				
			}
			
			if (sourceDocName.length() > 0) {
				System.out.println("Doc: " + sourceDocName);

				pstmtDocID.setString(1, sourceDocName);
				ResultSet rs = pstmtDocID.executeQuery();

				if (rs.next()) {
					int docID = rs.getInt(1);
					genDocAnnots(docID, sourceAnnotList);
				}
			}
			
			
			conn.close();
			db.close();
			reader.close();
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	private void genDocAnnots(int docID, List<String> sourceAnnotList) throws MSADBException
	{
		/*
		if (checkAnnotations(docID)) {
			System.out.println("skipping...");
			return;
		}
		*/
		
		List<Annotation> annotList = db.getAnnotations("ner", "conll2003_document", docID, -1, -1, "Token", null);
		
		String currAnnot = null;
		long lastEnd = -1;
		//StringBuffer currVal = new StringBuffer();
		int i=0; 
		int j=0;
		
		while (i < sourceAnnotList.size() && j < annotList.size()) {
			String[] parts = sourceAnnotList.get(i).split("\\|\\|\\|");
			
			Annotation annot = annotList.get(j);
			String annotVal = annot.getValue();
			
			
			System.out.println(parts[0] + ", " + annotVal);
			
			//long startIndex = annot.getStart();
			
			
			if (!parts[0].startsWith(annotVal) && !annotVal.startsWith(parts[0])) {
				System.out.println("Alignment Error!!");
				System.exit(-1);
			}
			
			StringBuffer strBuf = new StringBuffer(parts[0]);
			if (parts[0].length() < annot.getValue().length()) {
				int currLen = parts[0].length();

				while(!annot.getValue().endsWith(strBuf.toString()) && (currLen < annot.getValue().length())) {
					System.out.println(annotID + ", " + annot.getValue() + ", " + annot.getStart() + ", " + annot.getEnd() + ", " + strBuf.toString() + ", " + parts[1]);
					Annotation outAnnot = new Annotation(annotID++, parts[1], annot.getStart(), annot.getEnd(), annot.getValue(), new HashMap<String, Object>());
					db.writeAnnotation(outAnnot, "ner", "conll2003_document", (int) docID, "conll2003-token");
					i++;
					parts = sourceAnnotList.get(i).split("\\|\\|\\|");
					currLen += parts[0].length();
					String delim = " ";
					if (strBuf.toString().endsWith(".") || parts[0].equals(".") || strBuf.toString().endsWith("-") || parts[0].equals("-"))
						delim = "";
					
					strBuf.append(delim + parts[0]);
				}
			}
			
			if (strBuf.toString().length() > annot.getValue().length()) {
				long start = annot.getStart();
				long end = annot.getEnd();
				 while ((end - start) < strBuf.toString().length()) {
					System.out.println(annotID + ", " + annot.getValue() + ", " + annot.getStart() + ", " + annot.getEnd() + ", " + parts[1]);
					Annotation outAnnot = new Annotation(annotID++, parts[1], annot.getStart(), annot.getEnd(), annot.getValue(), new HashMap<String, Object>());
					db.writeAnnotation(outAnnot, "ner", "conll2003_document", (int) docID, "conll2003-token");
					
					j++;
					annot = annotList.get(j);
					end = annot.getEnd();
				}
			}
			
			System.out.println(annotID + ", " + annot.getValue() + ", " + annot.getStart() + ", " + annot.getEnd() + ", " + parts[1]);
			Annotation outAnnot = new Annotation(annotID++, parts[1], annot.getStart(), annot.getEnd(), annot.getValue(), new HashMap<String, Object>());
			db.writeAnnotation(outAnnot, "ner", "conll2003_document", (int) docID, "conll2003-token");
			
			i++;
			j++;
		}
	}
	
	public void combineAnnots(String user, String password, String host, String keyspace, String annotType)
	{
		try {
			conn = DBConnection.dbConnection(user, password, host, keyspace, "mysql");
			Statement stmt = conn.createStatement();
			
			db = new MySQLDBInterface();
			db.init(user, password, host, keyspace, null);
			
			long lastDocID = -1;
			int lastEnd = -1;
			int currStart = -1;
			StringBuilder currVal = null;
			long docID = -1;
			String lastVal = "";
			boolean sentStart = false;
			
			String stopAnnotType = "B" + annotType.substring(1);
			
			ResultSet rs = stmt.executeQuery("select document_id, start, end, value, annotation_type from annotation where "
				+ "(annotation_type = '" + annotType + "' or annotation_type = '" + stopAnnotType + "') and provenance = 'conll2003-token' order by document_id, start");
			while (rs.next()) {
				docID = rs.getLong(1);
				int start = rs.getInt(2);
				int end = rs.getInt(3);
				String value = rs.getString(4);
				String annotType2 = rs.getString(5);
				
				
				if (annotType.equals("O") && !Character.isUpperCase(value.charAt(0))) {
					lastVal = value;
					continue;
				}
					
				
				System.out.println("annot docID: " + docID + " start: " + start + " end: " + end + " value: " + value + " annotType: " + annotType2);
				
				if (docID == lastDocID && (start == lastEnd + 1 || start == lastEnd) && !annotType2.equals(stopAnnotType)) {
					currVal.append(" " + value);
				}
				else {
					if (currVal != null) {
						long annotDocID = docID;
						if (docID != lastDocID)
							annotDocID = lastDocID;

						if (!annotType.equals("O") || (annotType.equals("O") && currVal.indexOf(" ") < 0 && !sentStart && currStart > 0) || 
							(annotType.equals("O") && currVal.indexOf(" ") >= 0)) {
								annotDocID = lastDocID;
							Annotation outAnnot = new Annotation(annotID++, annotType, currStart, lastEnd, currVal.toString().trim(), new HashMap<String, Object>());
							db.writeAnnotation(outAnnot, keyspace, "conll2003_document", (int) annotDocID, "conll2003-entity");
							System.out.println("combined start: " + currStart + " end: " + lastEnd + " value: " + currVal.toString());
						}
						else
							System.out.println("single entity sent start " + annotDocID + "|" + currStart + "|" + currVal.toString());
					}

					currStart = start;
					currVal = new StringBuilder(value);
					if (lastVal.equals("."))
						sentStart = true;
					else
						sentStart = false;
				}
				

				lastEnd = end;
				lastDocID = docID;
				lastVal = value;
			}
			
			Annotation outAnnot = new Annotation(annotID++, annotType, currStart, lastEnd, currVal.toString().trim(), new HashMap<String, Object>());
			db.writeAnnotation(outAnnot, keyspace, "conll2003_document", (int) docID, "conll2003-entity");
			System.out.println("combined start: " + currStart + " end: " + lastEnd + " value: " + currVal.toString());
			
			stmt.close();
			conn.close();
			db.close();
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	private boolean checkAnnotations(int docID) throws MSADBException
	{
		try {
			pstmtCheck.setInt(1, docID);
			ResultSet rs = pstmtCheck.executeQuery();
			
			if (rs.next()) {
				int count = rs.getInt(1);
				if (count > 0)
					return true;
			}
		}
		catch(Exception e)
		{
			throw new MSADBException(e);
		}
			
		return false;
	}
	
	public static void main(String[] args)
	{
		if (args.length != 5) {
			System.out.println("usage: user password host dbName annotType");
			System.exit(0);
		}
		
		GenCoNLLAnnotations gen = new GenCoNLLAnnotations();
		//gen.genAnnotations("fmeng", "fmeng", "10.9.94.203", "ner", "/Users/frankmeng/Documents/Research/msa-ie/data/ner/eng.train");
		//gen.genAnnotations("fmeng", "fmeng", "10.9.94.203", "ner", "/Users/frankmeng/Documents/Research/msa-ie/data/ner/eng.testa");
		//gen.genAnnotations("fmeng", "fmeng", "10.9.94.203", "ner", "/Users/frankmeng/Documents/Research/msa-ie/data/ner/eng.testb");
		gen.combineAnnots(args[0], args[1], args[2], args[3], args[4]);
	}
}
