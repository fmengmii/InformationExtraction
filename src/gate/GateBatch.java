
package gate;

import gate.Annotation;
import gate.Document;
import gate.Corpus;
import gate.CorpusController;
import gate.AnnotationSet;
import gate.FeatureMap;
import gate.Gate;
import gate.Factory;
import gate.util.*;
import gate.util.persistence.PersistenceManager;
import gate.corpora.DocumentImpl;

import java.nio.file.FileVisitResult;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.HashSet;
import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.BufferedOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;

import utils.db.DBConnection;

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.google.gson.Gson;


public class GateBatch
{

	private Connection conn;
	private Connection docConn;
	
	private Session session;
	private Cluster cluster;
	private Session docSession;
	private Cluster docCluster;
	
	private int batchSize;
	
	private Map<String, Boolean> annotationTypeMap;
	private boolean allFlag;
	private String dbType = "mysql";
	private String docDBType = "mysql";
	private String rq = "`";
	
	private PreparedStatement pstmt;
	private BoundStatement bstmt;
	private com.datastax.driver.core.PreparedStatement pstmtCheckDocAnnots;
	
	private Gson gson;
	private List<File> fileList;
	private ResultSet rs;
	private com.datastax.driver.core.ResultSet cassRS;
	private File docFile;
	private boolean hasText;
	private int currDoc;
	private int docID;
	
	private String host;
	private String dbName;
	
	private String tempDocFolder;
	
	private String docDBHost;
	private String docDBName;
	private String docNamespace;
	private String docTable;
	private String docIDCol;
	private String docTextCol;
	
	private String annotInputTable;
	private String annotOutputTable;
	private String fileRoot;
	
	private String logFileName;
	private PrintWriter logPW;
	
	private boolean dbWrite;
	private String provenance;
	private String[] loadAnnotationTypes;
	private String docQuery;
	
	private int firstFile = 0;

	private File gappFile = null;
	private List annotTypesToWrite = null;
	private String encoding = null;
	private boolean skip = false;
	
	private boolean verbose;
	
	private PreparedStatement pstmtGetText;
	
	
	
	//static private final String tempDocFolder = "/Users/frankmeng/Documents/Research/annotation/test-docs/";
	
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
			annotationTypeMap = new HashMap<String, Boolean>();
			gson = new Gson();
  
			tempDocFolder = props.getProperty("tempDocFolder");
		  
			String annotationTypesStr = props.getProperty("annotationTypes");
			if (annotationTypesStr != null) {
				String[] annotationTypes = annotationTypesStr.split(",");
		  
				for (int i=0; i<annotationTypes.length; i++) {
				  annotationTypeMap.put(annotationTypes[i].trim(), true);
				  
				  if (annotationTypes[i].toLowerCase().equals("all"))
						  allFlag = true;
				}
			}
		  
			dbType = props.getProperty("dbType");
			host = props.getProperty("host");
			dbName = props.getProperty("dbName");
		  
		  
			docDBType = props.getProperty("docDBType");
			docDBHost = props.getProperty("docDBHost");
			docDBName = props.getProperty("docDBName");
		  
			annotInputTable = props.getProperty("annotationInputTable");
			annotOutputTable = props.getProperty("annotationOutputTable");

			docNamespace = props.getProperty("docNamespace");
			docTable = props.getProperty("docTable");
			docIDCol = props.getProperty("docIDCol");
			docTextCol = props.getProperty("docTextCol");
			
			provenance = props.getProperty("provenance");
			verbose = Boolean.parseBoolean(props.getProperty("verbose"));
			
			String skipStr = props.getProperty("skip");
			if (skipStr != null) {
				skip = Boolean.parseBoolean(skipStr);
			}
		  
			System.setProperty("gate.plugins.home", props.getProperty("gate.plugins.home"));
			System.setProperty("gate.site.config", props.getProperty("gate.site.config"));
			System.setProperty("gate.home", props.getProperty("gate.home"));
		  
			batchSize = 1000;
			String batchSizeStr = props.getProperty("batchSize");
			if (batchSizeStr != null)
				batchSize = Integer.parseInt(batchSizeStr);
		  
			fileRoot = props.getProperty("fileRoot");
		  
		  
		  
			//log file
			logFileName = props.getProperty("logFile");
		  
			logPW = new PrintWriter(new FileWriter(logFileName));
		  
			//set db write flag
			String dbWriteStr = props.getProperty("dbWrite");
			dbWrite = true;
			if (dbWriteStr != null && dbWriteStr.length() > 0)
				dbWrite = Boolean.parseBoolean(props.getProperty("dbWrite"));
		  
			//load annotations
			String loadAnnotStr = props.getProperty("loadAnnotationTypes");
			loadAnnotationTypes = null;
			if (loadAnnotStr != null && loadAnnotStr.length() > 0) 
				loadAnnotationTypes = loadAnnotStr.split(",");
			
			docQuery = props.getProperty("docQuery");
			gappFile = new File(props.getProperty("gappFile"));
			
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	
  /**
   * The main entry point.  First we parse the command line options (see
   * usage() method for details), then we take all remaining command line
   * parameters to be file names to process.  Each file is loaded, processed
   * using the application and the results written to the output file
   * (inputFile.out.xml).
   */
	
	public void process(String user, String password, String docUser, String docPassword)
	{
		try {
			docFile = new File(tempDocFolder + "temp.txt");
			
			conn = DBConnection.dbConnection(user, password, host, dbName, dbType);					
			
			docConn = DBConnection.dbConnection(docUser, docPassword, docDBHost, docDBName, docDBType);
			Statement stmt = conn.createStatement();
			//ResultSet rs = stmt.executeQuery("select RECORD_ID, REPORT_IMPRESSION from RADIOLOGY_REPORTS");
			//rs = stmt.executeQuery("select " + docIDCol + ", " + docTextCol + " from " + docTable + " order by " + docIDCol);
			
			Statement stmt2 = docConn.createStatement();
			List<Long> docIDList = new ArrayList<Long>();
			ResultSet rs2 = stmt.executeQuery(docQuery);
			while (rs2.next()) {
				docIDList.add(rs2.getLong(1));
			}
			
			
			String queryStr = "insert into " + annotOutputTable
					+ " (id, document_namespace, document_table, document_id, document_name, annotation_type, start, " + rq + "end" + rq + ", value, features, provenance) "
					+ "	values (?,?,?,?,?,?,?,?,?,?,?)";
			
			pstmtGetText = docConn.prepareStatement("select " + docTextCol + " from " + docTable + " where " + docIDCol + " = ?");



			conn.setAutoCommit(false);
			rq = DBConnection.reservedQuote;
			
			pstmt = conn.prepareStatement(queryStr);
			  
			pstmt.setString(2, docNamespace);
			pstmt.setString(3, docTable);
			pstmt.setString(11, provenance);

		  
			// initialize GATE - this must be done before calling any GATE APIs
			Gate.init();
		
			// load the saved application
			CorpusController application =
					(CorpusController)PersistenceManager.loadObjectFromFile(gappFile);
		
			// Create a Corpus to use.  We recycle the same Corpus object for each
			// iteration.  The string parameter to newCorpus() is simply the
			// GATE-internal name to use for the corpus.  It has no particular
			// significance.
			Corpus corpus = Factory.newCorpus("BatchProcessApp Corpus");
			application.setCorpus(corpus);
		  
			fileList = new ArrayList<File>();
		
			if (fileRoot != null) {
				Path start = Paths.get(fileRoot);
				java.nio.file.Files.walkFileTree(start, new SimpleFileVisitor<Path>() {
					@Override
					public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException
					{
						fileList.add(file.toFile());
						return FileVisitResult.CONTINUE;
					}
				});
			}
		  
			int count = 0;
			currDoc = 0;
		  
			for (long docID : docIDList) {
				
				String reportText = "";
				hasText = false;
				pstmtGetText.setLong(1, docID);
				rs = pstmtGetText.executeQuery();
				if (rs.next())
					reportText = rs.getString(1);
				
				if (reportText != null && reportText.length() > 0) {
					  reportText = reportText.trim();
					  PrintWriter pw = new PrintWriter(new FileWriter(docFile));
					  pw.println(reportText);
					  pw.close();
					  hasText = true;
				  }
				
				if (!hasText)
					continue;
			  
				System.out.println("Processing document " + docID + "...");
				Document doc = Factory.newDocument(docFile.toURI().toURL(), encoding);
				DocumentContent docContent = doc.getContent();
		  
				boolean loaded = true;
				int maxAnnotID = 0;
				
				//if skip flag is set, check to see if annotations exist for this document
				if (skip && checkDocumentAnnotations(docID) > 0) {
					System.out.println("Skipping...");
					continue;
				}
				
				if (loadAnnotationTypes != null) {
					maxAnnotID = addAnnotationsFromDB(docID, doc.getAnnotations(), loadAnnotationTypes, annotInputTable);
					((DocumentImpl) doc).setNextAnnotationId(maxAnnotID+1);
			  
					//check to see if all annotations were loaded
					for (int i=0; i<loadAnnotationTypes.length; i++) {
						AnnotationSet loadAnnotSet = doc.getAnnotations().get(loadAnnotationTypes[i]);
						if (loadAnnotSet.size() == 0) {
							loaded = false;
							break;
						}
					}
				}
		  
				if (!loaded)
					continue;
		
				// put the document in the corpus
				corpus.add(doc);
		  
				// run the application
				try {
					application.execute();
				}
				catch(Exception e)
				{
					e.printStackTrace();
					logPW.println("Error in document: " + docID);
					logPW.println("Message: " + e.getMessage());
					logPW.println("\n\n");
					logPW.flush();
					System.exit(-1);
				}
		  
				System.out.println("Finished processing Gate...");
		  
				AnnotationSet defaultAnnots = doc.getAnnotations();
				Iterator iter = defaultAnnots.iterator();
		  
				while (iter.hasNext()) {
					Annotation annot = (Annotation) iter.next();
					
					if (annotationTypeMap.get(annot.getType()) != null || allFlag) {
						//System.out.println("inserting : " + annot.getType());
						//System.out.println(reportText.substring(annot.getStartNode().getOffset().intValue(), annot.getEndNode().getOffset().intValue()));
						if (dbWrite)
							insertIntoDB(annot, docID, docContent);
						
						count++;
					}
			  
					if (count % batchSize == 0) {
						if (!dbType.equals("cassandra")) {
							pstmt.executeBatch();
							conn.commit();
						}
					}
			  
				}
		  
				if (!dbType.equals("cassandra")) {
					pstmt.executeBatch();
					conn.commit();
				}
		  
				
				long check = checkDocumentAnnotations(docID);
				if (check != count) {
					System.out.println("Missing inserts: " + check + ", " + count);
					System.exit(0);
				}
				
				
				count = 0;
		  
				// remove the document from the corpus again
				corpus.remove(0);
				corpus.unloadDocument(doc);
				corpus.clear();
		  
				// Release the document, as it is no longer needed     
				Factory.deleteResource(doc);
			  
				currDoc++;
			}
		  
			logPW.close();
		  
			if (!dbType.equals("cassandra")) {
				pstmt.executeBatch();
				conn.commit();	  
				pstmt.close();
				pstmtGetText.close();
				stmt.close();
				stmt2.close();
				conn.close();
			}
			else {
				session.close();
				cluster.close();
			}
			
			if (!docDBType.equals("cassandra")) {
				docConn.close();
			}
			else {
				docSession.close();
				docCluster.close();
			}
			
			System.out.println("All done");
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}

  private int addAnnotationsFromDB(long docID2, AnnotationSet annotSet, String[] loadAnnotationTypes, String annotInputTable) throws SQLException, InvalidOffsetException
  {
	  System.out.println("loading annotations...");
	  Statement stmt = conn.createStatement();
	  StringBuilder strBlder = new StringBuilder("select id, annotation_type, start, " + rq + "end" + rq + ", features from " + annotInputTable + " where (");
	  
	  boolean first = true;
	  for (String annotType : loadAnnotationTypes) {
		  if (!first)
			  strBlder.append(" or ");
		  else
			  first = false;
		  strBlder.append("annotation_type = '" + annotType + "'");
	  }
	  
	  strBlder.append(") and document_id = " + docID2 + " order by id");
	  
	  ResultSet rs = stmt.executeQuery(strBlder.toString());
	  int maxAnnotID = -1;
	  while (rs.next()) {
		  maxAnnotID = rs.getInt(1);
		  FeatureMap fm = Factory.newFeatureMap();
		  String features = rs.getString(5);
		  Map<Object, Object> fm2 = new HashMap<Object, Object>();
		  fm2 = gson.fromJson(features, fm2.getClass());
		  for (Map.Entry<Object, Object> entry : fm2.entrySet()) {
			  fm.put(entry.getKey(), entry.getValue());
		  }
		  
		  annotSet.add(maxAnnotID, rs.getLong(3), rs.getLong(4), rs.getString(2), fm);
	  }
	  
	  rs.close();
	  stmt.close();
	  
	  return maxAnnotID;
  }

  private void insertIntoDB(Annotation annot, long docID2, DocumentContent docContent) throws SQLException, InvalidOffsetException
  {
	  String annotType = annot.getType();
	  long start = annot.getStartNode().getOffset();
	  long end = annot.getEndNode().getOffset();
	  int id = annot.getId();
	  String valueStr = docContent.getContent(start, end).toString();
	  
	  if (verbose)
		  System.out.println("docNamespace: " + docNamespace + " docTable: " + docTable + " docID: " + docID2 + " type: " + annotType 
			+ " start: " + start + " end: " + end + " id: " + id);
	  
	  String featureStr = getFeatureString(annot);
	  insertIntoRelational(id, docID2, annotType, (int) start, (int) end, valueStr, featureStr);
  }
  
  private void insertIntoRelational(int id, long docID, String annotType, int start, int end, String valueStr, String featureStr) throws SQLException
  {
	  String docName = docNamespace + "-" + docTable + "-" + docID;
	  pstmt.setInt(1, id);
	  //pstmt.setString(2, docNamespace);
	  //pstmt.setString(3, docTable);
	  pstmt.setLong(4, docID);
	  pstmt.setString(5, docName);
	  pstmt.setString(6, annotType);
	  pstmt.setLong(7, start);
	  pstmt.setLong(8, end);
	  pstmt.setString(9, valueStr);
	  pstmt.setString(10, featureStr);
	  pstmt.addBatch();
  }
  
  private void insertIntoCassandra(int id, int docID, String annotType, int start, int end, String valueStr, String featureStr) throws InvalidOffsetException
  { 
	  String docName = docNamespace + "-" + docTable + "-" + docID;

	  if (verbose) {
		  System.out.println(id + ", " + docNamespace + ", " + docTable + ", " + docID + ", " + docName + ", " 
				 + annotType + ", " + start + ", " + end + ", " + valueStr + ", " + featureStr + ", " + provenance);
	  }
	  session.execute(bstmt.bind(id, docNamespace, docTable, docID, docName, annotType, start, end, valueStr, featureStr, provenance));
  }
  
  private long checkDocumentAnnotations(long docID2) throws SQLException
  {
	  long count = 0;
	  
	  if (dbType.equals("cassandra")) {
		  com.datastax.driver.core.ResultSet rs = session.execute(pstmtCheckDocAnnots.bind(docID2));
		  
		  Iterator<Row> iter = rs.iterator();
		  if (iter.hasNext()) {
			  Row row = iter.next();
			  count = row.getLong(0);
		  }
	  }
	  else {
		  Statement stmt = conn.createStatement();
		  ResultSet rs = stmt.executeQuery("select count(*) from annotation where document_namespace = '" + docNamespace + "' and document_table = '" + docTable + "' "
		  	+ "and document_id = " + docID2 + " and provenance = '" + provenance + "'");
		  if (rs.next()) {
			  count = rs.getLong(1);
		  }
		  
		  stmt.close();
	  }
	  
	  return count;
  }
  
  private String getFeatureString(Annotation annot)
  {
	  FeatureMap featureMap = annot.getFeatures();
	  StringBuilder strBlder = new StringBuilder("{");
	  
	  Iterator iter2 = featureMap.keySet().iterator();
	  for (;iter2.hasNext();) {
		  
		  if (strBlder.length() > 1)
			  strBlder.append(",");

		  String key = (String) iter2.next();
		  Object obj =  featureMap.get(key);
		  String objStr = cleanObjString(obj.toString());
		  if (!(obj instanceof List))
			  //System.out.println(key + "=" + obj.toString());
			  strBlder.append("\"" + key + "\":\"" + objStr + "\"");
			  
		  else {
			  boolean first = true;
			  StringBuilder listStr = new StringBuilder("\"" + key + "\":[");
			  List valueList = (List) obj;
			  for (Object value : valueList) {
				  if (!first) {
					  listStr.append(",");
				  }
				  
				  //System.out.println(key + "=" + value.toString());
				  listStr.append("\"" + value.toString() + "\"");
				  first = false;
			  }
			  
			  listStr.append("]");
			  
			  strBlder.append(listStr);
		  }
		  

	  }
	  
	  strBlder.append("}");
	  
	  return strBlder.toString();
  }
  
  private String cleanObjString(String str)
  {
	  StringBuilder strBlder = new StringBuilder();
	  for (int i=0; i<str.length(); i++) {
		  char ch = str.charAt(i);
		  if (ch == '"') {
			  strBlder.append("\\\"");
		  }
		  else if (ch == '\\')
			  strBlder.append("\\\\");
		  else
			  strBlder.append(ch);
	  }
	  
	  return strBlder.toString();
  }
  
  private Object convertValue(String value)
  {
	  Object obj = value;
	  boolean converted = false;
	  try {
		  obj = Double.parseDouble(value);
		  converted = true;
	  }
	  catch(NumberFormatException e)
	  {
	  }
	  
	  if (!converted) {
		  try {
			  obj = Integer.parseInt(value);
			  converted = true;
		  }
		  catch(NumberFormatException e)
		  {
		  }
	  }
	  
	  if (!converted)
		  obj = value;
	  
	  return obj;
  }
  
  public static void main(String[] args)
  {

	  if (args.length != 5) {
		  System.out.println("usage: user password docUser docPassword config");
		  System.exit(0);
	  }
	  
	  GateBatch gateBatch = new GateBatch();
	  gateBatch.init(args[4]);
	  gateBatch.process(args[0], args[1], args[2], args[3]);
  }
  
}