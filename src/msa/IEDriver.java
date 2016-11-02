package msa;

import java.io.*;
import java.sql.*;
import java.util.*;

import com.google.gson.Gson;

import gate.GateBatch;
import utils.db.DBConnection;

public class IEDriver
{
	private GateBatch gate;
	private GenMSADriver genMSADriver;
	private FilterPatterns filterPatt;
	private BestPatterns bestPatt;
	private AutoAnnotate autoAnnot;
	
	private Gson gson;
	
	//General
	
	private Connection conn;
	private String host;
	private String keyspace;
	private String msaKeyspace;
	private String dbType;
	private String dbName;
	private String docDBHost;
	private String docDBName;
	private String docDBType;
	private String docNamespace;
	private String docTable;
	private String provenance;
	
	private boolean write;
	private String schema;
	private boolean verbose;
	
	private List<Map<String, Object>> msaAnnotFilterList;
	private List<Double> scoreList;
	
	private String group;
	private String targetGroup;
	
	private String newDocQuery;
	private String annotTypeQuery;
	

	
	//Gate
	//private String gateDocQuery;
	private String docIDCol;
	private String docTextCol;
	private String tempDocFolder;
	private String gappFile;
	private String annotationTypes;
	private String annotInputTable;
	private String annotOutputTable;
	private String gatePluginsHome;
	private String gateSiteConfig;
	private String gateHome;
	private String gateLogFile;
	private String gateProvenance;
	
	
	//MSA
	private String msaDocQuery;
	private boolean punct;
	private int syntax;
	private int phrase;
	private int maxGaps;
	private boolean requireTarget;
	private String targetType;
	private String targetProvenance;
	private String tokType;
	private int msaMinRows;
	private int limit;
	private int msaBlockSize;
	
	
	
	//Filter
	private String filterDocQuery;
	private String indexTableName;
	private double filterThreshold;
	private int filterMinCount;
	private int blockSize;
	
	
	//Best
	private double bestThreshold;
	private int bestMinCount;
	
	
	//AutoAnnot
	private String autoDBQuery;
	private int profileMinTotal;
	private double profileMinPrec;
	private boolean writeAnnots;
	private boolean filterAllCaps;
	private String autoOutFile;
	private boolean writeAuto;
	private String runName;
	private String autoMatchTable;
	private boolean evalFlag;
	private String autoProvenance;
	
	
	//Global flags
	private boolean gateFlag;
	private boolean msaFlag;
	private boolean filterFlag;
	private boolean bestFlag;
	private boolean autoFlag;
	
	
	private Statement stmt;
	private PreparedStatement pstmtInsertFrameInstanceStatus;
	private PreparedStatement pstmtInsertDocStatus;
	private PreparedStatement pstmtGetNewDocs;
	private PreparedStatement pstmtGetDocsWithStatus;
	private PreparedStatement pstmtUpdateDocsWithStatus;
	private PreparedStatement pstmtTableLookup;

	
	private long sleep;
	
	
	public IEDriver()
	{
		gate = new GateBatch();
		genMSADriver = new GenMSADriver();
		filterPatt = new FilterPatterns();
		bestPatt = new BestPatterns();
		autoAnnot = new AutoAnnotate();
		
		gson = new Gson();
	}
	
	public void init(String user, String password, String config)
	{
		try {
			Properties props = new Properties();
			props.load(new FileReader(config));
			
			
			
			//General properties
			host = props.getProperty("host");
			dbName = props.getProperty("dbName");
			dbType = props.getProperty("dbType");
			
			docDBHost = props.getProperty("docDBHost");
			docDBName = props.getProperty("docDBName");
			docDBType = props.getProperty("docDBType");
			
			keyspace = props.getProperty("keyspace");
			msaKeyspace = props.getProperty("msaKeyspace");
			
			write = Boolean.parseBoolean(props.getProperty("write"));
			docNamespace = props.getProperty("docNamespace");
			docTable = props.getProperty("docTable");
			
			verbose = Boolean.parseBoolean(props.getProperty("verbose"));

			schema = props.getProperty("schema") + ".";
			sleep = Long.parseLong(props.getProperty("sleep"));
			
			gateFlag = Boolean.parseBoolean(props.getProperty("gateFlag"));
			msaFlag = Boolean.parseBoolean(props.getProperty("msaFlag"));
			filterFlag = Boolean.parseBoolean(props.getProperty("filterFlag"));
			bestFlag = Boolean.parseBoolean(props.getProperty("bestFlag"));
			autoFlag = Boolean.parseBoolean(props.getProperty("autoFlag"));
			
			newDocQuery = props.getProperty("newDocQuery");
			annotTypeQuery = props.getProperty("annotTypeQuery");
			
			
			//get Gate properties
			//gateDocQuery = props.getProperty("gateDocQuery");
			docIDCol = props.getProperty("docIDCol");
			docTextCol = props.getProperty("docTextCol");
			tempDocFolder = props.getProperty("tempDocFolder");
			gappFile = props.getProperty("gappFile");
			annotationTypes = props.getProperty("annotationTypes");
			annotInputTable = props.getProperty("annotationInputTable");
			annotOutputTable = props.getProperty("annotationOutputTable");
			gatePluginsHome = props.getProperty("gate.plugins.home");
			gateSiteConfig = props.getProperty("gate.site.config");
			gateHome = props.getProperty("gate.home");
			gateLogFile = props.getProperty("logFile");
			gateProvenance = props.getProperty("gateProvenance");
			
			
			//get MSA properties
			msaKeyspace = props.getProperty("msaKeyspace");
			group = props.getProperty("group");
			targetGroup = props.getProperty("targetGroup");
			msaDocQuery = props.getProperty("msaDocQuery");
			
			punct = Boolean.parseBoolean(props.getProperty("punctuation"));
			maxGaps = Integer.parseInt(props.getProperty("maxGaps"));
			syntax = Integer.parseInt(props.getProperty("syntax"));
			phrase = Integer.parseInt(props.getProperty("phrase"));
			requireTarget = Boolean.parseBoolean(props.getProperty("requireTarget"));
			targetType = props.getProperty("targetType");
			targetProvenance = props.getProperty("targetProvenance");
			tokType = props.getProperty("tokType");
			
			msaAnnotFilterList = new ArrayList<Map<String, Object>>();
			msaAnnotFilterList = gson.fromJson(props.getProperty("msaAnnotFilterList"), msaAnnotFilterList.getClass());
			
			scoreList = new ArrayList<Double>();
			scoreList = gson.fromJson(props.getProperty("scoreList"), scoreList.getClass());

			msaMinRows = Integer.parseInt(props.getProperty("msaMinRows"));
			limit = Integer.parseInt(props.getProperty("limit"));
			msaBlockSize = Integer.parseInt(props.getProperty("msaBlockSize"));
			
			filterDocQuery = props.getProperty("filterDocQuery");
			indexTableName = props.getProperty("indexTableName");
			
			
			
			
			//get FilterPatterns properties
			blockSize = Integer.parseInt(props.getProperty("blockSize"));
			filterThreshold = Double.parseDouble(props.getProperty("filterThreshold"));
			filterMinCount = Integer.parseInt(props.getProperty("filterMinCount"));
			
			
			
			//get BestPatterns properties
			bestThreshold = Double.parseDouble(props.getProperty("bestThreshold"));
			bestMinCount = Integer.parseInt(props.getProperty("bestMinCount"));
			
			
			//get AutoAnnot properties
			autoDBQuery = props.getProperty("autoDBQuery");
			profileMinTotal = Integer.parseInt(props.getProperty("profileMinTotal"));
			profileMinPrec = Double.parseDouble(props.getProperty("profileMinPrec"));
			writeAnnots = Boolean.parseBoolean(props.getProperty("writeAnnots"));
			filterAllCaps = Boolean.parseBoolean(props.getProperty("filterAllCaps"));
			autoOutFile = props.getProperty("autoOutFile");
			writeAuto = Boolean.parseBoolean(props.getProperty("writeAuto"));
			runName = props.getProperty("runName");
			autoMatchTable = props.getProperty("autoMatchTable");
			evalFlag = Boolean.parseBoolean(props.getProperty("evalFlag"));
			autoProvenance = props.getProperty("autoProvenance");
			
			
			
			
			conn = DBConnection.dbConnection(user, password, host, keyspace, dbType);
			
			pstmtInsertFrameInstanceStatus = conn.prepareStatement("insert into " + schema + "frame_instance_status (frame_instance_id, status) values (?,0)");
			pstmtInsertDocStatus = conn.prepareStatement("insert into " + schema + "document_status (document_namespace, document_table, document_id, status) "
				+ "values (?,?,?,0)");
			pstmtGetNewDocs = conn.prepareStatement("select document_namespace, document_table, document_id from frame_instance_document where frame_instance_id = ?");
			pstmtGetDocsWithStatus = conn.prepareStatement("select document_namespace, document_table, document_id from document_status where status = ?");
			pstmtUpdateDocsWithStatus = conn.prepareStatement("update document_status set status = ? where status = ?");
			pstmtTableLookup = conn.prepareStatement("select table_name from tablename_lookup where table_type = ? and annotation_type = ?");
			
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	public void close()
	{
		try {
			stmt.close();
			pstmtInsertFrameInstanceStatus.close();
			pstmtInsertDocStatus.close();
			pstmtGetNewDocs.close();
			pstmtGetDocsWithStatus.close();
			pstmtUpdateDocsWithStatus.close();
			pstmtTableLookup.close();
			
			conn.close();
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	public void run(String annotUser, String annotPassword, String docUser, String docPassword, String msaUser, String msaPassword)
	{
		try {
			//init gate batch processor
			Properties gateProps = new Properties();
			gateProps.setProperty("host", host);
			gateProps.setProperty("dbName", dbName);
			gateProps.setProperty("dbType", dbType);
			gateProps.setProperty("docDBHost", docDBHost);
			gateProps.setProperty("docDBName", docDBName);
			gateProps.setProperty("docDBType", docDBType);
			gateProps.setProperty("tempDocFolder", tempDocFolder);
			gateProps.setProperty("gappFile", gappFile);
			gateProps.setProperty("annotationTypes", annotationTypes);
			gateProps.setProperty("annotationInputTable", annotInputTable);
			gateProps.setProperty("annotationOutputTable", annotOutputTable);
			gateProps.setProperty("gate.plugins.home", gatePluginsHome);
			gateProps.setProperty("gate.site.config", gateSiteConfig);
			gateProps.setProperty("gate.home", gateHome);
			gateProps.setProperty("logFile", gateLogFile);
			gateProps.setProperty("dbWrite", Boolean.toString(write));
			gateProps.setProperty("docNamespace", docNamespace);
			gateProps.setProperty("docTable", docTable);
			gateProps.setProperty("docIDCol", docIDCol);
			gateProps.setProperty("docTextCol", docTextCol);
			gateProps.setProperty("provenance", gateProvenance);
			
			String gateDocQuery = "select a." + docIDCol + ", a." + docTextCol + " from " + schema + docTable + " a, document_status b "
				+ "where b.document_namespace = '" + docNamespace + "' and b.document_table = '" + docTable + "' and a." + docIDCol + " = b.document_id and b.status = 0 "
				+ "order by a." + docIDCol;
			
			gateProps.setProperty("docQuery", gateDocQuery);
			
			gate.init(gateProps);
			
			
			
			//init GenMSA

			Properties msaProps = new Properties();
			msaProps.setProperty("host", host);
			msaProps.setProperty("dbName", dbName);
			msaProps.setProperty("dbType", dbType);
			msaProps.setProperty("docDBHost", docDBHost);
			msaProps.setProperty("docDBName", docDBName);
			msaProps.setProperty("docDBType", docDBType);
			msaProps.setProperty("docDBQuery", msaDocQuery);
			msaProps.setProperty("keyspace", keyspace);
			msaProps.setProperty("msaKeyspace", msaKeyspace);
			msaProps.setProperty("docNamespace", docNamespace);
			msaProps.setProperty("docTable", docTable);
			
			msaProps.setProperty("group", group);
			msaProps.setProperty("targetGroup", targetGroup);
			msaProps.setProperty("punctuation", Boolean.toString(punct));
			msaProps.setProperty("verbose", Boolean.toString(verbose));
			msaProps.setProperty("maxGaps", Integer.toString(maxGaps));
			msaProps.setProperty("syntax", Integer.toString(syntax));
			msaProps.setProperty("phrase", Integer.toString(phrase));
			msaProps.setProperty("requireTarget", Boolean.toString(requireTarget));
			//msaProps.setProperty("targetType", targetType);
			msaProps.setProperty("targetProvenance", targetProvenance);
			msaProps.setProperty("tokType", tokType);
			msaProps.setProperty("msaAnnotFilterList", gson.toJson(msaAnnotFilterList));
			msaProps.setProperty("scoreList", gson.toJson(scoreList));

			msaProps.setProperty("msaMinRows", Integer.toString(msaMinRows));
			msaProps.setProperty("msaBlockSize", Integer.toString(msaBlockSize));
			msaProps.setProperty("limit", Integer.toString(limit));
			msaProps.setProperty("write", Boolean.toString(write));
			//msaProps.setProperty("msaTable", msaTable);
			
			genMSADriver.init(msaProps);
			

			
			
			
			
			//init FilterPatterns
			Properties pattProps = new Properties();
			pattProps.setProperty("host", host);
			pattProps.setProperty("dbName", dbName);
			pattProps.setProperty("dbType", dbType);
			pattProps.setProperty("docDBHost", docDBHost);
			pattProps.setProperty("docDBName", docDBName);
			pattProps.setProperty("docDBType", docDBType);
			pattProps.setProperty("docDBQuery", filterDocQuery);
			pattProps.setProperty("keyspace", keyspace);
			pattProps.setProperty("msaKeyspace", msaKeyspace);
			pattProps.setProperty("docNamespace", docNamespace);
			pattProps.setProperty("docTable", docTable);
			//pattProps.setProperty("profileTable", msaTable);
			
			pattProps.setProperty("group", group);
			pattProps.setProperty("targetGroup", targetGroup);
			pattProps.setProperty("punctuation", Boolean.toString(punct));
			pattProps.setProperty("verbose", Boolean.toString(verbose));
			pattProps.setProperty("maxGaps", Integer.toString(maxGaps));
			pattProps.setProperty("syntax", Integer.toString(syntax));
			pattProps.setProperty("phrase", Integer.toString(phrase));
			pattProps.setProperty("requireTarget", Boolean.toString(requireTarget));
			pattProps.setProperty("targetType", targetType);
			pattProps.setProperty("targetProvenance", targetProvenance);
			pattProps.setProperty("tokType", tokType);

			pattProps.setProperty("limit", Integer.toString(limit));
			
			pattProps.setProperty("indexTableName", indexTableName);

			
			pattProps.setProperty("msaAnnotFilterList", gson.toJson(msaAnnotFilterList));
			pattProps.setProperty("scoreList", gson.toJson(scoreList));
			
			pattProps.setProperty("filterThreshold", Double.toString(filterThreshold));
			pattProps.setProperty("filterMinCount", Integer.toString(filterMinCount));
			
			pattProps.setProperty("blockSize", Integer.toString(blockSize));
			pattProps.setProperty("write", Boolean.toString(write));
			
			filterPatt.init(pattProps);
			
			
			//Auto annot
			Properties autoProps = new Properties();
			autoProps.setProperty("host", host);
			autoProps.setProperty("dbType", dbType);
			autoProps.setProperty("docDBHost", docDBHost);
			autoProps.setProperty("docDBName", docDBName);
			autoProps.setProperty("docDBType", docDBType);
			autoProps.setProperty("docDBQuery", autoDBQuery);
			autoProps.setProperty("keyspace", keyspace);
			autoProps.setProperty("msaKeyspace", msaKeyspace);
			autoProps.setProperty("docNamespace", docNamespace);
			autoProps.setProperty("docTable", docTable);
			
			autoProps.setProperty("punctuation", Boolean.toString(punct));
			autoProps.setProperty("verbose", Boolean.toString(verbose));
			autoProps.setProperty("maxGaps", Integer.toString(maxGaps));
			autoProps.setProperty("syntax", Integer.toString(syntax));
			autoProps.setProperty("phrase", Integer.toString(phrase));
			autoProps.setProperty("requireTarget", Boolean.toString(requireTarget));
			autoProps.setProperty("targetType", targetType);
			autoProps.setProperty("targetProvenance", targetProvenance);
			autoProps.setProperty("tokType", tokType);

			autoProps.setProperty("limit", Integer.toString(limit));
			
			autoProps.setProperty("runName", runName);
			autoProps.setProperty("autoMatchTable", autoMatchTable);
						
			autoProps.setProperty("annotFilterList", gson.toJson(msaAnnotFilterList));
			autoProps.setProperty("scoreList", gson.toJson(scoreList));
			
			autoProps.setProperty("filterThreshold", Double.toString(filterThreshold));
			autoProps.setProperty("filterMinCount", Integer.toString(filterMinCount));
			
			autoProps.setProperty("blockSize", Integer.toString(blockSize));
			autoProps.setProperty("write", Boolean.toString(writeAuto));
			
			autoProps.setProperty("profileMinTotal", Integer.toString(profileMinTotal));
			autoProps.setProperty("profileMinPrec", Double.toString(profileMinPrec));
			autoProps.setProperty("writeAnnots", Boolean.toString(writeAnnots));
			autoProps.setProperty("filterAllCaps", Boolean.toString(filterAllCaps));
			autoProps.setProperty("autoOutFile", autoOutFile);
			autoProps.setProperty("evalFlag", Boolean.toString(evalFlag));
			autoProps.setProperty("autoProvenance", autoProvenance);
			
			autoAnnot.init(autoProps);

			
			
			

			stmt = conn.createStatement();
						
			boolean flag = true;
			
			List<String> annotTypeList = new ArrayList<String>();
			List<String> profileTableList = new ArrayList<String>();
			List<String> indexTableList = new ArrayList<String>();
			List<String> finalTableList = new ArrayList<String>();
			
			ResultSet rs = stmt.executeQuery(annotTypeQuery);
			while (rs.next()) {
				String annotType = rs.getString(1);
				annotTypeList.add(annotType);
				
				pstmtTableLookup.setString(2, annotType);
				pstmtTableLookup.setString(1, "profile");
				
				ResultSet rs2 = pstmtTableLookup.executeQuery();
				String profileTable = "";
				if (rs2.next()) {
					profileTable = rs2.getString(1);
				}
				profileTableList.add(profileTable);
				
				pstmtTableLookup.setString(1, "index");
				rs2 = pstmtTableLookup.executeQuery();
				String indexTable = "";
				if (rs2.next()) {
					indexTable = rs2.getString(1);
				}
				indexTableList.add(indexTable);

				
				pstmtTableLookup.setString(1, "final");
				rs2 = pstmtTableLookup.executeQuery();
				String finalTable = "";
				if (rs2.next()) {
					finalTable = rs2.getString(1);
				}
				finalTableList.add(finalTable);
			}
			
			
			while (flag) {
				//get docs from frameinstances that have been imported but not insert into the frame_instance_status table
						
				if (gateFlag) {
					List<DocBean> docList = checkNewDocuments();
					
					//run GATE pipeline
					if (docList.size() > 0) {
						gate.process(annotUser, annotPassword, docUser, docPassword);
						//updateDocsWithStatus(0, 1);
					}
				}
				
				
				//gen MSAs
				if (msaFlag) {
					//run MSA generator
					for (int i=0; i<annotTypeList.size(); i++) {
						String annotType = annotTypeList.get(i);
						String profileTable = profileTableList.get(i);
						
						genMSADriver.setTargetType(annotType);
						genMSADriver.setProfileTable(profileTable);
						genMSADriver.run(annotUser, annotPassword, docUser, docPassword);
						updateDocsWithStatus(1, 2);
					}
				}
				
				
				//filter patterns
				if (filterFlag) {
					//run filter patterns
					filterPatt.setAnnotTypeList(annotTypeList);
					filterPatt.setProfileTableList(profileTableList);
					filterPatt.setIndexTableList(indexTableList);
					filterPatt.filterPatterns(annotUser, annotPassword, docUser, docPassword, msaUser, msaPassword);
					updateDocsWithStatus(2, 3);
				}
				
				//best patterns
				if (bestFlag && filterPatt.getDocIDList().size() > 0) {
					for (int i=0; i<annotTypeList.size(); i++) {
						String annotType = annotTypeList.get(i);
						String profileTable = profileTableList.get(i);
						String indexTable = indexTableList.get(i);
						String finalTable = finalTableList.get(i);
						
						if (targetType.length() == 0 || profileTable.length() == 0 || indexTable.length() == 0 || finalTable.length() == 0)
							continue;
						
						bestPatt.getBestPatterns(msaUser, msaPassword, annotUser, annotPassword, host, keyspace, msaKeyspace, dbType, annotType, finalTable, 
							indexTable, profileTable, targetProvenance, bestThreshold, bestMinCount);
					}
				}
				
				//auto annotate
				if (autoFlag) {
					autoAnnot.setAnnotTypeList(annotTypeList);
					autoAnnot.setProfileTableList(profileTableList);
					autoAnnot.setFinalTableList(finalTableList);
					autoAnnot.annotate(msaUser, msaPassword, docUser, docPassword);
					if (writeAuto)
						autoAnnot.writeAnnotations();
					autoAnnot.close();
				}
				
				//sleep
				if (sleep >= 0) {
					if (verbose)
						System.out.println("sleeping for " + sleep + "ms...");
					Thread.sleep(sleep);
				}
				else
					break;
			}
			
			close();
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	private List<DocBean> checkNewDocuments() throws SQLException
	{
		List<DocBean> docList = new ArrayList<DocBean>();
		
		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery(newDocQuery);
		
		while (rs.next()) {
			
			long frameInstanceID = rs.getLong(1);
			
			pstmtInsertFrameInstanceStatus.setLong(1, frameInstanceID);
			pstmtInsertFrameInstanceStatus.execute();
			
			pstmtGetNewDocs.setLong(1, frameInstanceID);
			
			ResultSet rs2 = pstmtGetNewDocs.executeQuery();

			while (rs2.next()) {
				if (docList.size() == 15)
					break;

				String docNamespace = rs2.getString(1);
				String docTable = rs2.getString(2);
				long docID = rs2.getLong(3);
				
				pstmtInsertDocStatus.setString(1, docNamespace);
				pstmtInsertDocStatus.setString(2, docTable);
				pstmtInsertDocStatus.setLong(3, docID);
				
				try {
					pstmtInsertDocStatus.execute();
				}
				catch(SQLException e)
				{
					e.printStackTrace();
				}
				
				DocBean docBean = new DocBean(docNamespace, docTable, docID);
				docList.add(docBean);
			}
			
		}
		
		stmt.close();
		
		return docList;
	}
	
	private List<DocBean> getDocsWithStatus(int status) throws SQLException
	{
		List<DocBean> docList = new ArrayList<DocBean>();
		pstmtGetDocsWithStatus.setInt(1, status);
		ResultSet rs = pstmtGetDocsWithStatus.executeQuery();
		while (rs.next()) {
			String docNamespace = rs.getString(1);
			String docTable = rs.getString(2);
			long docID = rs.getLong(3);
			DocBean docBean = new DocBean(docNamespace, docTable, docID);
			docList.add(docBean);
		}
		
		return docList;
	}
	
	private void updateDocsWithStatus(int oldStatus, int newStatus) throws SQLException
	{
		List<DocBean> docList = new ArrayList<DocBean>();
		pstmtUpdateDocsWithStatus.setInt(1, newStatus);
		pstmtUpdateDocsWithStatus.setInt(2, oldStatus);
		pstmtUpdateDocsWithStatus.execute();
	}
	
	public static void main(String[] args)
	{
		if (args.length != 7) {
			System.out.println("usage: annotUser annotPassword docUser docPassword msaUser msaPassword config");
			System.exit(0);
		}
		
		IEDriver ie = new IEDriver();
		ie.init(args[0], args[1], args[6]);
		ie.run(args[0], args[1], args[2], args[3], args[4], args[5]);
	}
}
