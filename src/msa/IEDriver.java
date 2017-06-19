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
	private PopulateFrame pop;
	
	private Gson gson;
	
	//General
	
	private Connection conn;
	private String host;
	//private String keyspace;
	//private String msaKeyspace;
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
	private String docSchema;
	private boolean verbose;
	
	private List<Map<String, Object>> msaAnnotFilterList;
	private List<Double> scoreList;
	
	private String group;
	private String targetGroup;
	
	private String newDocQuery;
	private String annotTypeQuery;

	private String profileTableName;
	private String indexTableName;
	private String finalTableName;


	

	
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
	private String gateDocQuery;
	
	
	//MSA
	private String msaDocQuery;
	private boolean punct;
	private int syntax;
	private int phrase;
	private int maxGaps;
	private boolean requireTarget;
	//private String targetType;
	private String targetProvenance;
	private String tokType;
	private int msaMinRows;
	private int targetMinRows;
	private int limit;
	private int msaBlockSize;
	
	
	
	//Filter
	private String filterDocQuery;
	private double negFilterThreshold;
	private int negFilterMinCount;
	private double posFilterThreshold;
	private int posFilterMinCount;
	private int blockSize;
	private int clusterSize;
	
	
	//Best
	private double negThreshold;
	private int negMinCount;
	private double posThreshold;
	private int posMinCount;
	
	
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
	private boolean populateFlag;
	private boolean incrementalFlag;
	
	
	private Statement stmt;
	private PreparedStatement pstmtInsertFrameInstanceStatus;
	private PreparedStatement pstmtInsertDocStatus;
	private PreparedStatement pstmtGetNewDocs;
	private PreparedStatement pstmtGetDocsWithStatus;
	private PreparedStatement pstmtUpdateDocsWithStatus;
	private PreparedStatement pstmtUpdateDocsWithStatusDocID;
	private PreparedStatement pstmtTableLookup;
	private PreparedStatement pstmtCheckAutoStatus;
	private PreparedStatement pstmtInsertAutoStatus;
	private PreparedStatement pstmtUpdateAutoStatus;
	private PreparedStatement pstmtGetMinAutoDocID;
	private PreparedStatement pstmtGetMaxAutoDocID;
	private PreparedStatement pstmtGetMaxAutoProfileID;
	private PreparedStatement pstmtGetAnnotCount;
	private PreparedStatement pstmtResetGenMSAStatus;
	private PreparedStatement pstmtInsertGenMSAStatus;
	private PreparedStatement pstmtUpdateGenMSAStatus;
	private PreparedStatement pstmtGetGenMSAStatus;
	private PreparedStatement pstmtGetGenFilterStatus;
	private PreparedStatement pstmtInsertGenFilterStatus;
	private PreparedStatement pstmtUpdateGenFilterStatus;
	private PreparedStatement pstmtGetAutoDocIDs;
	private PreparedStatement pstmtSetFrameInstanceLocks;
	private PreparedStatement pstmtDeleteFrameInstanceLocks;
	

	
	private long sleep;
	
	
	public IEDriver()
	{
		gate = new GateBatch();
		genMSADriver = new GenMSADriver();
		filterPatt = new FilterPatterns();
		bestPatt = new BestPatterns();
		autoAnnot = new AutoAnnotate();
		pop = new PopulateFrame();
		
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
			
			//keyspace = props.getProperty("keyspace");
			//msaKeyspace = props.getProperty("msaKeyspace");
			
			write = Boolean.parseBoolean(props.getProperty("write"));
			docNamespace = props.getProperty("docNamespace");
			docTable = props.getProperty("docTable");
			
			verbose = Boolean.parseBoolean(props.getProperty("verbose"));

			schema = props.getProperty("schema");
			docSchema = props.getProperty("docSchema");
			sleep = Long.parseLong(props.getProperty("sleep"));
			
			gateFlag = Boolean.parseBoolean(props.getProperty("gateFlag"));
			msaFlag = Boolean.parseBoolean(props.getProperty("msaFlag"));
			filterFlag = Boolean.parseBoolean(props.getProperty("filterFlag"));
			bestFlag = Boolean.parseBoolean(props.getProperty("bestFlag"));
			autoFlag = Boolean.parseBoolean(props.getProperty("autoFlag"));
			
			newDocQuery = props.getProperty("newDocQuery");
			annotTypeQuery = props.getProperty("annotTypeQuery");
			
			incrementalFlag = Boolean.parseBoolean(props.getProperty("incrementalFlag"));
			
			
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
			gateDocQuery = props.getProperty("gateDocQuery");
			
			
			//get MSA properties
			//msaKeyspace = props.getProperty("msaKeyspace");
			group = props.getProperty("group");
			targetGroup = props.getProperty("targetGroup");
			msaDocQuery = props.getProperty("msaDocQuery");
			
			punct = Boolean.parseBoolean(props.getProperty("punctuation"));
			maxGaps = Integer.parseInt(props.getProperty("maxGaps"));
			syntax = Integer.parseInt(props.getProperty("syntax"));
			phrase = Integer.parseInt(props.getProperty("phrase"));
			requireTarget = Boolean.parseBoolean(props.getProperty("requireTarget"));
			//targetType = props.getProperty("targetType");
			targetProvenance = props.getProperty("targetProvenance");
			tokType = props.getProperty("tokType");
			
			msaAnnotFilterList = new ArrayList<Map<String, Object>>();
			msaAnnotFilterList = gson.fromJson(props.getProperty("msaAnnotFilterList"), msaAnnotFilterList.getClass());
			
			scoreList = new ArrayList<Double>();
			scoreList = gson.fromJson(props.getProperty("scoreList"), scoreList.getClass());

			msaMinRows = Integer.parseInt(props.getProperty("msaMinRows"));
			targetMinRows = Integer.parseInt(props.getProperty("targetMinRows"));
			limit = Integer.parseInt(props.getProperty("limit"));
			msaBlockSize = Integer.parseInt(props.getProperty("msaBlockSize"));
			
			filterDocQuery = props.getProperty("filterDocQuery");
			profileTableName = props.getProperty("profileTableName");
			indexTableName = props.getProperty("indexTableName");
			finalTableName = props.getProperty("finalTableName");
			
			
			
			
			//get FilterPatterns properties
			blockSize = Integer.parseInt(props.getProperty("blockSize"));
			negFilterThreshold = Double.parseDouble(props.getProperty("negFilterThreshold"));
			negFilterMinCount = Integer.parseInt(props.getProperty("negFilterMinCount"));
			posFilterThreshold = Double.parseDouble(props.getProperty("posFilterThreshold"));
			posFilterMinCount = Integer.parseInt(props.getProperty("posFilterMinCount"));
			clusterSize = Integer.parseInt(props.getProperty("clusterSize"));
			
			
			
			//get BestPatterns properties
			negThreshold = Double.parseDouble(props.getProperty("negThreshold"));
			negMinCount = Integer.parseInt(props.getProperty("negMinCount"));
			posThreshold = Double.parseDouble(props.getProperty("posThreshold"));
			posMinCount = Integer.parseInt(props.getProperty("posMinCount"));
			
			
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
			
			//get Populate properties
			populateFlag = Boolean.parseBoolean(props.getProperty("populateFlag"));
			
			
			
			
			conn = DBConnection.dbConnection(user, password, host, dbName, dbType);
			
			String schema2 = schema + ".";
			pstmtInsertFrameInstanceStatus = conn.prepareStatement("insert into " + schema2 + "frame_instance_status (frame_instance_id, status) values (?,0)");
			pstmtInsertDocStatus = conn.prepareStatement("insert into " + schema2 + "document_status (document_namespace, document_table, document_id, status) "
				+ "values (?,?,?,0)");
			pstmtGetNewDocs = conn.prepareStatement("select document_namespace, document_table, document_id from " + schema2 + "frame_instance_document where frame_instance_id = ?");
			pstmtGetDocsWithStatus = conn.prepareStatement("select document_namespace, document_table, document_id from " + schema2 + "document_status where status = ?");
			pstmtUpdateDocsWithStatus = conn.prepareStatement("update " + schema2 + "document_status set status = ? where status = ?");
			pstmtUpdateDocsWithStatusDocID = conn.prepareStatement("update " + schema2 + "document_status set status = ? where status = ? and document_id = ?");
			pstmtUpdateGenFilterStatus = conn.prepareStatement("update " + schema2 + "gen_filter_status set document_id = ? where annotation_type = ?");
			pstmtTableLookup = conn.prepareStatement("select table_name from " + schema2 + "tablename_lookup where table_type = ? and annotation_type = ?");
			pstmtInsertAutoStatus = conn.prepareStatement("insert into " + schema2 + "auto_status (annotation_type, profile_id, document_id) values (?,0,0)");
			pstmtGetMinAutoDocID = conn.prepareStatement("select document_id from " + schema2 + "filter_status where annotation_type = ?");
			pstmtGetMaxAutoDocID = conn.prepareStatement("select max(document_id) from " + schema2 + "document_status where status = 0");
			pstmtCheckAutoStatus = conn.prepareStatement("select profile_id, document_id from " + schema2 + "auto_status where annotation_type = ?");
			pstmtUpdateAutoStatus = conn.prepareStatement("update " + schema2 + "auto_status set document_id = ?, profile_id = ? where annotation_type = ?");
			pstmtGetMaxAutoProfileID = conn.prepareStatement("select max(profile_id) from " + schema2 + "profile where annotation_type = ? and profile_type = ?");
			pstmtGetAnnotCount = conn.prepareStatement("select count(*) from " + schema2 + "annotation a, " + schema2 + "document_status b where a.annotation_type = ? and b.status = 1 and "
				+ "a.document_namespace = b.document_namespace and a.document_table = b.document_table and a.document_id = b.document_id");
			pstmtResetGenMSAStatus = conn.prepareStatement("update " + schema2 + "gen_msa_status set profile_count = 0");
			pstmtInsertGenMSAStatus = conn.prepareStatement("insert into " + schema2 + "gen_msa_status (annotation_type, profile_count) values (?,?)");
			pstmtUpdateGenMSAStatus = conn.prepareStatement("update " + schema2 + "gen_msa_status set profile_count = ? where annotation_type = ?");
			pstmtGetGenMSAStatus = conn.prepareStatement("select profile_count from " + schema2 + "gen_msa_status where annotation_type = ?");
			pstmtGetGenFilterStatus = conn.prepareStatement("select document_id from " + schema2 + "gen_filter_status");
			pstmtInsertGenFilterStatus = conn.prepareStatement("insert into " + schema2 + "gen_filter_status (document_id) values (?)");
			pstmtUpdateGenFilterStatus = conn.prepareStatement("update " + schema2 + "gen_filter_status set document_id = ?");
			
			pstmtGetAutoDocIDs = conn.prepareStatement(autoDBQuery);
			pstmtSetFrameInstanceLocks = conn.prepareStatement("insert into " + schema2 + "frame_instance_lock (frame_instance_id, username) values (?,?)");
			pstmtDeleteFrameInstanceLocks = conn.prepareStatement("delete from " + schema2 + "frame_instance_lock where username = ?");
			pstmtSetFrameInstanceLocks.setString(2, "msa-ie");
			pstmtDeleteFrameInstanceLocks.setString(1, "msa-ie");
			
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
			
			genMSADriver.close();
			filterPatt.close();
			//pop.close();
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	public void run(String user, String password, String docUser, String docPassword)
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
			gateProps.setProperty("schema", schema);
			gateProps.setProperty("docSchema", docSchema);
			
			//String gateDocQuery = "select a." + docIDCol + ", a." + docTextCol + " from " + schema + docTable + " a, document_status b "
			//	+ "where b.document_namespace = '" + docNamespace + "' and b.document_table = '" + docTable + "' and a." + docIDCol + " = b.document_id and b.status = 0 "
			//	+ "order by a." + docIDCol;
			
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
			//msaProps.setProperty("keyspace", keyspace);
			//msaProps.setProperty("msaKeyspace", msaKeyspace);
			msaProps.setProperty("docNamespace", docNamespace);
			msaProps.setProperty("docTable", docTable);
			msaProps.setProperty("schema", schema);
			
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
			msaProps.setProperty("targetMinRows", Integer.toString(targetMinRows));
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
			//pattProps.setProperty("keyspace", keyspace);
			//pattProps.setProperty("msaKeyspace", msaKeyspace);
			pattProps.setProperty("docNamespace", docNamespace);
			pattProps.setProperty("docTable", docTable);
			//pattProps.setProperty("profileTable", msaTable);
			pattProps.setProperty("schema", schema);
			
			pattProps.setProperty("group", group);
			pattProps.setProperty("targetGroup", targetGroup);
			pattProps.setProperty("punctuation", Boolean.toString(punct));
			pattProps.setProperty("verbose", Boolean.toString(verbose));
			pattProps.setProperty("maxGaps", Integer.toString(maxGaps));
			pattProps.setProperty("syntax", Integer.toString(syntax));
			pattProps.setProperty("phrase", Integer.toString(phrase));
			pattProps.setProperty("requireTarget", Boolean.toString(requireTarget));
			//pattProps.setProperty("targetType", targetType);
			pattProps.setProperty("targetProvenance", targetProvenance);
			pattProps.setProperty("tokType", tokType);
			
			pattProps.setProperty("clusterSize", Integer.toString(clusterSize));

			pattProps.setProperty("limit", Integer.toString(limit));
			
			pattProps.setProperty("indexTableName", indexTableName);

			
			pattProps.setProperty("msaAnnotFilterList", gson.toJson(msaAnnotFilterList));
			pattProps.setProperty("scoreList", gson.toJson(scoreList));
			
			pattProps.setProperty("negFilterThreshold", Double.toString(negFilterThreshold));
			pattProps.setProperty("negFilterMinCount", Integer.toString(negFilterMinCount));
			pattProps.setProperty("posFilterThreshold", Double.toString(posFilterThreshold));
			pattProps.setProperty("posFilterMinCount", Integer.toString(posFilterMinCount));
			
			pattProps.setProperty("blockSize", Integer.toString(blockSize));
			pattProps.setProperty("write", Boolean.toString(write));
			
			filterPatt.init(pattProps);
			
			
			//best patts props
			Properties bestProps = new Properties();
			
			bestProps.setProperty("host", host);
			bestProps.setProperty("dbName", dbName);
			//bestProps.setProperty("keyspace", keyspace);
			//bestProps.setProperty("annotKeyspace", keyspace);
			//bestProps.setProperty("msaKeyspace", msaKeyspace);
			bestProps.setProperty("dbType", dbType);
			bestProps.setProperty("schema", schema);
			//bestProps.setProperty("annotType", annotType);
			//bestProps.setProperty("finalTable", finalTable);
			//bestProps.setProperty("indexTable", indexTableName);
			//bestProps.setProperty("profileTable", profileTableName);
			bestProps.setProperty("provenance", targetProvenance);
			bestProps.setProperty("negThreshold", Double.toString(negThreshold));
			bestProps.setProperty("negMinCount", Integer.toString(negMinCount));
			bestProps.setProperty("posThreshold", Double.toString(posThreshold));
			bestProps.setProperty("posMinCount", Integer.toString(posMinCount));
			
			bestPatt.init(bestProps);
			
			
			//Auto annot
			Properties autoProps = new Properties();
			autoProps.setProperty("host", host);
			autoProps.setProperty("dbName", dbName);
			autoProps.setProperty("dbType", dbType);
			autoProps.setProperty("docDBHost", docDBHost);
			autoProps.setProperty("docDBName", docDBName);
			autoProps.setProperty("docDBType", docDBType);
			autoProps.setProperty("docDBQuery", autoDBQuery);
			//autoProps.setProperty("keyspace", keyspace);
			//autoProps.setProperty("msaKeyspace", msaKeyspace);
			autoProps.setProperty("docNamespace", docNamespace);
			autoProps.setProperty("docTable", docTable);
			autoProps.setProperty("schema", schema);
			
			autoProps.setProperty("punctuation", Boolean.toString(punct));
			autoProps.setProperty("verbose", Boolean.toString(verbose));
			autoProps.setProperty("maxGaps", Integer.toString(maxGaps));
			autoProps.setProperty("syntax", Integer.toString(syntax));
			autoProps.setProperty("phrase", Integer.toString(phrase));
			autoProps.setProperty("requireTarget", Boolean.toString(requireTarget));
			//autoProps.setProperty("targetType", targetType);
			autoProps.setProperty("targetProvenance", targetProvenance);
			autoProps.setProperty("tokType", tokType);

			autoProps.setProperty("limit", Integer.toString(limit));
			
			autoProps.setProperty("runName", runName);
			autoProps.setProperty("autoMatchTable", autoMatchTable);
						
			autoProps.setProperty("msaAnnotFilterList", gson.toJson(msaAnnotFilterList));
			autoProps.setProperty("scoreList", gson.toJson(scoreList));
			
			//autoProps.setProperty("filterThreshold", Double.toString(filterThreshold));
			//autoProps.setProperty("filterMinCount", Integer.toString(filterMinCount));
			
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

			
			pop.init(conn, schema);
			
			

			stmt = conn.createStatement();
						
			boolean flag = true;
			
			List<String> annotTypeList = new ArrayList<String>();
			List<String> profileTableList = new ArrayList<String>();
			List<String> indexTableList = new ArrayList<String>();
			List<String> finalTableList = new ArrayList<String>();
			Map<String, Boolean> requireTargetMap = new HashMap<String, Boolean>();
			
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
				//profileTableList.add(profileTable);
				profileTableList.add(profileTableName);
				
				
				pstmtTableLookup.setString(1, "index");
				rs2 = pstmtTableLookup.executeQuery();
				String indexTable = "";
				if (rs2.next()) {
					indexTable = rs2.getString(1);
				}
				//indexTableList.add(indexTable);
				indexTableList.add(indexTableName);

				
				pstmtTableLookup.setString(1, "final");
				rs2 = pstmtTableLookup.executeQuery();
				String finalTable = "";
				if (rs2.next()) {
					finalTable = rs2.getString(1);
				}
				//finalTableList.add(finalTable);
				finalTableList.add(finalTableName);
			}
			
			/*
			rs = stmt.executeQuery("select a.annotation_type, b.element_type from slot a, element b, value c, element_value d "
					+ "where a.slot_id = c.slot_id and d.value_id = c.value_id and b.element_id = c.element_id");
			while (rs.next()) {
				String annotType = rs.getString(1);
				int elementType = rs.getInt(2);
				
				boolean requireTarget = false;
				if (elementType == 1)
					requireTarget = true;
				
				requireTargetMap.put(annotType, requireTarget);
			}
			*/
			
			
			while (flag) {
				//get docs from frameinstances that have been imported but not insert into the frame_instance_status table
						
				if (gateFlag) {
					System.out.println("** GATE **");
					List<DocBean> docList = checkNewDocuments();
					
					//run GATE pipeline
					if (docList.size() > 0) {
						gate.process(user, password, docUser, docPassword);
						//updateDocsWithStatus(0, 1);
					}
				}
				
				
				List<String> activeAnnotTypeList = new ArrayList<String>();
				List<DocBean> docList = new ArrayList<DocBean>();
				long minDocID = -1;
				
				//gen MSAs
				if (msaFlag) {
					System.out.println("** Gen MSA **");
					//run MSA generator
					
					
					
					for (int i=0; i<annotTypeList.size(); i++) {
					//for (int i=0; i<2; i++) {
						String annotType = annotTypeList.get(i);
						String profileTable = profileTableList.get(i);
						
						int annotCount = 0;
						pstmtGetAnnotCount.setString(1, annotType);
						rs = pstmtGetAnnotCount.executeQuery();
						if (rs.next()) {
							annotCount = rs.getInt(1);
						}
						

						int currCount = -1;
						pstmtGetGenMSAStatus.setString(1, annotType);
						rs = pstmtGetGenMSAStatus.executeQuery();
						if (rs.next()) {
							currCount = rs.getInt(1);
						}
						
						if (currCount == -1) {
							pstmtInsertGenMSAStatus.setString(1, annotType);
							pstmtInsertGenMSAStatus.setInt(2, 0);
							pstmtInsertGenMSAStatus.execute();
							currCount = 0;
						}
						
						minDocID = -1;
						rs = pstmtGetGenFilterStatus.executeQuery();
						if (rs.next())
							minDocID = rs.getLong(1);


						if (annotCount > currCount) {
							activeAnnotTypeList.add(annotType);
							genMSADriver.setTargetType(annotType);
							genMSADriver.setProfileTable(profileTable);
							//genMSADriver.setRequireTarget(requireTargetMap.get(annotType));
							genMSADriver.run(user, password, docUser, docPassword);
							
							pstmtUpdateGenMSAStatus.setInt(1, annotCount);
							pstmtUpdateGenMSAStatus.setString(2, annotType);
							pstmtUpdateGenMSAStatus.execute();
							
							docList = genMSADriver.getDocList();
						}
					}
					
					//gen relation patterns
					
				}
				
				
				//filter patterns
				if (filterFlag) {
					System.out.println("** FILTER **");
					//run filter patterns
					
					//filterPatt.setMinDocIDList(minDocIDList);
					filterPatt.setAnnotTypeList(activeAnnotTypeList);
					filterPatt.setProfileTableList(profileTableList);
					filterPatt.setIndexTableList(indexTableList);
					filterPatt.filterPatterns(user, password, docUser, docPassword, user, password);
					//updateDocsWithStatus(2, 3);
				}
				
				if (docList.size() > 0) {
					updateDocsWithStatusDocID(1, 2, docList);
					pstmtResetGenMSAStatus.execute();
				}

				
				//best patterns
				if (bestFlag && filterPatt.getDocIDMap().size() > 0) {
					System.out.println("** BEST **");
					bestPatt.setAnnotTypeList(activeAnnotTypeList);
					bestPatt.setProfileTableList(profileTableList);
					bestPatt.setIndexTableList(indexTableList);
					bestPatt.setFinalTableList(finalTableList);					
					bestPatt.getBestPatterns(user, password, user, password);
				}
				
				//auto annotate
				if (autoFlag) {
					System.out.println("** AUTO **");
					
					//set frame instance locks
					ResultSet rs2 = pstmtGetAutoDocIDs.executeQuery();
					while (rs2.next()) {
						long docID = rs2.getLong(1);
						pstmtSetFrameInstanceLocks.setLong(1, docID);
						
						try {
							pstmtSetFrameInstanceLocks.execute();
						}
						catch(SQLException e)
						{
							//there is already a lock on this document
						}
					}
					
					autoAnnot.setAnnotTypeList(activeAnnotTypeList);
					autoAnnot.setProfileTableList(profileTableList);
					autoAnnot.setFinalTableList(finalTableList);
					autoAnnot.annotate(user, password, docUser, docPassword);
					
					pstmtDeleteFrameInstanceLocks.execute();
					
					autoAnnot.close();
				}
				
				
				//populate
				if (populateFlag) {
					System.out.println("*** POPULATE ***");
					pop.populate();
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
				//if (docList.size() == 30)
				//	break;

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
		pstmtUpdateDocsWithStatus.setInt(1, newStatus);
		pstmtUpdateDocsWithStatus.setInt(2, oldStatus);
		pstmtUpdateDocsWithStatus.execute();
	}
	
	private void updateDocsWithStatusDocID(int oldStatus, int newStatus, List<DocBean> docList) throws SQLException
	{
		for (DocBean docBean : docList) {
			pstmtUpdateDocsWithStatusDocID.setInt(1, newStatus);
			pstmtUpdateDocsWithStatusDocID.setInt(2, oldStatus);
			pstmtUpdateDocsWithStatusDocID.setLong(3, docBean.getDocID());
			pstmtUpdateDocsWithStatusDocID.execute();
		}
	}
	
	public static void main(String[] args)
	{
		if (args.length != 5) {
			System.out.println("usage: user password docUser docPassword config");
			System.exit(0);
		}
		
		IEDriver ie = new IEDriver();
		ie.init(args[0], args[1], args[4]);
		ie.run(args[0], args[1], args[2], args[3]);
	}
}
