package msa;

import java.io.*;
import java.sql.*;
import java.util.*;

import com.google.gson.Gson;

import gate.*;
import msa.pipeline.AnnotateDuplicate;
import utils.db.DBConnection;

public class IEDriver
{
	private GateBatch gate;
	private GenMSADriver genMSADriver;
	private AnnotateDuplicate dupAnnot;
	private FilterPatterns filterPatt;
	private BestPatterns bestPatt;
	private AutoAnnotateNER autoAnnot;
	private PopulateFrame pop;
	private Cleanup cleanup;
	
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
	
	private int runIterations;
	private boolean write;
	private String schema;
	private String schema2;
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
	
	
	private int projID;


	

	
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
	private GateCallback gateCallback;
	
	
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
	
	//Dup
	private String dupQuery;
	
	
	//Filter
	private String filterDocQuery;
	private double negFilterThreshold;
	private int negFilterMinCount;
	private double posFilterThreshold;
	private int posFilterMinCount;
	private int blockSize;
	private int clusterSize;
	private int docBlockSize;
	private int profileSizeLimit;
	
	
	//Best
	private String bestDocQuery;
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
	private boolean autoRecheck;
	private double minGlobalPrec;
	private int minGlobalCount;
	private double minGlobalNegPrec;
	private int minGlobalNegCount;
	
	
	//Cleanup
	private int minFreqCount;
	private int lowFreqDocCount;
	
	
	//Global flags
	private boolean gateFlag;
	private boolean msaFlag;
	private boolean filterFlag;
	private boolean bestFlag;
	private boolean autoFlag;
	private boolean populateFlag;
	private boolean incrementalFlag;
	private boolean cleanupFlag;
	private boolean dupFlag;
	
	
	private Statement stmt;
	private PreparedStatement pstmtInsertFrameInstanceStatus;
	private PreparedStatement pstmtCheckFrameInstanceStatus;
	private PreparedStatement pstmtUpdateFrameInstanceStatus;
	private PreparedStatement pstmtInsertDocStatus;
	private PreparedStatement pstmtUpdateDocStatus;
	private PreparedStatement pstmtGetNewDocs;
	private PreparedStatement pstmtGetDocsWithStatus;
	private PreparedStatement pstmtUpdateDocsWithStatus;
	private PreparedStatement pstmtUpdateDocsWithStatusDocID;
	private PreparedStatement pstmtUpdateFrameInstanceWithStatus;
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
	private PreparedStatement pstmtGetAutoRecheckDocIDs;
	private PreparedStatement pstmtSetFrameInstanceLocks;
	private PreparedStatement pstmtDeleteFrameInstanceLocks;
	private PreparedStatement pstmtUpdateProfileGroup;
	private PreparedStatement pstmtDeleteIncompleteFromIndex;
	private PreparedStatement pstmtDeleteFinalTable;
	private PreparedStatement pstmtGetFrameInstanceID;
	private PreparedStatement pstmtSetDocStatusRange;
	private PreparedStatement pstmtSetFrameInstanceStatusRange;
	
	private long sleep;
	
	
	public IEDriver()
	{
		gate = new GateBatch();
		genMSADriver = new GenMSADriver();
		dupAnnot = new AnnotateDuplicate();
		filterPatt = new FilterPatterns();
		bestPatt = new BestPatterns();
		autoAnnot = new AutoAnnotateNER();
		pop = new PopulateFrame();
		cleanup = new Cleanup();
		
		gson = new Gson();
		//gate.setCallback(gateCallback);
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
			runIterations = Integer.parseInt(props.getProperty("runIterations"));
			
			//keyspace = props.getProperty("keyspace");
			//msaKeyspace = props.getProperty("msaKeyspace");
			
			write = Boolean.parseBoolean(props.getProperty("write"));
			//docNamespace = props.getProperty("docNamespace");
			docTable = props.getProperty("docTable");
			
			verbose = Boolean.parseBoolean(props.getProperty("verbose"));

			schema = props.getProperty("schema");
			docSchema = props.getProperty("docSchema");
			docNamespace = docSchema;
			sleep = Long.parseLong(props.getProperty("sleep"));
			
			gateFlag = Boolean.parseBoolean(props.getProperty("gateFlag"));
			msaFlag = Boolean.parseBoolean(props.getProperty("msaFlag"));
			filterFlag = Boolean.parseBoolean(props.getProperty("filterFlag"));
			bestFlag = Boolean.parseBoolean(props.getProperty("bestFlag"));
			autoFlag = Boolean.parseBoolean(props.getProperty("autoFlag"));
			dupFlag = Boolean.parseBoolean(props.getProperty("dupFlag"));
			
			//newDocQuery = props.getProperty("newDocQuery");
			
			annotTypeQuery = props.getProperty("annotTypeQuery");
			
			incrementalFlag = Boolean.parseBoolean(props.getProperty("incrementalFlag"));
			
			
			String projName = props.getProperty("projectName");
			
			
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
			long gateDuration = Long.parseLong(props.getProperty("gateDuration"));
			gateCallback = new TimeCallback(gateDuration);
			
			gate.setCallback(gateCallback);
			
			
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
			docBlockSize = Integer.parseInt(props.getProperty("docBlockSize"));
			profileSizeLimit = Integer.parseInt(props.getProperty("profileSizeLimit"));
			
			
			
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
			autoRecheck = Boolean.parseBoolean(props.getProperty("autoRecheck"));
			
			minGlobalPrec = Double.parseDouble(props.getProperty("minGlobalPrec"));
			minGlobalCount = Integer.parseInt(props.getProperty("minGlobalCount"));
			minGlobalNegPrec = Double.parseDouble(props.getProperty("minGlobalNegPrec"));
			minGlobalNegCount = Integer.parseInt(props.getProperty("minGlobalNegCount"));
			
			int autoDocLimit = Integer.parseInt(props.getProperty("autoDocLimit"));
			
			//get Populate properties
			populateFlag = Boolean.parseBoolean(props.getProperty("populateFlag"));
			
			
			//get Cleanup properties
			minFreqCount = Integer.parseInt(props.getProperty("minFreqCount"));
			lowFreqDocCount = Integer.parseInt(props.getProperty("lowFreqDocCount"));
			
			
			
			
			conn = DBConnection.dbConnection(user, password, host, dbName, dbType);
			
			schema2 = schema + ".";
			String rq = DBConnection.reservedQuote;
			
			
			//get projectID
			Statement stmt= conn.createStatement();
			projID = -1;
			if (projName != null) {
				ResultSet rs = stmt.executeQuery("select project_id from " + schema2 + "project where name = '" + projName + "'");
				if (rs.next()) {
					projID = rs.getInt(1);
				}
			}
			
			pstmtInsertFrameInstanceStatus = conn.prepareStatement("insert into " + schema2 + "frame_instance_status (frame_instance_id, status) values (?,0)");
			pstmtCheckFrameInstanceStatus = conn.prepareStatement("select status from " + schema2 + "frame_instance_status where frame_instance_id = ?");
			pstmtUpdateFrameInstanceStatus = conn.prepareStatement("update " + schema2 + "frame_instance_status set status = ? where frame_instance_id = ?");
			pstmtInsertDocStatus = conn.prepareStatement("insert into " + schema2 + "document_status (document_namespace, document_table, document_id, status, user_id) "
				+ "values (?,?,?,0,-1)");
			pstmtUpdateDocStatus = conn.prepareStatement("update " + schema2 + "document_status set status = 1 where document_namespace = ? and document_table = ? and document_id = ? and status = -2");
			pstmtGetNewDocs = conn.prepareStatement("select a.document_namespace, a.document_table, a.document_id from " + schema2 + "frame_instance_document a left join " 
				+ schema2 + "document_status b on (a.document_id = b.document_id) "
				+ "where a.frame_instance_id = ? and (b.status is null or b.status = -2) order by a.frame_instance_id, a.document_id");
			
			pstmtGetDocsWithStatus = conn.prepareStatement("select document_namespace, document_table, document_id from " + schema2 + "document_status where status = ?");
			pstmtUpdateDocsWithStatus = conn.prepareStatement("update " + schema2 + "document_status set status = ? where status = ?");			
			pstmtUpdateDocsWithStatusDocID = conn.prepareStatement("update " + schema2 + "document_status set status = ? where status = ? and document_id = ?");
			pstmtUpdateFrameInstanceWithStatus = conn.prepareStatement("update " + schema2 + "frame_instance_status set status = ? where status = ? and frame_instance_status = ?");

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
			//pstmtGetGenMSAStatus = conn.prepareStatement("select profile_count from " + schema2 + "gen_msa_status where annotation_type = ?");
			//pstmtGetGenMSAStatus = conn.prepareStatement("select count(*) from " + schema2 + "document_status where status = 1");
			pstmtGetGenMSAStatus = conn.prepareStatement("select count(*) from " + schema2 + "document_status a, " + schema2 + "annotation b "
				+ "where b.provenance = 'validation-tool' and b.annotation_type = ? and a.status = 1 and a.document_id = b.document_id and a.document_id in "
				+ "(select d.document_id from " + schema2 + "project_frame_instance c, " + schema2 + "frame_instance_document d "
				+ "where c.frame_instance_id = d.frame_instance_id and c.project_id = " + projID + ")");
			pstmtGetGenFilterStatus = conn.prepareStatement("select document_id from " + schema2 + "gen_filter_status");
			pstmtInsertGenFilterStatus = conn.prepareStatement("insert into " + schema2 + "gen_filter_status (document_id) values (?)");
			pstmtUpdateGenFilterStatus = conn.prepareStatement("update " + schema2 + "gen_filter_status set document_id = ?");
			
			
			pstmtUpdateProfileGroup = conn.prepareStatement("update " + schema2 + "profile set " + rq + "group" + rq + " = ? where " + rq + "group" + rq + " = ?");
			
			pstmtSetFrameInstanceLocks = conn.prepareStatement("insert into " + schema2 + "frame_instance_lock (frame_instance_id, username) values (?,?)");
			pstmtDeleteFrameInstanceLocks = conn.prepareStatement("delete from " + schema2 + "frame_instance_lock where username = ?");
			pstmtSetFrameInstanceLocks.setString(2, "##auto");
			pstmtDeleteFrameInstanceLocks.setString(1, "##auto");
			
			pstmtGetFrameInstanceID = conn.prepareStatement("select frame_instance_id from " + schema2 + "frame_instance_document where document_id = ?");
			
			pstmtSetDocStatusRange = conn.prepareStatement("update " + schema2 + "document_status set status = 2 where document_id >= ? and document_id <= ?");
			pstmtSetFrameInstanceStatusRange = conn.prepareStatement("update " + schema2 + "frame_instance_status set status = 1 where frame_instance_id >= ? and frame_instance_id <= ?");
			
			
			newDocQuery = "select a.frame_instance_id, b.status from " + schema2 + "project_frame_instance a left join " + schema2 + "frame_instance_status b on (a.frame_instance_id = b.frame_instance_id) "
				+ "where (a.project_id = " + projID + " and b.frame_instance_id is null or b.status = -2) order by frame_instance_id";
			gateDocQuery = "select document_id from " + schema2 + "document_status where status = 0 or status = -2 order by document_id";
			msaDocQuery = "select document_id, status from " + schema2 + "document_status where (status = 1 or status = 2) and document_id in "
				+ "(select b.document_id from " + schema2 + "project_frame_instance a, " + schema2 + "frame_instance_document b "
				+ "where a.frame_instance_id = b.frame_instance_id and a.project_id = " + projID + ") order by document_id";
			filterDocQuery = "select document_id from " + schema2 + "document_status where status = -4 and document_id in "
				+ "(select b.document_id from " + schema2 + "project_frame_instance a, " + schema2 + "frame_instance_document b "
				+ "where a.frame_instance_id = b.frame_instance_id and a.project_id = " + projID + ") "
				+ "order by document_id";
			bestDocQuery = "select document_id, status from " + schema2 + "document_status" + " where status = -4 "
				+ "order by document_id";
			
			dupQuery = "select a.document_id, a.start, a." + rq + "end" + rq + ", a.annotation_type, c.PatientSID from " + schema2 + "annotation a, " + schema2 + "document_status b, " + schema2 + "documents c "
				+ "where a.provenance = 'validation-tool' and b.status = 1 and a.document_id = b.document_id  and a.document_id = c.document_id and "
				+ "a.document_id in "
				+ "(select e.document_id from " + schema2 + "project_frame_instance d, " + schema2 + "frame_instance_document e "
				+ "where d.frame_instance_id = e.frame_instance_id and d.project_id = " + projID + ") "
				+ "order by a.document_id, a.start";
					
			
			
			if (dbType.equals("mysql"))	{
				autoDBQuery = "(select document_id from " + schema2 + "document_status where status = 0 and document_id in "
					+ "(select b.document_id from " + schema2 + "project_frame_instance a, " + schema2 + "frame_instance_document b "
					+ "where a.frame_instance_id = b.frame_instance_id and a.project_id = " + projID + ") ";
					//+ "order by document_id";
				if (autoDocLimit > 0)
 					autoDBQuery += " limit " + autoDocLimit + ")";
				else
					autoDBQuery += ")";
			}
			else if (dbType.startsWith("sqlserver")) {
				autoDBQuery = "document_id from " + schema2 + "document_status where status = 0 and document_id in "
					+ "(select b.document_id from " + schema2 + "project_frame_instance a, " + schema2 + "frame_instance_document b "
					+ "where a.frame_instance_id = b.frame_instance_id and a.project_id = " + projID + ")) ";
					//+ "order by document_id";
				if (autoDocLimit > 0)
					autoDBQuery = "(select top(" + autoDocLimit + ") " + autoDBQuery;
				else
					autoDBQuery = "(select " + autoDBQuery;
			}
			
			
			//autoDBQuery += " union (select document_id from " + schema2 + "document_status where (status = 1 or status = -4) and document_id in "
			//	+ "(select b.document_id from " + schema2 + "project_frame_instance a, " + schema2 + "frame_instance_document b "
			//	+ "where a.frame_instance_id = b.frame_instance_id and a.project_id = " + projID + ")) "
			autoDBQuery	+= "order by document_id";
			
			
			pstmtGetAutoDocIDs = conn.prepareStatement(autoDBQuery);
			pstmtGetAutoRecheckDocIDs = conn.prepareStatement("select document_id from " + schema2 + "document_status where status = 1 order by document_id");
			
			pstmtDeleteIncompleteFromIndex = conn.prepareStatement("delete from " + schema2 + rq + "index" + rq + 
				" where document_id in (select distinct a.document_id from " + schema2 + "document_status a where a.status = 1)");
			
			pstmtDeleteFinalTable = conn.prepareStatement("delete from " + schema2 + "final");
			
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
			

			
			
			//init AnnotateDuplicate
			Properties dupProps = new Properties();
			dupProps.setProperty("host", host);
			dupProps.setProperty("dbName", dbName);
			dupProps.setProperty("dbType", dbType);
			dupProps.setProperty("docBDHost", docDBHost);
			dupProps.setProperty("docDBName", docDBName);
			dupProps.setProperty("docDBType", docDBType);
			dupProps.setProperty("docNamespace", docNamespace);
			dupProps.setProperty("docTable", docTable);
			dupProps.setProperty("schema", schema);
			dupProps.setProperty("annotQuery", dupQuery);
			dupProps.setProperty("punctuation", Boolean.toString(punct));
			dupProps.setProperty("msaAnnotFilterList", gson.toJson(msaAnnotFilterList));
			dupProps.setProperty("tokType", tokType);
			
			dupAnnot.init(user, password, dupProps);
			
			
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
			pattProps.setProperty("targetProvenance", targetProvenance + "%");
			pattProps.setProperty("tokType", tokType);
			
			pattProps.setProperty("clusterSize", Integer.toString(clusterSize));

			pattProps.setProperty("limit", Integer.toString(limit));
			
			pattProps.setProperty("profileTable", profileTableName);
			pattProps.setProperty("indexTable", indexTableName);
			pattProps.setProperty("finalTable", finalTableName);

			
			pattProps.setProperty("msaAnnotFilterList", gson.toJson(msaAnnotFilterList));
			pattProps.setProperty("scoreList", gson.toJson(scoreList));
			
			pattProps.setProperty("negFilterThreshold", Double.toString(negFilterThreshold));
			pattProps.setProperty("negFilterMinCount", Integer.toString(negFilterMinCount));
			pattProps.setProperty("posFilterThreshold", Double.toString(posFilterThreshold));
			pattProps.setProperty("posFilterMinCount", Integer.toString(posFilterMinCount));
			
			pattProps.setProperty("blockSize", Integer.toString(blockSize));
			pattProps.setProperty("write", Boolean.toString(write));
			pattProps.setProperty("docBlockSize", Integer.toString(docBlockSize));
			
			pattProps.setProperty("profileSizeLimit", Integer.toString(profileSizeLimit));
			
			filterPatt.init(user, password, user, password, pattProps);
			
			
			//best patts props
			Properties bestProps = new Properties();
			
			bestProps.setProperty("host", host);
			bestProps.setProperty("dbName", dbName);
			//bestProps.setProperty("keyspace", keyspace);
			//bestProps.setProperty("annotKeyspace", keyspace);
			//bestProps.setProperty("msaKeyspace", msaKeyspace);
			bestProps.setProperty("dbType", dbType);
			bestProps.setProperty("schema", schema);
			bestProps.setProperty("filterFlag", "true");
			//bestProps.setProperty("finalTable", finalTable);
			//bestProps.setProperty("indexTable", indexTableName);
			//bestProps.setProperty("profileTable", profileTableName);
			bestProps.setProperty("docQuery", bestDocQuery);
			bestProps.setProperty("provenance", targetProvenance);
			bestProps.setProperty("negThreshold", Double.toString(negThreshold));
			bestProps.setProperty("negMinCount", Integer.toString(negMinCount));
			bestProps.setProperty("posThreshold", Double.toString(posThreshold));
			bestProps.setProperty("posMinCount", Integer.toString(posMinCount));
			bestProps.setProperty("group", group);
			
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
			//autoProps.setProperty("requireTarget", Boolean.toString(requireTarget));
			autoProps.setProperty("requireTarget", "false");
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
			
			autoProps.setProperty("minGlobalPrec", Double.toString(minGlobalPrec));
			autoProps.setProperty("minGlobalCount", Integer.toString(minGlobalCount));
			autoProps.setProperty("minGlobalNegPrec", Double.toString(minGlobalNegPrec));
			autoProps.setProperty("minGlobalNegCount", Integer.toString(minGlobalNegCount));
			
			autoAnnot.init(user, password, docUser, docPassword, autoProps);

			
			pop.init(user, password, host, dbName, dbType, schema, autoProvenance, DBConnection.reservedQuote);
			
			
			//Cleanup
			Properties cleanupProps = new Properties();
			cleanupProps.setProperty("host", host);
			cleanupProps.setProperty("dbName", dbName);
			cleanupProps.setProperty("dbType", dbType);
			cleanupProps.setProperty("schema", schema);
			
			cleanupProps.setProperty("profileTable", profileTableName);
			cleanupProps.setProperty("indexTable", indexTableName);
			cleanupProps.setProperty("finalTable", finalTableName);
			
			cleanupProps.setProperty("minFreqCount", Integer.toString(minFreqCount));
			cleanupProps.setProperty("lowFreqDocCount", Integer.toString(lowFreqDocCount));
			
			cleanup.init(cleanupProps);
			

			stmt = conn.createStatement();
			String rq = DBConnection.reservedQuote;
						
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
			
			
			
			
			//clean up any previous incomplete runs
			
			/*
			System.out.println("clean up previous runs...");
			//stmt.execute("delete from " + schema2 + rq + "index" + rq + 
			//" where document_id in (select distinct a.document_id from " + schema2 + "document_status a where a.status = 1)");
			
			//stmt.execute("delete from " + schema2 + rq + "index" + rq + " where profile_id in (select a.profile_id from " + schema2 + "profile a where a." + rq + "group" + rq + " = '" + group + "')");
			
			stmt.execute("delete from " + schema2 + "annotation where provenance = '" + gateProvenance + "' and not exists (select a.document_id from " + schema2 + "document_status a where a.document_id = document_id)");
			
			int docStatusOneCount = 0;
			rs = stmt.executeQuery("select count(*) from " + schema2 + "document_status where status = 1");
			if (rs.next())
				docStatusOneCount = rs.getInt(1);
			
			int profileNewCount = 0;
			rs = stmt.executeQuery("select count(*) from " + schema2 + "profile where " + rq + "group" + rq + " = '" + group + "'");
			if (rs.next())
				profileNewCount = rs.getInt(1);
				*/
			
			//clear final table if there were documents with status 1 or there were new profiles that didn't finish processing
			//if (docStatusOneCount > 0 || profileNewCount > 0) {
				//System.out.println("delete from final table...");
				//stmt.execute("delete from " + schema2 + "final");
			//}
			
			
			//pstmtUpdateProfileGroup.setString(1, group + "##");
			//pstmtUpdateProfileGroup.setString(2, group);
			//pstmtUpdateProfileGroup.execute();

			//updateDocsWithStatus(2, 1);
			
			
			String docQuery = "select distinct document_id from " + schema2 + "document_status where status = -4 and document_id in "
				+ "(select distinct b.document_id from " + schema2 + "project_frame_instance a, " + schema2 + "frame_instance_document b "
				+ "where a.frame_instance_id = b.frame_instance_id and a.project_id = " + projID + ") order by document_id ";
			
			if (dbType.equals("mysql"))
				docQuery += "limit ?, ?";
			else if (dbType.startsWith("sqlserver"))
				docQuery += "offset ? rows fetch next ? rows only";
			
			PreparedStatement pstmtFetchDocs = conn.prepareStatement(docQuery);
			
			
			
			
			
			int count = 0;
			
			while (flag) {
				//get docs from frameinstances that have been imported but not insert into the frame_instance_status table
						
				if (gateFlag) {
					System.out.println("** GATE **");
					List<DocBean> docList = checkNewDocuments();
					List<Long> docIDList = new ArrayList<Long>();
					for (DocBean doc : docList) {
						Long docID = doc.getDocID();
						docIDList.add(docID);
					}
					
					//run GATE pipeline
					if (docList.size() > 0) {
						gate.setDocIDList(docIDList);
						gate.process(user, password, docUser, docPassword);
						//updateDocsWithStatus(0, 1);
						long lastDocID = gate.getCurrDoc();
						
						pstmtInsertDocStatus.setString(1, docNamespace);
						pstmtInsertDocStatus.setString(2, docTable);
						
						pstmtUpdateDocStatus.setString(1, docNamespace);
						pstmtUpdateDocStatus.setString(2, docTable);
						
						
						for (int i=0; i<docList.size(); i++) {
							DocBean doc = docList.get(i);
							long frameInstanceID = doc.getFrameInstanceID();
							long nextFrameInstanceID = -1;
							if (i < docList.size()-1) {
								DocBean doc2 = docList.get(i+1);
								nextFrameInstanceID = doc2.getFrameInstanceID();
							}
							
							int status = -1;
							pstmtCheckFrameInstanceStatus.setLong(1, frameInstanceID);
							ResultSet rs2 = pstmtCheckFrameInstanceStatus.executeQuery();
							if (rs2.next()) {
								status = rs2.getInt(1);
							}
							
							if (nextFrameInstanceID != frameInstanceID) {
								if (status == -1) {
									pstmtInsertFrameInstanceStatus.setLong(1, frameInstanceID);
									pstmtInsertFrameInstanceStatus.execute();
								}
								else if (status == -2) {
									pstmtUpdateFrameInstanceStatus.setInt(1, 1);
									pstmtUpdateFrameInstanceStatus.setLong(2, frameInstanceID);
									pstmtUpdateFrameInstanceStatus.execute();
								}
								else if (status == -3) {
									pstmtUpdateFrameInstanceStatus.setInt(1, 0);
									pstmtUpdateFrameInstanceStatus.setLong(2, frameInstanceID);
									pstmtUpdateFrameInstanceStatus.execute();
								}
							}
							
							Integer docStatus = doc.getStatus();
							long docID = doc.getDocID();
							if (docStatus == null) {
								pstmtInsertDocStatus.setLong(3, docID);
								pstmtInsertDocStatus.execute();
							}
							else {
								pstmtUpdateDocStatus.setLong(3, docID);
								pstmtUpdateDocStatus.execute();
							}
							
							if (doc.getDocID() == lastDocID) {
								break;
							}
						}
					}
					
					
					//derived annotations
					/*
					stmt.execute("delete from " + schema2 + "annotation2");
					PreparedStatement pstmtDerived = conn.prepareStatement("insert into " + schema2 + "annotation2 select * from " + schema2 + "annotation where annotation_type = 'MetaMap' and "
						+ "(features like '%acab%' or features like '%anab%' or features like '%biof%' or features like '%cgab%' or "
						+ "features like '%comd%' or features like '%dsyn%' or features like '%emod%' or features like '%fndg%' or "
						+ "features like '%inpo%' or features like '%lbtr%' or features like '%menp%' or features like '%mobd%' or "
						+ "features like '%neop%' or features like '%patf%' or features like '%phsf%' or features like '%sosy%') and document_id = ?");

					for (DocBean doc : docList) {
						long docID = doc.getDocID();
						pstmtDerived.setLong(1, docID);
						pstmtDerived.execute();
					}
					
					stmt.execute("update " + schema2 + "annotation2 set annotation_type = 'metamap-finding'");

					stmt.execute("insert into " + schema2 + "annotation select * from annotation2");
					*/
				}
				
				
				
				
				List<String> activeAnnotTypeList = new ArrayList<String>();
				List<DocBean> docList = new ArrayList<DocBean>();
				//long minDocID = -1;
				int lastDocIndex = -1;
				
				GenSentences genSent = null;
				
				for (String annotType : annotTypeList) {
					int currCount = 0;
					pstmtGetGenMSAStatus.setString(1, annotType);
					rs = pstmtGetGenMSAStatus.executeQuery();

					if (rs.next()) {
						currCount = rs.getInt(1);
					}
					
					if (currCount == 0)
						continue;
					
					activeAnnotTypeList.add(annotType);
				}
				
				
				
				
				
				//duplicate
				if (dupFlag) {
					System.out.println("** Duplicate **");
					dupAnnot.setProjID(projID);
					dupAnnot.run();
				}
				
				
				
				//set status
				stmt.execute("update " + schema2 + "frame_instance_status set status = -4 where status = 1 and frame_instance_id in "
					+ "(select distinct a.frame_instance_id from " + schema2 + "project_frame_instance a where a.project_id = " + projID + ")");
				stmt.execute("update " + schema2 + "document_status set status = -4 where status = 1 and document_id in "
					+ "(select distinct b.document_id from " + schema2 + "project_frame_instance a, " + schema2 + "frame_instance_document b "
					+ "where a.frame_instance_id = b.frame_instance_id and a.project_id = " + projID + ")");
				
				
				
				
				List<Long> docIDList2 = new ArrayList<Long>();
				rs = stmt.executeQuery("select distinct document_id from " + schema2 + "document_status where status = 2 and document_id in "
					+ "(select distinct b.document_id from " + schema2 + "project_frame_instance a, " + schema2 + "frame_instance_document b "
					+ "where a.frame_instance_id = b.frame_instance_id and a.project_id = " + projID + ")");
				while(rs.next()) {
					docIDList2.add(rs.getLong(1));
				}
				
				int offset = 0;
				
				pstmtFetchDocs.setInt(1, offset);
				pstmtFetchDocs.setInt(2, docBlockSize);
				
				List<Long> docIDList1 = new ArrayList<Long>();
				rs = pstmtFetchDocs.executeQuery();
				while(rs.next()) {
					docIDList1.add(rs.getLong(1));
				}
				
				
				while (docIDList1.size() > 0) {
					
					System.out.println("Doc Block: " + docIDList1.get(0) + " to " + docIDList1.get(docIDList1.size()-1));
				
					//gen MSAs
					if (msaFlag) {
						System.out.println("** Gen MSA **");
						//run MSA generator
						
						rs = stmt.executeQuery("select count(*) from " + schema2 + "document_status where status = 1 and document_id in "
							+ "(select distinct b.document_id from " + schema2 + "project_frame_instance a, " + schema2 + "frame_instance_document b "
							+ "where a.frame_instance_id = b.frame_instance_id and a.project_id = " + projID + ")");
						 
						int msaDocCount = 0;
						if (rs.next()) {
							msaDocCount= rs.getInt(1);
						}
						
						
						//only run if there are docs with status = 1
						if (msaDocCount > 0) {
						
							List<Long> genDocIDList = new ArrayList<Long>();
							genDocIDList.addAll(docIDList2);
							genDocIDList.addAll(docIDList1);
							genMSADriver.setDocIDList(genDocIDList);
						
							genMSADriver.getSentences(user, password, annotTypeList);
							
							for (int i=0; i<annotTypeList.size(); i++) {
							//for (int i=0; i<2; i++) {
								String annotType = annotTypeList.get(i);
								String profileTable = profileTableList.get(i);
								
		
								/*
								int currCount = 0;
								pstmtGetGenMSAStatus.setString(1, annotType);
								rs = pstmtGetGenMSAStatus.executeQuery();
		
								while (rs.next()) {
									currCount = rs.getInt(1);
								}
								
								if (currCount == 0)
									continue;
									*/
	
								
		
								//if (annotCount > currCount) {
								//activeAnnotTypeList.add(annotType);
								genMSADriver.setTargetType(annotType);
								genMSADriver.setProfileTable(profileTable);
								//genMSADriver.setRequireTarget(requireTargetMap.get(annotType));
								//genMSADriver.setGroup(Integer.toString(count));
								genMSADriver.run(user, password, docUser, docPassword);
								
								
								
								docList = genMSADriver.getDocList();
								lastDocIndex = genMSADriver.getLastDocIndex();
								
							}
						
						//gen relation patterns
						}
						
					}
					
					
					genSent = genMSADriver.getGenSent();
					
					
					//status = -5 are documents within a range of status = 1 documents that somehow were not validated by the user
					//these documents will be considered disabled and not used for analysis
					long lastDocID = docIDList1.get(docIDList1.size()-1);
					stmt.execute("update " + schema2 + "document_status set status = -5 where status = 0 and document_id < " + lastDocID + " and document_id in "
						+ "(select distinct b.document_id from " + schema2 + "project_frame_instance a, " + schema2 + "frame_instance_document b "
						+ "where a.frame_instance_id = b.frame_instance_id and a.project_id = " + projID + ")");
					stmt.execute("update " + schema2 + "frame_instance_id set status = -5 where frame_instance_id in "
						+ "(select distinct a.frame_instance_id from " + schema2 + "frame_instance_document a, " + schema2 + "document_status b "
						+ "where a.document_id = b.document_id and b.status = -5)");
					
					
					
					
					//phase 1 for profile type 3 (repeated entire sentences)
					//filter patterns
					if (filterFlag) {
						System.out.println("** FILTER**");
						//run filter patterns
						
						filterPatt.setProfileType(0);
						//filterPatt.setTargetProvenance("validation-tool");
						filterPatt.setGenSent(genSent);
						
						filterPatt.setDocIDList(docIDList1);
						//filterPatt.readDocIDList();
						
						
						filterPatt.setAnnotTypeList(activeAnnotTypeList);
						filterPatt.setProfileTableList(profileTableList);
						filterPatt.setIndexTableList(indexTableList);
						filterPatt.filterPatterns(user, password, docUser, docPassword, user, password);
						
						//second incremental for existing patterns to only run on new docs (status = 1)
						/*
						System.out.println("** FILTER Second Pass **");
						filterPatt.setDocDBQuery("select document_id from " + schema2 + "document_status where status = 1 order by document_id");
						filterPatt.setGroup(group + "##");
						filterPatt.setTargetGroup(group + "##");
						filterPatt.filterPatterns(user, password, docUser, docPassword, user, password);
						*/
						
					}
					
					
					offset += docBlockSize;
					pstmtFetchDocs.setInt(1, offset);
					docIDList1 = new ArrayList<Long>();
					rs = pstmtFetchDocs.executeQuery();
					while(rs.next()) {
						docIDList1.add(rs.getLong(1));
					}
					
					docIDList2.clear();
				
				}
				
				
				
				//best patterns
				if (bestFlag) {
					System.out.println("** BEST**");
					
					
					//pstmtDeleteFinalTable.execute();
					
					bestPatt.setAnnotTypeList(activeAnnotTypeList);
					bestPatt.setProfileTableList(profileTableList);
					bestPatt.setIndexTableList(indexTableList);
					bestPatt.setFinalTableList(finalTableList);					
					bestPatt.getBestPatterns(user, password, user, password);
				}
				
				
				//genSent = null;
				
				if (genSent == null)
					genSent = filterPatt.getGenSent();
				
				
				//auto annotate
				if (autoFlag) {
					System.out.println("** AUTO**");
					
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
					

					autoAnnot.setProfileType(0);
					autoAnnot.setGenSent(genSent);
					autoAnnot.setAnnotTypeList(activeAnnotTypeList);
					autoAnnot.setProfileTableList(profileTableList);
					autoAnnot.setFinalTableList(finalTableList);
					autoAnnot.setDocDBQuery(autoDBQuery);
					autoAnnot.setProfileMinPrec(profileMinPrec);
					
					autoAnnot.annotate(user, password);
					
					pstmtDeleteFrameInstanceLocks.execute();
					
					//autoAnnot.close();
				}
				
				
				
				
				//auto recheck
				if (autoRecheck) {
					System.out.println("** AUTO RECHECK **");
					
					//set frame instance locks
					ResultSet rs2 = pstmtGetAutoRecheckDocIDs.executeQuery();
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
					autoAnnot.setAutoProvenance("##auto-recheck");
					autoAnnot.setDocDBQuery("select document_id from " + schema2 + "document_status where status = 1 order by document_id");
					autoAnnot.setProfileMinPrec(0.8);
					
					autoAnnot.annotate(user, password);
					
					pstmtDeleteFrameInstanceLocks.execute();
					
					//autoAnnot.close();
				}
				
				
				//populate
				if (populateFlag) {
					System.out.println("*** POPULATE ***");
					pop.populate(projID);
				}
				
				
				
				//reset status to 2
				stmt.execute("update " + schema2 + "frame_instance_status set status = 2 where status = -4 and frame_instance_id in "
						+ "(select distinct a.frame_instance_id from " + schema2 + "project_frame_instance a "
						+ "where a.project_id = " + projID + ")");
					
				stmt.execute("update " + schema2 + "document_status set status = 2 where status = -4 and document_id in "
						+ "(select distinct b.document_id from " + schema2 + "project_frame_instance a, " + schema2 + "frame_instance_document b "
						+ "where a.frame_instance_id = b.frame_instance_id and a.project_id = " + projID + ")");
				
				
				int docStatusCount = 0;
				rs = stmt.executeQuery("select count(*) from " + schema2 + "document_status where status = 2 and document_id in "
					+ "(select distinct b.document_id from " + schema2 + "project_frame_instance a, " + schema2 + "frame_instance_document b "
					+ "where a.frame_instance_id = b.frame_instance_id and a.project_id = " + projID + ")");
				if (rs.next()) {
					docStatusCount = rs.getInt(1);
				}
				
				if (docStatusCount >= blockSize) {
					stmt.execute("update " + schema2 + "frame_instance_status set status = 3 where status = 2 and frame_instance_id in "
						+ "(select distinct a.frame_instance_id from " + schema2 + "project_frame_instance a where a.project_id = " + projID + ")");
					stmt.execute("update " + schema2 + "document_status set status = 3 where status = 2 and document_id in "
						+ "(select distinct b.document_id from " + schema2 + "project_frame_instance a, " + schema2 + "frame_instance_document b "
						+ "where a.frame_instance_id = b.frame_instance_id and a.project_id = " + projID + ")");
				}
				
				
				
				
				
				//clean genSent cache
				rs = stmt.executeQuery("select document_id from " + schema2 + "document_status where status = 3 and document_id in "
					+ "(select distinct b.document_id from " + schema2 + "project_frame_instance a, " + schema2 + "frame_instance_document b "
					+ "where a.frame_instance_id = b.frame_instance_id and a.project_id = " + projID + ")");
				
				if (genSent != null && genSent.getDB() != null) {
					while (rs.next()) {
						long docID = rs.getLong(1);
						genSent.removeDocID(docID);
					}
				}
				
				
				
				
				
				//cleanup
				if (cleanupFlag) {
					System.out.println("*** CLEANUP ***");
					cleanup.cleanup();
				}
				
								
				
				//sleep
				if (sleep >= 0) {
					if (verbose)
						System.out.println("sleeping for " + sleep + "ms...");
					Thread.sleep(sleep);
				}
				else
					break;
				
				count++;
				if (runIterations >= 0 && count >= runIterations)
					break;
				else if (runIterations < 0)
					count = 0;
				
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
			Integer status = rs.getInt(2);
			
			if (rs.wasNull())
				status = null;
			
			
			//pstmtInsertFrameInstanceStatus.setLong(1, frameInstanceID);
			//pstmtInsertFrameInstanceStatus.execute();
			
			pstmtGetNewDocs.setLong(1, frameInstanceID);
			
			ResultSet rs2 = pstmtGetNewDocs.executeQuery();

			
			while (rs2.next()) {
				//if (docList.size() == 30)
				//	break;

				String docNamespace = rs2.getString(1);
				String docTable = rs2.getString(2);
				long docID = rs2.getLong(3);
				
				
				//pstmtInsertDocStatus.setString(1, docNamespace);
				//pstmtInsertDocStatus.setString(2, docTable);
				//pstmtInsertDocStatus.setLong(3, docID);
				  
				//try {
					//pstmtInsertDocStatus.execute();
				//}
				//catch(SQLException e)
				//{
				//	e.printStackTrace();
				//}
				
				DocBean docBean = new DocBean(docNamespace, docTable, docID, frameInstanceID, status);
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
		Map<Long, Boolean> frameInstanceMap = new HashMap<Long, Boolean>();
		
		pstmtUpdateDocsWithStatusDocID.setInt(1, newStatus);
		pstmtUpdateDocsWithStatusDocID.setInt(2, oldStatus);
		
		pstmtUpdateFrameInstanceStatus.setInt(1, newStatus);
		//pstmtUpdateFrameInstanceWithStatus.setInt(2, oldStatus);

		for (DocBean docBean : docList) {
			long frameInstanceID = -1;
			pstmtGetFrameInstanceID.setLong(1, docBean.getDocID());
			ResultSet rs = pstmtGetFrameInstanceID.executeQuery();
			if (rs.next()) {
				frameInstanceID = rs.getLong(1);
			}
			
			System.out.println("frameInstanceID: " + frameInstanceID + "status: " + newStatus); 
			
			if (frameInstanceMap.get(frameInstanceID) == null) {
				frameInstanceMap.put(frameInstanceID, true);
				//pstmtUpdateFrameInstanceStatus.setLong(1, newStatus);
				pstmtUpdateFrameInstanceStatus.setLong(2, frameInstanceID);
				pstmtUpdateFrameInstanceStatus.execute();
				System.out.println(pstmtUpdateFrameInstanceStatus.toString());
			}
				
			
			System.out.println("docID: " + docBean.getDocID() + "status: " + newStatus); 
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
