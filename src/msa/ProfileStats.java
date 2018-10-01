package msa;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.google.gson.Gson;

import align.AnnotationSequenceGrid;
import align.SmithWatermanDim;
import utils.db.DBConnection;

public class ProfileStats
{
	private List<String> annotTypeNameList;
	private Map<String, Boolean> ansMap;
	
	private SmithWatermanDim sw;
	private int maxGaps = 1;
	private int syntax = 1;
	private int phrase = 0;
	
	private Gson gson;
	
	private Connection conn;
	private Connection msaConn;
	
	private double negFilterThreshold = 0.9;
	private int negFilterMinCount = 20;
	private double posFilterThreshold = 0.95;
	private int posFilterMinCount = 40;
	private int blockSize = 10;
	private int internalBlockSize = 10;

	
	private ProfileMatcher profileMatcher;
	private boolean keepUnmatched = false;
	
	//private Map<String, MSAProfile> msaProfileMap;
	
	private PreparedStatement pstmt;
	private PreparedStatement pstmt2;
	private PreparedStatement pstmt3;
	private PreparedStatement pstmt4;
	private PreparedStatement pstmt5;
	private PreparedStatement pstmt6;
	
	private Map<String, Integer> posMap;
	private Map<String, Integer> negMap;
	
	private List<Long> docIDList;
	
	private boolean write = true;
	
	private String annotType;
	private MatchWriter matchWriter;
	
	private String schema;
	
	private PrintWriter pw;
	private String finalTable;
	
	private String provenance;
	
	public ProfileStats()
	{
		sw = new SmithWatermanDim();
		//sw.setVerbose(true);
		gson = new Gson();
		//genContext = new GenContext();
		profileMatcher = new ProfileMatcher();
	}
	
	public ProfileStats(String host)
	{
		this();
	}
	
	public void setMaxGaps(int maxGaps)
	{
		this.maxGaps = maxGaps;
	}
	
	public void setSyntax(int syntax)
	{
		this.syntax = syntax;
	}
	
	public void setPhrase(int phrase)
	{
		this.phrase = phrase;
	}
	
	public void setNegFilterThreshold(double filterThreshold)
	{
		this.negFilterThreshold = filterThreshold;
	}
	
	public void setNegFilterMinCount(int filterMinCount)
	{
		this.negFilterMinCount = filterMinCount;
	}
	
	public void setPosFilterThreshold(double filterThreshold)
	{
		this.posFilterThreshold = filterThreshold;
	}
	
	public void setPosFilterMinCount(int filterMinCount)
	{
		this.posFilterMinCount = filterMinCount;
	}
	
	public void setAnnotTypeNameList(List<String> annotTypeNameList)
	{
		this.annotTypeNameList = annotTypeNameList;
	}
	
	public void setScoreList(List<Double> scoreList)
	{
		sw.setScoreMap(annotTypeNameList, scoreList);
	}
	
	public void setKeepUnmatched(boolean keepUnmatched)
	{
		this.keepUnmatched = keepUnmatched;
	}
	
	public void setBlockSize(int blockSize)
	{
		this.blockSize = blockSize;
	}
	
	public void setWrite(boolean write)
	{
		this.write = write;
	}
	
	public void setDocIDList(List<Long> docIDList)
	{
		this.docIDList = docIDList;
	}
	
	public void setSchema(String schema)
	{
		this.schema = schema + ".";
	}
	
	public void setPrintWriter(PrintWriter pw)
	{
		this.pw = pw;
	}
	
	public void setAnnotType(String annotType)
	{
		this.annotType = annotType;
	}
	
	public void init(String annotUser, String annotPassword, String msaUser, String msaPassword, String host, String keyspace, String msaKeyspace, String dbType,
		String indexTable, String finalTable, String annotType, String provenance)
	{
		try {
			//this.annotType = annotType;
			this.provenance = provenance;
			
			conn = DBConnection.dbConnection(annotUser, annotPassword, host, keyspace, dbType);
			//conn = DBConnection.dbConnection("fmeng", "fmeng", "localhost", keyspace, "mysql");
			msaConn = DBConnection.dbConnection(msaUser, msaPassword, host, msaKeyspace, dbType);
			
			pstmt = msaConn.prepareStatement("select document_id, start, end from msa_profile_match_index where profile_id = ? and target_id = ?");
			//pstmt2 = msaConn.prepareStatement("select count(*) from msa_profile_match_index where profile_id = ?");
			pstmt2 = msaConn.prepareStatement("insert into " + schema + "final (profile_id, target_id, prec) values (?,?,?)");
			pstmt3 = msaConn.prepareStatement("update " + schema + "final set prec = ? where profile_id = ? and target_id = ?");
			pstmt4 = msaConn.prepareStatement("select count(*) from " + schema + "final where profile_id = ? and target_id = ?");
			pstmt5 = msaConn.prepareStatement("select target_id from " + schema + "final where profile_id = ? and prec < 0.0");
			pstmt6 = msaConn.prepareStatement("update " + schema + "final set true_pos = 0, false_pos = 0, total = 0 where profile_id = ? and target_id = ?");
			
			matchWriter = new MatchWriter();
			matchWriter.init(msaUser, msaPassword, host, msaKeyspace, dbType, indexTable);
			
			profileMatcher.setAligner(sw);
			profileMatcher.setProfileMatch(true);
			
			this.finalTable = finalTable;
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	public void close()
	{
		try {
			pstmt.close();
			pstmt2.close();
			conn.close();
			msaConn.close();

			matchWriter.close();
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	
	public void getProfileStats(List<AnnotationSequenceGrid> gridList, List<ProfileGrid> profileGridList, List<AnnotationSequenceGrid> targetGridList, int profileType, 
		Map<AnnotationSequenceGrid, MSAProfile> msaProfileMap, Map<AnnotationSequenceGrid, MSAProfile> msaTargetProfileMap, ProfileInvertedIndex invertedIndex, Map<Long, AnnotationSequenceGrid> targetIDMap)
	{		

		try {
			posMap = new HashMap<String, Integer>();
			negMap = new HashMap<String, Integer>();
			
			System.out.println("reading answers...");
			ansMap = readAnswers(annotType, provenance);
			
			//preload pos and neg maps
			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery("select profile_id, target_id, true_pos, false_pos from " + schema + finalTable);
			while (rs.next()) {
				long profileID = rs.getLong(1);
				long targetID = rs.getLong(2);
				int posCount = rs.getInt(3);
				int negCount = rs.getInt(4);
				posMap.put(profileID + "|" + targetID, posCount);
				negMap.put(profileID + "|" + targetID, negCount);
			}
			
			
			//remove known inactive pattern/target combinations
			removeInactivePatterns(profileGridList, msaProfileMap, targetIDMap);
			
			
			int maxGridSize = 0;
			for (AnnotationSequenceGrid grid  : gridList) {
				if (grid.size() > maxGridSize)
					maxGridSize = grid.size();
			}			

			List<String> profileGridStrList = new ArrayList<String>();
			for (ProfileGrid profileGrid : profileGridList) {
				AnnotationSequenceGrid grid = profileGrid.getGrid();
				profileGridStrList.add(gson.toJson(grid.getSequence().getToks()));
			}
			
			System.out.println("\n\nGetting profile stats...");
			
			//filter in stages, where low performing profiles are quickly filtered out
			//this will make the system more efficient
			int start = 0;
			int end = internalBlockSize;
			int added = 0;
			int filtered = 0;
			int count = 0;
			
			if (end > gridList.size())
				end = gridList.size();
			
			List<MSAProfile> profileList = new ArrayList<MSAProfile>();
			//Map<String, Boolean> profileAddedMap = new HashMap<String, Boolean>();
			
			List<ProfileMatch> currMatchList = new ArrayList<ProfileMatch>();
			
			while (start < gridList.size()) {
				System.out.println("BLOCK: " + start + ", " + end);
				
				if (pw != null)
					pw.println("BLOCK: " + start + ", " + end);
				
				List<ProfileMatch> matchList = profileMatcher.matchProfile(gridList, profileGridList, targetGridList, annotType, false, maxGaps, syntax, phrase, true, start, end, msaProfileMap, msaTargetProfileMap, invertedIndex);
				
				currMatchList.addAll(matchList);

				//write the matches
				if ((count % blockSize == 0) && write) {
					matchWriter.write(currMatchList);
					currMatchList = new ArrayList<ProfileMatch>();
				}
				
				/*
				for (ProfileMatch profileMatch : matchList) {
					System.out.println(profileMatch.getProfileStr() + ", " + profileMatch.getGridStr() + ", " + profileMatch.getTargetStr());
				}
				*/				
				
				//filter low precision patterns
				removePatterns(matchList, profileGridList, targetGridList, msaProfileMap, msaTargetProfileMap);
				
				
				//List<String> profileMapStrList = new ArrayList<String>();
				/*
				for (String profileGridStr : msaProfileMap.keySet()) {
					MSAProfile profile = msaProfileMap.get(profileGridStr);
					
					int index = profileGridStrList.indexOf(profileGridStr);
					
					if (index < 0)
						continue;
					
					List<String> toks = new ArrayList<String>();
					toks = gson.fromJson(profileGridStr, toks.getClass());
					
					System.out.println("processing: " + profileGridStr);
					
					
					int posTotal = profile.getFalsePos() + profile.getTruePos();
					System.out.println(profileGridStr + ": tp: " + profile.getTruePos() + ", fp: " + profile.getFalsePos());
					
					if (posTotal >= minFilterNegCount) {

						boolean posProfile = getProfileStats(profile);
						
						if (!posProfile) {
							System.out.println("\n\nfiltered out: " + profileGridStr + "\n\n");
							profile.setScore(-1.0);
							profileIDMap.remove(profileGridStr);
							
							profileGridList.remove(index);
							profileGridStrList.remove(index);
							filtered++;
							profileAddedMap.put(profileGridStr, true);
						}
					}
				}
				*/

				
				start = end;
				end += internalBlockSize;
				if (end > gridList.size())
					end = gridList.size();
				
				count++;
			}
			
			if (write)
				matchWriter.write(currMatchList);
			
			System.out.println("filtered: " + filtered + ", added: " + added);
			System.out.println("profile size: " + profileList.size());
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	
	private void removeInactivePatterns(List<ProfileGrid> profileGridList, Map<AnnotationSequenceGrid, MSAProfile> msaProfileMap, Map<Long, AnnotationSequenceGrid> targetIDMap) throws SQLException
	{
		for (ProfileGrid profileGrid : profileGridList) {
			MSAProfile msaProfile = msaProfileMap.get(profileGrid.getGrid());
			long profileID = msaProfile.getProfileID();
			pstmt5.setLong(1, profileID);
			pstmt6.setLong(1, profileID);
			
			Map<AnnotationSequenceGrid, Boolean> targetGridMap = profileGrid.getTargetGridMap();
			
			ResultSet rs = pstmt5.executeQuery();
			while (rs.next()) {
				long targetID = rs.getLong(1);
				AnnotationSequenceGrid targetGrid = targetIDMap.get(targetID);
				targetGridMap.remove(targetGrid);
				pstmt6.setLong(2, targetID);
				pstmt6.execute();
			}
		}
	}
	
	
	private void removePatterns(List<ProfileMatch> matchList, List<ProfileGrid> profileGridList, List<AnnotationSequenceGrid> targetGridList, Map<AnnotationSequenceGrid, MSAProfile> msaProfileMap, 
		Map<AnnotationSequenceGrid, MSAProfile> msaTargetProfileMap) throws SQLException
	{
		Map<String, Boolean> matchMap = new HashMap<String, Boolean>();
		
		for (ProfileMatch match : matchList) {
			long docID = match.getSequence().getDocID();
			int start = match.getTargetIndexes()[0];
			int end = match.getTargetIndexes()[1];
			MSAProfile profile = match.getProfile();
			long profileID = profile.getProfileID();

			//String key = profileID + "|" + targetID;

			Boolean ans = ansMap.get(docID + "|" + start + "|" + end);
				
			String key = Long.toString(profileID);
			if (ans != null) {
				Integer count = posMap.get(key);
				if (count == null)
					count = 0;
				posMap.put(key, ++count);
			}
			else {
				Integer count = negMap.get(key);
				if (count == null)
					count = 0;
				negMap.put(key, ++count);
			}
			
			List<MSAProfile> targetList = match.getTargetList();
			for (MSAProfile target : targetList) {
				long targetID = target.getProfileID();
				key = profileID + "|" + targetID;
				
				if (ans != null) {
					Integer count = posMap.get(key);
					if (count == null)
						count = 0;
					posMap.put(key, ++count);
				}
				else {
					Integer count = negMap.get(key);
					if (count == null)
						count = 0;
					negMap.put(key, ++count);
				}

			}
		}
		
		
		for (int i=0; i<profileGridList.size(); i++) {
			ProfileGrid profileGrid = profileGridList.get(i);
			MSAProfile profile = msaProfileMap.get(profileGrid.getGrid());
			long profileID = profile.getProfileID();
			
			Map<AnnotationSequenceGrid, Boolean> targetGridMap = profileGrid.getTargetGridMap();

			
			Iterator<AnnotationSequenceGrid> iter = targetGridMap.keySet().iterator();
			for (;iter.hasNext();) {
				AnnotationSequenceGrid targetGrid = iter.next();
				
				MSAProfile target = msaTargetProfileMap.get(targetGrid);
				long targetID = target.getProfileID();
				
				Integer tp = posMap.get(profileID + "|" + targetID);
				if (tp == null)
					tp = 0;
				Integer fp = negMap.get(profileID + "|" + targetID);
				if (fp == null)
					fp = 0;
				
				if (tp == 0 && fp == 0)
					continue;
				
				double prec = ((double) tp) / ((double) (tp + fp));
				
				//if ((tp + fp) >= 10 && prec < .9) {
				boolean negRemove = false;
				boolean posRemove = false;
				
				
				
				if (((tp + fp) >= negFilterMinCount && prec < negFilterThreshold))
					negRemove = true;
				
				if ((tp + fp)  >= posFilterMinCount && prec >= posFilterThreshold)
					posRemove = true;
				
				if (negRemove || posRemove) {
					//remove target
					System.out.println("Removing target: " + profile.getProfileID() + "," + target.getProfileID() + ": " + profile.getProfileStr() + ", " + target.getProfileStr());
					System.out.println(tp + ", " + fp + ", " + prec);
					if (negRemove)
						System.out.println("Neg remove");
					else
						System.out.println("Pos remove");
					
					if (pw != null) {
						pw.println("Removing target: " + profile.getProfileID() + "," + target.getProfileID() + ": " + profile.getProfileStr() + ", " + target.getProfileStr());
						pw.println(tp + ", " + fp + ", " + prec);
						if (negRemove)
							pw.println("Neg remove");
						else
							pw.println("Pos remove");
						pw.flush();
					}
					//targetGridList.remove(j);
					
					//Map<AnnotationSequenceGrid, Boolean> targetFilterGrid = profileGrid.getTargetGridMap();
					//targetFilterGrid.put(targetGrid, true);
					//targetGridMap.put(targetGrid, false);
					
					iter.remove();
					
					posMap.remove(profileID + "|" + targetID);
					negMap.remove(profileID + "|" + targetID);
					//j--;
					
					if (write) {
						try {
							pstmt4.setLong(1, profileID);
							pstmt4.setLong(2, targetID);
							ResultSet rs = pstmt4.executeQuery();
							boolean insert =true;
							if (rs.next()) {
								int count = rs.getInt(1);
								if (count > 0) 
									insert = false;
							}
							
							if (insert) {
								pstmt2.setLong(1, profileID);
								pstmt2.setLong(2, targetID);
								pstmt2.setDouble(3, -1.0);
								pstmt2.execute();
							}
							else {
								pstmt3.setDouble(1, -1.0);
								pstmt3.setLong(2, profileID);
								pstmt3.setLong(3, targetID);
								pstmt3.executeQuery();
							}
						}
						catch(SQLException e)
						{
						}
					}
				}
			}			
		}
	}
	
	private boolean getProfileStats(MSAProfile profile)
	{
		int tp = profile.getTruePos();
		int fp = profile.getFalsePos();
		double score = ((double) tp) / ((double) (tp + fp));
		if (score > negFilterThreshold) {
			return true;
		}
		
		return false;

	}
	
	private Map<String, Boolean> readAnswers(String annotType, String provenance) throws SQLException
	{
		Map<String, Boolean> ansMap = new HashMap<String, Boolean>();
		
		/*
		PreparedStatement pstmt = conn.prepareStatement("select a.start, a.end from annotation a, annotation b where a.document_id = ? and a.annotation_type = 'Token' and "
			+ "a.start >= b.start and a.end <= b.end and b.annotation_type = '" + annotType + "' and a.document_id = b.document_id order by start");

		if (docIDList == null) {
			docIDList = new ArrayList<Long>();
			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery("select distinct document_id from annotation where provenance = '" + provenance + " and annotation_type = '" + annotType + " order by document_id");
			while (rs.next()) {
				docIDList.add(rs.getLong(1));
			}
			
			stmt.close();
		}
		
		for (long docID : docIDList) {
			pstmt.setLong(1, docID);
			
			ResultSet rs = pstmt.executeQuery();
			while (rs.next()) {
				int start = rs.getInt(1);
				int end = rs.getInt(2);
				//String value = rs.getString(3);
				ansMap.put(docID + "|" + start + "|" + end, true);
				//System.out.println(docID + "|" + start2 + "|" + end2 + ", value: " + value);
			}
			
		}
		*/
		
		String rq = DBConnection.reservedQuote;
		
		Statement stmt = conn.createStatement();
		
		
		/*
		ResultSet rs = stmt.executeQuery("select a.document_id, a.start, a." + rq + "end" + rq + " from " + schema + "annotation a, " + schema + "annotation b where a.annotation_type = 'Token' and "
			+ "a.start >= b.start and a." + rq + "end" + rq + " <= b." + rq + "end" + rq + " and b.annotation_type = '" + annotType + "' "
			+ "and b.provenance = '" + provenance + "' and a.document_id = b.document_id order by start");
			//and a.document_id < 1163 order by start");
		
		while (rs.next()) {
			long docID = rs.getLong(1);
			int start = rs.getInt(2);
			int end = rs.getInt(3);
			ansMap.put(docID + "|" + start + "|" + end, true);
		}
		*/
		
		
		
		
		ResultSet rs = stmt.executeQuery("select distinct document_id, start, " + rq + "end" + rq
			+ "from " + schema + "annotation where annotation_type = '" + annotType
			+ "' and provenance = '" + provenance + "' order by document_id, start");
		
		while (rs.next()) {
			long docID = rs.getLong(1);
			int start = rs.getInt(2);
			int end = rs.getInt(3);
			
			ansMap.put(docID + "|" + start + "|" + end, true);
		}
		
		
		//pstmt.close();
		stmt.close();
		
		return ansMap;
	}
}
