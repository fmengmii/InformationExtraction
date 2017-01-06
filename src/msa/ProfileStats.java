package msa;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
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
	
	private double filterThreshold = 0.2;
	private int filterMinCount = 20;
	private int blockSize = 10;
	private int internalBlockSize = 10;

	
	private ProfileMatcher profileMatcher;
	private boolean keepUnmatched = false;
	
	//private Map<String, MSAProfile> msaProfileMap;
	
	private PreparedStatement pstmt;
	private PreparedStatement pstmt2;
	
	private Map<String, Integer> posMap;
	private Map<String, Integer> negMap;
	
	private List<Long> docIDList;
	
	private boolean write = false;
	
	private String host;
	
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
		this.host = host;
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
	
	public void setFilterThreshold(double filterThreshold)
	{
		this.filterThreshold = filterThreshold;
	}
	
	public void setFilterMinCount(int filterMinCount)
	{
		this.filterMinCount = filterMinCount;
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
	
	public void setHost(String host)
	{
		this.host = host;
	}
	
	public void setDocIDList(List<Long> docIDList)
	{
		this.docIDList = docIDList;
	}
	
	public void getProfileStats(List<AnnotationSequenceGrid> gridList, List<ProfileGrid> profileGridList, String annotType, int profileType, String keyspace, 
		String msaKeyspace, String provenance, Map<AnnotationSequenceGrid, MSAProfile> msaProfileMap, Map<AnnotationSequenceGrid, MSAProfile> msaTargetProfileMap, String indexTableName)
	{
		System.out.println("filtering...");
		
		posMap = new HashMap<String, Integer>();
		negMap = new HashMap<String, Integer>();
		
		
		/*
		if (msaProfileMap == null)
			msaProfileMap = new HashMap<String, MSAProfile>();
			*/
		
		
		//sw.setScoreMap(annotTypeNameList);
		//sw.setVerbose(true);
		profileMatcher.setAligner(sw);
		profileMatcher.setProfileMatch(true);
		
		//Map<String, List<MSAProfile>> finalPatternMap = new HashMap<String, List<MSAProfile>>();

		try {
			conn = DBConnection.dbConnection("fmeng", "fmeng", host, keyspace, "mysql");
			//conn = DBConnection.dbConnection("fmeng", "fmeng", "localhost", keyspace, "mysql");
			msaConn = DBConnection.dbConnection("fmeng", "fmeng", host, msaKeyspace, "mysql");
			
			pstmt = msaConn.prepareStatement("select document_id, start, end from msa_profile_match_index where profile_id = ? and target_id = ?");
			pstmt2 = msaConn.prepareStatement("select count(*) from msa_profile_match_index where profile_id = ?");
			
			MatchWriter matchWriter = new MatchWriter();
			matchWriter.init("fmeng", "fmeng", host, msaKeyspace, "mysql", indexTableName);
			
			System.out.println("reading answers...");
			ansMap = readAnswers(annotType, provenance);


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
				List<ProfileMatch> matchList = profileMatcher.matchProfile(gridList, profileGridList, annotType, false, maxGaps, syntax, phrase, true, start, end, msaProfileMap, msaTargetProfileMap);
				
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
				removeLowPrecision(matchList, profileGridList, msaProfileMap, msaTargetProfileMap);
				
				
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
			
			pstmt.close();
			conn.close();
			msaConn.close();

			matchWriter.close();
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	private void removeLowPrecision(List<ProfileMatch> matchList, List<ProfileGrid> profileGridList, Map<AnnotationSequenceGrid, MSAProfile> msaProfileMap, 
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
			String profileID = Long.toString(profile.getProfileID());
			
			List<AnnotationSequenceGrid> targetGridList = profileGrid.getTargetGridList();
				
			Integer tp = posMap.get(profileID);
			if (tp == null)
				tp = 0;
			Integer fp = negMap.get(profileID);
			if (fp == null)
				fp = 0;
			
			double prec = ((double) tp) / ((double) (tp + fp));
			if ((tp + fp) >= filterMinCount && prec < filterThreshold) {
				//remove profile
				System.out.println("Removing: " + profile.getProfileStr());
				System.out.println(tp + ", " + fp + ", " + prec);
				profileGridList.remove(i);
				posMap.remove(profileID);
				negMap.remove(profileID);
				i--;
			}
			
			for (int j=0; j<targetGridList.size(); j++) {
				AnnotationSequenceGrid targetGrid = targetGridList.get(j);
				MSAProfile target = msaTargetProfileMap.get(targetGrid);
				long targetID = target.getProfileID();
				
				tp = posMap.get(profileID + "|" + targetID);
				if (tp == null)
					tp = 0;
				fp = negMap.get(profileID + "|" + targetID);
				if (fp == null)
					fp = 0;
				
				prec = ((double) tp) / ((double) (tp + fp));
				if ((tp + fp) >= 10 && prec < .9) {
					//remove target
					System.out.println("Removing target: " + profile.getProfileStr() + ", " + target.getProfileStr());
					System.out.println(tp + ", " + fp + ", " + prec);
					targetGridList.remove(j);
					posMap.remove(profileID + "|" + targetID);
					negMap.remove(profileID + "|" + targetID);
					j--;
				}
			}
		}
	}
	
	private boolean getProfileStats(MSAProfile profile)
	{
		int tp = profile.getTruePos();
		int fp = profile.getFalsePos();
		double score = ((double) tp) / ((double) (tp + fp));
		if (score > filterThreshold) {
			return true;
		}
		
		return false;

	}
	
	private Map<String, Boolean> readAnswers(String annotType, String provenance) throws SQLException
	{
		Map<String, Boolean> ansMap = new HashMap<String, Boolean>();
		
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
		
		pstmt.close();
		
		return ansMap;
	}
}
