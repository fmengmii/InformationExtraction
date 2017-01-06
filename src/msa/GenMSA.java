package msa;

import java.io.FileReader;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.google.gson.Gson;

import align.*;
import msa.db.CassandraDBInterface;
import msa.db.MSADBException;
import msa.db.MSADBInterface;
import msa.db.MySQLDBInterface;
import ngram.GenNGrams;
import ngram.NGram;
import ngram.NGramList;
import nlputils.sequence.SequenceUtilities;
import utils.db.DBConnection;

public class GenMSA
{

	private Gson gson;
	private Map<String, MultipleSequenceAlignment> msaMap;
	private List<MultipleSequenceAlignment> msaList;
	private List<List<String>> profileList;
	private List<String> profileStrList;
	private Map<String, List<NGram>> globalContextVecMap;
	private SmithWatermanDim sw;
	private GenAnnotationGrid genGrid;
	private SmithWatermanMulti swMulti;
	private Boolean punct;
	private List<AnnotationSequence> seqList;
	//private List<List<String>> toksList;
	private List<AnnotationSentence> sentList;
	private List<AnnotationSequenceGrid> gridList;
	private List<String> annotTypeNameList;
	private List<Double> scoreList;
	
	private String host;
	private String dbType;
	private String docNamespace;
	private String docTable;
	private String msaKeyspace;
	private String group;
	private boolean write;
	private String provenanceEval;
	
	private boolean verbose;
	private List<Map<String, Object>> annotFilterList;
	private int limit;
	private boolean requireTarget = false;
	private String tokType;
	private String targetType;
	private String targetProvenance;
	
	private MSADBInterface db;
	private Connection docDBConn;
	
	private int maxGaps = 2;
	private int syntax = 2;
	private int phrase = 0;
	private int maxGapsGlobal = 10;
	private double profileThreshold = 0.8;
	
	private List<Long> docIDList = null;
	private List<String> profileGroupList;
	
	private Map<String, Double> ngramMap;
	private Map<Long, List<NGramList>> ngramListMap;
	
	private GenNGrams genNGrams;
	
	private int minSize = 1;
	private int msaMinRows;
	private boolean matchSize = false;
	
	
	public GenMSA()
	{
		gson = new Gson();
		sw = new SmithWatermanDim();
		sw.setMultiMatch(true);
		
		//sw.setVerbose(true);
		swMulti = new SmithWatermanMulti();
		genNGrams = new GenNGrams();
		//genGrid = new GenAnnotationGrid();
	}
	
	public void setGroup(String group)
	{
		this.group = group;
	}
	
	public void setRequireTarget(boolean requireTarget)
	{
		this.requireTarget = requireTarget;
	}
	
	public void setPunct(boolean punct)
	{
		this.punct = punct;
	}
	
	public void setAnnotFilterList(List<Map<String, Object>> annotFilterList)
	{
		this.annotFilterList = annotFilterList;
	}
	
	public void setDB(MSADBInterface db)
	{
		this.db = db;
	}
	
	public void setDocNamespace(String docNamespace)
	{
		this.docNamespace = docNamespace;
	}
	
	public void setDocTable(String docTable)
	{
		this.docTable = docTable;
	}
	
	public void setDocIDList(List<Long> docIDList)
	{
		this.docIDList = docIDList;
	}
	
	public void setLimit(int limit)
	{
		this.limit = limit;
	}
	
	public void setSeqList(List<AnnotationSequence> seqList)
	{
		this.seqList = seqList;
	}
	
	public void setSentList(List<AnnotationSentence> sentList)
	{
		this.sentList = sentList;
	}
	
	public void setMaxGaps(int maxGaps)
	{
		this.maxGaps = maxGaps;
	}
	
	public void setGapPenalty(double gapPenalty)
	{
		sw.setGapPenalty(gapPenalty);
	}
	
	public List<AnnotationSequenceGrid> getGridList()
	{
		return gridList;
	}
	
	public void setGenGrid(GenAnnotationGrid genGrid)
	{
		this.genGrid = genGrid;
	}
	
	public void setTokType(String tokType)
	{
		this.tokType = tokType;
	}
	
	public void setGridList(List<AnnotationSequenceGrid> gridList)
	{
		this.gridList = gridList;
	}
	
	public void setVerbose(boolean verbose)
	{
		this.verbose = verbose;
	}
	
	public void setMatchSize(boolean matchSize)
	{
		this.matchSize = matchSize;
	}
	
	public void setMinSize(int minSize)
	{
		this.minSize = minSize;
	}
	
	public void setMsaMinRows(int msaMinRows)
	{
		this.msaMinRows = msaMinRows;
	}
	
	public void setAnnotTypeNameList(List<String> annotTypeNameList)
	{
		this.annotTypeNameList = annotTypeNameList;
	}
	
	public void setScoreList(List<Double> scoreList)
	{
		this.scoreList = scoreList;
	}
	
	public void setSyntax(int syntax)
	{
		this.syntax = syntax;
	}
	
	public void setPhrase(int phrase)
	{
		this.phrase = phrase;
	}
	
	public void init(String user, String password, String config)
	{
		try {
			Properties props = new Properties();
			props.load(new FileReader(config));
			
			host = props.getProperty("host");
			String keyspace = props.getProperty("keyspace");
			msaKeyspace = props.getProperty("msaKeyspace");
			dbType = props.getProperty("dbType");
			
			docNamespace = props.getProperty("docNamespace");
			docTable = props.getProperty("docTable");
			group = props.getProperty("group");
			String provenance = props.getProperty("provenance");
			provenanceEval = props.getProperty("provenanceEval");
			
			punct = Boolean.parseBoolean(props.getProperty("punct"));
			write = Boolean.parseBoolean(props.getProperty("write"));
			verbose = Boolean.parseBoolean(props.getProperty("verbose"));
			maxGaps = Integer.parseInt(props.getProperty("maxGaps"));
			requireTarget = Boolean.parseBoolean(props.getProperty("requireTarget"));
			
			syntax = Integer.parseInt(props.getProperty("syntax"));
			
			limit = -1;
			String limitStr = props.getProperty("limit");
			if (limitStr != null)
				limit = Integer.parseInt(limitStr);
			
			if (dbType.equals("cassandra"))
				db = new CassandraDBInterface();
			else if (dbType.equals("mysql"))
				db = new MySQLDBInterface();
				
			db.init(user, password, host, keyspace, msaKeyspace);
			
			annotFilterList = new ArrayList<Map<String, Object>>();
			annotFilterList = gson.fromJson(props.getProperty("annotFilterList"), annotFilterList.getClass());
			tokType = props.getProperty("tokType");
			targetType = props.getProperty("targetType");
			targetProvenance = props.getProperty("targetProvenance");
			minSize = Integer.parseInt(props.getProperty("minSize"));
			matchSize = Boolean.parseBoolean(props.getProperty("matchSize"));
			
			if (targetType != null) {
				Map<String, Object> targetMap = new HashMap<String, Object>();
				targetMap.put("annotType", targetType);
				targetMap.put("target", true);
				targetMap.put("provenance", targetProvenance);
				annotFilterList.add(targetMap);
			}
			
			List<String> annotTypeNameList = MSAUtils.getAnnotationTypeNameList(annotFilterList, tokType);
			
			genGrid = new GenAnnotationGrid();
			genGrid.setAnnotTypeNameList(annotTypeNameList);
			genGrid.setTokType(tokType);
			
			profileGroupList = new ArrayList<String>();
			profileGroupList = gson.fromJson(props.getProperty("profileGroupList"), profileGroupList.getClass());
			
			
			//check for docID list info
			String docDBUser = props.getProperty("docDBUser");
			if (docDBUser != null) {
				String docDBPassword = props.getProperty("docDBPassword");
				String docDBHost = props.getProperty("docDBHost");
				String docDBName = props.getProperty("docDBName");
				String docDBType = props.getProperty("docDBType");
				String docDBQuery = props.getProperty("docDBQuery");
				docDBConn = DBConnection.dbConnection(docDBUser, docDBPassword, docDBHost, docDBName, docDBType);

				getDocIDList(docDBUser, docDBPassword, docDBHost, docDBName, docDBType, docDBQuery);
				
				docDBConn.close();
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	public void genSentences()
	{
		//get sentences
		System.out.println("getting sentences...");
		
		GenSentences genSent = new GenSentences();
		genSent.setRequireTarget(requireTarget);
		genSent.setPunct(punct);
		genSent.setTokenType(tokType);
		genSent.setVerbose(verbose);
		genSent.init(db, annotFilterList, targetType, targetProvenance);
		
		if (docIDList == null)
			genSent.genSentences(docNamespace, docTable, null, limit);
		else {
			genSent.setDocIDList(docIDList);
			genSent.genSentenceAnnots(docNamespace, docTable);
		}
		
		seqList = genSent.getSeqList();
		sentList = genSent.getSentList();
	}
	
	public void genTargetPhrases() throws MSADBException
	{
		GenSentences genSent = new GenSentences();
		genSent.setRequireTarget(requireTarget);
		genSent.setPunct(punct);
		genSent.setTokenType(tokType);
		genSent.setVerbose(verbose);
		genSent.setDocIDList(docIDList);
		genSent.init(db, annotFilterList, targetType, targetProvenance);
		
		genSent.genTargetPhrases(docNamespace, docTable, targetType, targetProvenance);
		
		seqList = genSent.getSeqList();
	}
			
	public List<MultipleSequenceAlignment> genMSA()
	{
		List<MultipleSequenceAlignment> msaList = new ArrayList<MultipleSequenceAlignment>();
		
		try {
			if (gridList == null) {
				gridList = new ArrayList<AnnotationSequenceGrid>();
				for (AnnotationSequence seq : seqList) {
					List<AnnotationSequenceGrid> seqGridList = genGrid.toAnnotSeqGrid(seq, requireTarget, true, false, true);
					gridList.addAll(seqGridList);
				}
			}
			
			msaMap = new HashMap<String, MultipleSequenceAlignment>();
			
			sw.setGapPenalty(1000000.0);
			//sw.setVerbose(verbose);
			sw.setScoreMap(annotTypeNameList, scoreList);
			
			for (int i=0; i<gridList.size()-1; i++) {
				AnnotationSequenceGrid grid1 = gridList.get(i);
				AnnotationSequence seq1 = grid1.getSequence();
				int size1 = grid1.size();
				
				//List<String> toks1 = seq1.getToks(tokType);
				List<String> toks1 = seq1.getToks();
				String toks1Str = SequenceUtilities.getStrFromToks(toks1);

				
				/*
				if (size1 < 3) {
					List<String> prevToks = new ArrayList<String>();
					if (i > 0)
						prevToks = sentList.get(i-1).getToks();
					List<String> nextToks = new ArrayList<String>();
					if (i < sentList.size()-1) {
						nextToks = sentList.get(i+1).getToks();
					}
					
					int count = 0;
					for (String tok : prevToks) {
						if (tok.equals(":target"))
							tok = ":number";
						toks1.add(count, tok);
						count++;
					}
					
					for (String tok : nextToks){
						if (tok.equals(":target"))
							tok = ":number";
						toks1.add(tok);
					}
				}
				*/
				
				
				
				long docID1 = -1;
				int sentID1 = -1;
				if (seq1 != null) {
					docID1 = seq1.getDocID();
					sentID1 = seq1.getSentID();
				}
				
				SentenceInfo sentInfo1 = new SentenceInfo(docNamespace, docTable, docID1, sentID1);
				
				
				for (int j=i+1; j<gridList.size(); j++) {
					AnnotationSequenceGrid grid2 = gridList.get(j);
					AnnotationSequence seq2 = grid2.getSequence();
					
					//List<String> toks2 = seq2.getToks(tokType);
					List<String> toks2 = seq2.getToks();
					String toks2Str = SequenceUtilities.getStrFromToks(toks2);
					
					
					
					/*
					if (toks2.size() < 3) {
						List<String> prevToks = new ArrayList<String>();
						if (j > 0)
							prevToks = sentList.get(j-1).getToks();
						List<String> nextToks = new ArrayList<String>();
						if (j < sentList.size()-1) {
							nextToks = sentList.get(j+1).getToks();
						}
						
						int count = 0;
						for (String tok : prevToks) {
							if (tok.equals(":target"))
								tok = ":number";
							toks2.add(count, tok);
							count++;
						}
						
						for (String tok : nextToks){
							if (tok.equals(":target"))
								tok = ":number";
							toks2.add(tok);
						}
					}
					*/
					
					

					
					/*
					if (toks2.size() == 0)
						continue;
						*/

					
					long docID2 = -1;
					int sentID2 = -1;
					if (seq2 != null) {
						docID2 = seq2.getDocID();
						sentID2 = seq2.getSentID();
					}
					
					SentenceInfo sentInfo2 = new SentenceInfo(docNamespace, docTable, docID2, sentID2);
					
					/*
					System.out.println("\n\ntoks1: " + toks1Str);
					System.out.println("toks2: " + toks2Str);
					System.out.println("grid1: " + grid1.toString());
					System.out.println("grid2: " + grid2.toString());
					*/
					
					
					sw.align(grid1, grid2);
					
					List<String> align1 = sw.getAlignment1();
					List<String> align2 = sw.getAlignment2();
					List<String> alignToks1 = sw.getAlignToks1();
					List<String> alignToks2 = sw.getAlignToks2();
					
					
					int gaps1 = MSAUtils.countGaps(align1, "|||");
					int gaps2 = MSAUtils.countGaps(align2, "|||");
					int syntax1 = MSAUtils.countSyntax(align1);
					int syntax2 = MSAUtils.countSyntax(align2);
					int phrase1 = MSAUtils.countPhrase(align1);
					int phrase2 = MSAUtils.countPhrase(align2);
					
					List<String> align1NoGaps = MSAUtils.removeGaps(align1, "|||");
					List<String> align2NoGaps = MSAUtils.removeGaps(align2, "|||");
					
					List<Integer> matchIndexes1 = sw.getMatchIndexes1();
					List<Integer> matchIndexes2 = sw.getMatchIndexes2();
					
					List<int[]> matchCoords1 = sw.getMatchCoords1();
					List<int[]> matchCoords2 = sw.getMatchCoords2();
					
					
					
					//if (align1.indexOf(":token|string|said") >= 0) {
					//System.out.println("align1: " + gson.toJson(align1));
					//System.out.println("align2: " + gson.toJson(align2));
					//System.out.println("matchCoords1: " + gson.toJson(matchCoords1));
					//System.out.println("matchCoords2: " + gson.toJson(matchCoords2));
					//}
					
					
					
					if (gaps1 <= maxGaps && gaps2 <= maxGaps && syntax1 <= syntax && syntax2 <= syntax && phrase1 <= phrase && phrase2 <= phrase &&
						matchIndexes1.size() >= minSize && matchIndexes2.size() >= minSize)	{
						
						if (verbose) {
							System.out.println("\n\nalign toks1: " + toks1Str);
							System.out.println("align toks2: " + toks2Str);
						}
						
					
						//if (verbose) {
							String align1Str = SequenceUtilities.getStrFromToks(align1);
							String align2Str = SequenceUtilities.getStrFromToks(align2);
							//System.out.println("\ntoks1Str: " + toks1Str);
							//System.out.println("toks2Str: " + toks2Str);
							//System.out.println("align1Str: " + align1Str);
							//System.out.println("align2Str: " + align2Str + "\n");
							
							String alignToks1Str = SequenceUtilities.getStrFromToks(alignToks1);
							String alignToks2Str = SequenceUtilities.getStrFromToks(alignToks2);
							//System.out.println("\ntoks1Str: " + toks1Str);
							//System.out.println("toks2Str: " + toks2Str);
							//System.out.println("alignToks1Str: " + alignToks1Str);
							//System.out.println("alignToks2Str: " + alignToks2Str + "\n");
							
							//System.out.println("matchIndexes1: " + gson.toJson(matchIndexes1));
							//System.out.println("matchIndexes2: " + gson.toJson(matchIndexes2));
							//System.out.println("matchCoords1: " + gson.toJson(matchCoords1));
							//System.out.println("matchCoords2: " + gson.toJson(matchCoords2));
							//System.out.println("gaps1: " + gaps1 + ", gaps2: " + gaps2 + ", syntax1: " + syntax1 + ", syntax2: " + syntax2);
						//}
						
						
						if (matchSize) {							
							int[] coords1 = matchCoords1.get(matchCoords1.size()-1);
							int start1 = matchCoords1.get(0)[0];
							int end1 = grid1.get(coords1[0]).get(coords1[1]).getEndIndex();
							
							int[] coords2 = matchCoords2.get(matchCoords2.size()-1);
							int start2 = matchCoords2.get(0)[0];
							int end2 = grid2.get(coords2[0]).get(coords2[1]).getEndIndex();
							
							if (!(start1 == 0 && end1 == grid1.size()) && !(start2 == 0 && end2 == grid2.size()))
								continue;
						}
						
						List<String> matchTokens = new ArrayList<String>();
						for (int matchIndex : matchIndexes1) {
							matchTokens.add(alignToks1.get(matchIndex));
						}
						
						String matchTokensStr = gson.toJson(matchTokens);
						
						//System.out.println("matchTokensStr: " + matchTokensStr);
						
						MultipleSequenceAlignment msa = msaMap.get(matchTokensStr);
						if (msa == null) {
							msa = new MultipleSequenceAlignment();
							msaMap.put(matchTokensStr, msa);
							msa.addRow(alignToks1, matchIndexes1, sentInfo1);
							msa.addRow(alignToks2, matchIndexes2, sentInfo2);
						}
						else
							msa.addRow(alignToks2, matchIndexes2, sentInfo2);					
					}
				}
			}
			
			int msaID = 0;
			Iterator<String> msaIter = msaMap.keySet().iterator();
			profileList = new ArrayList<List<String>>();
			profileStrList = new ArrayList<String>();
			
			System.out.println("\n\ngenmsa profiles...");
			
			
			for (;msaIter.hasNext();) {
				String msaKey = msaIter.next();
				MultipleSequenceAlignment msa = msaMap.get(msaKey);
				
				String profileStr = msa.toProfileString(false);
				System.out.println("profile: " + profileStr);
				
				//if (msa.getNumRows() > minSize) {
				if (msa.getTotal() >= msaMinRows) {
					msa.setID(msaID++);
					msaList.add(msa);
					//String profileStr = msa.toProfileString(false);
					List<String> profile = SequenceUtilities.getToksFromStr(profileStr);
					
					boolean inserted = false;
					for (int i=0; i<profileStrList.size(); i++) {
						String profileStr2 = profileStrList.get(i);
						if (profileStr2.length() > profileStr.length()) {
							profileStrList.add(i, profileStr);
							profileList.add(i, profile);
							inserted = true;
							break;
						}
					}
					
					if (!inserted) {
						profileStrList.add(profileStr);
						profileList.add(profile);
					}
					
					//profileList.add(SequenceUtilities.getToksFromStr(profileStr));
					//profileStrList.add(profileStr);
					
					//System.out.println(msa.toProfileString(false));
					//System.out.println(msa.toString());
					
					/*
					if (write) {
						db.writeMSAToDB(msa, docNamespace, docTable, group);
					}
					*/
				}
				
			}
			
			for (String profileStr : profileStrList) {
				System.out.println(profileStr);
			}
			
			System.out.println("\n\n");
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		
		return msaList;
	}
	
	/*
	private Map<String, List<NGram>> genGlobalContextVectors(Map<String, List<List<NGram>>> globalVecMap)
	{
		Map<String, List<NGram>> globalContextMap = new HashMap<String, List<NGram>>();
		for (String annotType : globalVecMap.keySet()) {
			List<List<NGram>> ngramListList = globalVecMap.get(annotType);
			
			List<NGram> ngramList = ngramListList.get(0);
			List<String> toks1 = genNGrams.getToksFromNGramList(ngramList);
			
			if (toks1.size() < 2)
				continue;
			
			for (int i=1; i<ngramListList.size(); i++) {
				List<NGram> ngramList2 = ngramListList.get(i);
				List<String> toks2 = genNGrams.getToksFromNGramList(ngramList2);
				
				if (toks2.size() < 2)
					continue;
				
				sw.align(toks1, toks2);
				List<String> align1 = sw.getAlignment1();
				List<String> align2 = sw.getAlignment2();
				
				List<String> align1NoGaps = MSAUtils.removeGaps(align1, "|||");
				List<String> align2NoGaps = MSAUtils.removeGaps(align2, "|||");
				
				int gaps1 = MSAUtils.countGaps(align1, "|||");
				int gaps2 = MSAUtils.countGaps(align2, "|||");
				
				List<Integer> matchIndexes1 = sw.getMatchIndexesList1();
				List<Integer> matchIndexes2 = sw.getMatchIndexesList2();
				
				if (gaps1 <= maxGapsGlobal && gaps2 <= maxGapsGlobal && align1NoGaps.size() > 1 && align2NoGaps.size() > 1) {
					for (int index : matchIndexes1) {
						ngramList.get(index).increment();
					}
					
					//insert ngrams into original vector
					int offset = matchIndexes1.get(0) - matchIndexes2.get(0);
					
					boolean inserted = false;
					for (NGram ngram2 : ngramList2) {
						for (int j=0; j<ngramList.size(); j++) {
							
							if (matchIndexes1.contains(j))
								continue;
							
							NGram ngram = ngramList.get(j);
							if (ngram.getIndex() > ngram2.getIndex() - offset) {
								ngramList.add(j, ngram2);
								inserted = true;
								break;
							}
						}
						
						if (!inserted)
							ngramList.add(ngram2);
					}
				}
			}
			
			globalContextMap.put(annotType, ngramList);
		}
		
		return globalContextMap;
	}
	*/
	
	/*
	private List<List<String>> getGlobalContext(long docID1, long start1, long docID2, long start2, Map<Long, List<NGramList>> ngramListMap)
	{
		List<List<String>> globalToksList = new ArrayList<List<String>>();
		
		List<NGramList> ngramListList1 = ngramListMap.get(docID1);
		List<NGramList> ngramListList2 = ngramListMap.get(docID2);
		
		List<String> context1 = new ArrayList<String>();
		for (NGramList ngramList : ngramListList1) {
			if (ngramList.getStart() < start1) {
				context1.addAll(ngramList.getNgramList());
			}
			else
				break;
		}
		
		List<String> context2 = new ArrayList<String>();
		for (NGramList ngramList : ngramListList2) {
			if (ngramList.getStart() < start2) {
				context2.addAll(ngramList.getNgramList());
			}
			else
				break;
		}
		
		if (context1.size() > 0 && context2.size() > 0) {
			sw.align(context1, context2);
			List<String> align1 = sw.getAlignment1();
			List<String> align2 = sw.getAlignment2();
			
			List<String> align1NoGaps = MSAUtils.removeGaps(align1, "|||");
			List<String> align2NoGaps = MSAUtils.removeGaps(align2, "|||");
			
			int gaps1 = MSAUtils.countGaps(align1, "|||");
			int gaps2 = MSAUtils.countGaps(align2, "|||");
			
			if (gaps1 <= maxGapsGlobal && gaps2 <= maxGapsGlobal && align1NoGaps.size() > 0 && align2NoGaps.size() > 0) {
				replaceSpaces(align1NoGaps);
				replaceSpaces(align2NoGaps);
				globalToksList.add(align1NoGaps);
				globalToksList.add(align2NoGaps);
			}
		}
		
		
		return globalToksList;
	}
	*/
	
	private void replaceSpaces(List<String> toks)
	{	
		for (int i=0; i<toks.size(); i++) {
			String tok = toks.get(i);
			toks.set(i, tok.replaceAll(" ", "-"));
		}
	}
	
	private void getDocIDList(String docDBUser, String docDBPassword, String docDBHost, String docDBName, String docDBType, String docDBQuery) throws Exception
	{
		//Connection docDBConn = DBConnection.dbConnection(docDBUser, docDBPassword, docDBHost, docDBName, docDBType);
		java.sql.Statement stmt = docDBConn.createStatement();
		java.sql.ResultSet rs = stmt.executeQuery(docDBQuery);
		
		docIDList = new ArrayList<Long>();
		
		while (rs.next()) {
			docIDList.add(rs.getLong(1));
		}
		
		stmt.close();
		//docDBConn.close();
		
	}
	
	public void close() throws MSADBException
	{
		if (db != null)
			db.close();
	}
	
	public static void main(String[] args)
	{
		if (args.length != 3) {
			System.out.println("usage: user password config");
			System.exit(0);
		}
		
		try {
			GenMSA genMSA = new GenMSA();
			genMSA.init(args[0], args[1], args[2]);
			//genMSA.genSentences();
			genMSA.genTargetPhrases();
			genMSA.genMSA();
			genMSA.close();
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
}
