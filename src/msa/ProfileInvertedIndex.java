package msa;

import java.util.*;
import align.*;
import nlputils.sequence.SequenceUtilities;

import java.sql.*;


public class ProfileInvertedIndex
{
	private Map<String, Map<ProfileGrid, Integer>> wordMap;
	private Map<AnnotationSequenceGrid, Integer> profileMaxMap;
	private Map<String, List<AnnotationSequenceGrid>> targetMap;
	private boolean targetFlag = false;
	private int maxGaps = 0;
	
	private Map<String, Map<ProfileGrid, Integer>> bigramMap;
	
	public ProfileInvertedIndex()
	{
	}
	
	public void setTargetFlag(boolean targetFlag)
	{
		this.targetFlag = targetFlag;
	}
	
	public void setMaxGaps(int maxGaps)
	{
		this.maxGaps = maxGaps;
	}
	
	public void genIndex(List<ProfileGrid> profileGridList, List<AnnotationSequenceGrid> targetGridList, Map<Long, ProfileGrid> profileIDMap, Map<Long, AnnotationSequenceGrid> targetIDMap) throws SQLException
	{
		wordMap = new HashMap<String, Map<ProfileGrid, Integer>>();
		bigramMap = new HashMap<String, Map<ProfileGrid, Integer>>();
		profileMaxMap = new HashMap<AnnotationSequenceGrid, Integer>();
		targetMap = new HashMap<String, List<AnnotationSequenceGrid>>();
		
		
		for (ProfileGrid profileGrid : profileGridList) {
			AnnotationSequenceGrid grid = profileGrid.getGrid();
			//System.out.println(grid.toString());
			
			String lastTok = null;
			for (int i=0; i<grid.size(); i++) {
				List<AnnotationGridElement> col = grid.get(i);
				//for (AnnotationGridElement elem : col) {
					String tok = col.get(0).getTok();
					if (tok.startsWith(":relation.")) {
						int index = tok.lastIndexOf("|");
						tok = tok.substring(0, index);
					}
					
					
					Map<ProfileGrid, Integer> gridMap = wordMap.get(tok);
					if (gridMap == null) {
						gridMap = new HashMap<ProfileGrid, Integer>();
						wordMap.put(tok, gridMap);
					}
					
					Integer count = gridMap.get(profileGrid);
					if (count == null)
						count = 0;
					
					gridMap.put(profileGrid, ++count);	
					
					//added support for bigrams
					String bigram = null;
					if (lastTok != null) {
						bigram = lastTok  + "|||" + tok;
						
						//System.out.println("profile bigram: " + bigram);

						gridMap = bigramMap.get(bigram);
						if (gridMap == null) {
							gridMap = new HashMap<ProfileGrid, Integer>();
							bigramMap.put(bigram, gridMap);
						}
						
						count = gridMap.get(profileGrid);
						if (count == null)
							count = 0;
						
						gridMap.put(profileGrid, ++count);
					}
				//}
					lastTok = tok;
			}
			
			profileMaxMap.put(grid, grid.size()-1);
						
			
			/*
			Map<String, Boolean> profileTargetMap = targetGridMap.get(profileGrid);
			List<AnnotationSequenceGrid> targetGridList = profileGrid.getTargetGridList();
			for (AnnotationSequenceGrid targetGrid : targetGridList) {
				for (int i=0; i<targetGrid.size(); i++) {
					List<AnnotationGridElement> col = targetGrid.get(i);
					for (AnnotationGridElement elem : col) {
						String tok = elem.getTok();
						profileTargetMap.put(tok, true);
					}
				}
			}
			*/
		}
		
		for (AnnotationSequenceGrid targetGrid : targetGridList) {
			
			for (int i=0; i<targetGrid.size(); i++) {
				List<AnnotationGridElement> col = targetGrid.get(i);
				String tok = col.get(0).getTok();
				
				//System.out.println("target tok: " + tok);
	
				List<AnnotationSequenceGrid> targetList = targetMap.get(tok);
				if (targetList == null) {
					targetList = new ArrayList<AnnotationSequenceGrid>();
					targetMap.put(tok, targetList);
				}
				
				targetList.add(targetGrid);
				
				profileMaxMap.put(targetGrid, targetGrid.size());
			}
		}
	}
	
	public List<ProfileGrid> getMatchedGridList(AnnotationSequenceGrid grid, String annotType)
	{
		List<ProfileGrid> profileGridList = new ArrayList<ProfileGrid>();
		Map<ProfileGrid, Integer> countMap = new HashMap<ProfileGrid, Integer>();
		Map<ProfileGrid, Integer> bigramCountMap = new HashMap<ProfileGrid, Integer>();
		Map<AnnotationSequenceGrid, Integer> targetCountMap = new HashMap<AnnotationSequenceGrid, Integer>();
		Map<String, Boolean> matchMap = new HashMap<String, Boolean>();
		Map<String, Boolean> bigramMatchMap = new HashMap<String, Boolean>();
		
		//System.out.println(grid.toString());
		
		//check tok matches for grids and targets
		//added support for bigrams
		
		List<AnnotationGridElement> lastCol = null;
		
		for (int i=0; i<grid.size(); i++) {
			List<AnnotationGridElement> col = grid.get(i);
			for (AnnotationGridElement elem : col) {
				String tok = elem.getTok();
				
				List<String> bigramList = new ArrayList<String>();
				
				if (lastCol != null) {
					//generate all possible bigrams
					for (AnnotationGridElement gridElem : lastCol) {
						String tok1 = gridElem.getTok();
						
						for (AnnotationGridElement gridElem2 : col) {
							String tok2 = gridElem2.getTok();
							String bigram = tok1 + "|||" + tok2;
							if (bigramMatchMap.get(bigram) == null) {
								bigramMatchMap.put(bigram, true);
								bigramList.add(bigram);
								
								//System.out.println("grid bigram: " + bigram);
							}
						}
					}
				}
				
				if (tok.startsWith(":relation.")) {
					int index = tok.lastIndexOf("|");
					tok = tok.substring(0, index);
				}
				
				if (matchMap.get(tok) == null) {
					matchMap.put(tok, true);
				}
				else
					continue;
				
				Map<ProfileGrid, Integer> gridMap = wordMap.get(tok);
				if (gridMap != null) {
					//System.out.println("matched: " + tok);
					
					for (ProfileGrid profileGrid : gridMap.keySet()) {
						if (profileGrid.getGrid().size() > grid.size())
							continue;
						
						int gridCount = gridMap.get(profileGrid);
						Integer count = countMap.get(profileGrid);
						if (count == null)
							count = 0;
						countMap.put(profileGrid, gridCount + count);
						//System.out.println("count: " + (gridCount + count));
					}
				}
				
				for (String bigram : bigramList) {
					gridMap = bigramMap.get(bigram);
					if (gridMap != null) {
						//System.out.println("matched: " + tok);
						
						for (ProfileGrid profileGrid : gridMap.keySet()) {
							if (profileGrid.getGrid().size() > grid.size())
								continue;
							
							int gridCount = gridMap.get(profileGrid);
							Integer count = bigramCountMap.get(profileGrid);
							if (count == null)
								count = 0;
							bigramCountMap.put(profileGrid, gridCount + count);
							//System.out.println("count: " + (gridCount + count));
						}
					}
				}

				
				//check target toks
				
				List<AnnotationSequenceGrid> targetList = targetMap.get(tok);
				if (targetList == null)
					continue;
				
				//System.out.println("target: " + tok);
				
				for (AnnotationSequenceGrid targetGrid : targetList) {
					Integer count = targetCountMap.get(targetGrid);
					if (count == null)
						count = 0;
					
					targetCountMap.put(targetGrid, ++count);
				}
				
				
			}
			
			lastCol = col;
		}
		
		
		//get active targets
		Map<AnnotationSequenceGrid, Boolean> activeTargetMap = new HashMap<AnnotationSequenceGrid, Boolean>();
		for (AnnotationSequenceGrid targetGrid : targetCountMap.keySet()) {
			int targetCount = targetCountMap.get(targetGrid);
			int targetCount2 = profileMaxMap.get(targetGrid);
			
			if (targetCount >= targetCount2)
				activeTargetMap.put(targetGrid, true);
			
		}
		
		
		//get active grids
		//use bigrams

		/*
		for (ProfileGrid profileGrid : countMap.keySet()) {
			int count = countMap.get(profileGrid);
			
			Integer count2 = profileMaxMap.get(profileGrid.getGrid());
			
			//System.out.println("count: " + count + " count2: " + count2);
			
			//found a matching profile grid
			if (count >= (count2 - maxGaps) - 2) {
				//System.out.println("Inverted Index: " + SequenceUtilities.getStrFromToks(profileGrid.getGrid().getSequence().getToks()));
				profileGridList.add(profileGrid);
				
				//set target grid map for this grid
				Map<AnnotationSequenceGrid, Boolean> targetGridMap = profileGrid.getTargetGridMap();
				for (AnnotationSequenceGrid targetGrid : targetGridMap.keySet()) {
					targetGridMap.put(targetGrid, false);
				}
				
				for (AnnotationSequenceGrid targetGrid : activeTargetMap.keySet()) {
					Boolean flag = targetGridMap.get(targetGrid);
					if (flag != null)
						targetGridMap.put(targetGrid, true);
				}
			}
		}
		*/
		
		
		for (ProfileGrid profileGrid : bigramCountMap.keySet()) {
			int count = bigramCountMap.get(profileGrid);
			
			Integer count2 = profileMaxMap.get(profileGrid.getGrid());
			
			//System.out.println("count: " + count + " count2: " + count2);
			
			//found a matching profile grid
			//total bigrams is one less than total toks
			if (count >= (count2 - maxGaps) - 3) {
				//System.out.println("Inverted Index: " + SequenceUtilities.getStrFromToks(profileGrid.getGrid().getSequence().getToks()));
				profileGridList.add(profileGrid);
				
				//set target grid map for this grid
				Map<AnnotationSequenceGrid, Boolean> targetGridMap = profileGrid.getTargetGridMap();
				for (AnnotationSequenceGrid targetGrid : targetGridMap.keySet()) {
					targetGridMap.put(targetGrid, false);
				}
				
				for (AnnotationSequenceGrid targetGrid : activeTargetMap.keySet()) {
					Boolean flag = targetGridMap.get(targetGrid);
					if (flag != null)
						targetGridMap.put(targetGrid, true);
				}
			}
		}
		
		return profileGridList;
	}
}
