package msa;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.gson.Gson;

import align.AnnotationGridElement;
import align.AnnotationSequenceGrid;
import align.SmithWatermanDim;
import nlputils.sequence.SequenceUtilities;

public class ProfileMatcher
{
	private Gson gson;
	private SmithWatermanDim sw;
	private boolean verbose = true;
	
	private List<ProfileMatch> noMatchList;
	private Map<String, List<String>> profileTargetMap;
	
	private Map<String, Double> targetProbMap;
	
	private int minSizeOffset = 0;
	
	private int maxGridLen = 300;
	
	
	private PrintWriter pw;
	
	private boolean profileMatch;

	
	public ProfileMatcher()
	{
		gson = new Gson();
		sw = new SmithWatermanDim();
	}
	
	public List<ProfileMatch> getNoMatchList()
	{
		return noMatchList;
	}
	
	public void setAligner(SmithWatermanDim sw)
	{
		this.sw = sw;
	}
	
	public void setPrintWriter(PrintWriter pw)
	{
		this.pw  = pw;
	}
	
	public Map<String, List<String>> getProfileTargetMap()
	{
		return profileTargetMap;
	}
	
	public void setProfileMatch(boolean profileMatch)
	{
		this.profileMatch = profileMatch;
		sw.setProfileMatch(profileMatch);
		sw.setMultiMatch(profileMatch);
	}
	
	public void setMinSizeOffset(int minSizeOffset)
	{
		this.minSizeOffset = minSizeOffset;
	}
	
	public void setTargetProbMap(Map<String, Double> targetProbMap)
	{
		this.targetProbMap = targetProbMap;
	}
	
	public List<ProfileMatch> matchProfile(List<AnnotationSequenceGrid> gridList, List<ProfileGrid> profileGridList, List<AnnotationSequenceGrid> targetGridList, String annotType, boolean extraction, 
		int maxGaps, int syntax, int phrase, boolean evaluation,  Map<AnnotationSequenceGrid, MSAProfile> msaProfileMap, Map<AnnotationSequenceGrid, MSAProfile> msaTargetProfileMap, ProfileInvertedIndex invertedIndex)
	{
		return matchProfile(gridList, profileGridList, targetGridList, annotType, extraction, maxGaps, syntax, phrase, evaluation, 0, gridList.size(), msaProfileMap, msaTargetProfileMap, invertedIndex);
	}
	
	public List<ProfileMatch> matchProfile(List<AnnotationSequenceGrid> gridList, List<ProfileGrid> profileGridList, Collection<AnnotationSequenceGrid> targetProfileGridList, String annotType, boolean extraction, 
		int maxGaps, int syntax, int phrase, boolean evaluation, int gridStartIndex, int gridEndIndex,  
		Map<AnnotationSequenceGrid, MSAProfile> msaProfileMap, Map<AnnotationSequenceGrid, MSAProfile> msaTargetProfileMap, ProfileInvertedIndex invertedIndex)
	{
		List<ProfileMatch> matchList = new ArrayList<ProfileMatch>();
		noMatchList = new ArrayList<ProfileMatch>();
		profileTargetMap = new HashMap<String, List<String>>();
		
		int profileSize = profileGridList.size();
		
		for (int i=gridStartIndex; i<gridEndIndex; i++) {

			AnnotationSequenceGrid grid = gridList.get(i);
			

			if (grid.size() > maxGridLen)
				continue;
			
			List<String> toks = grid.getSequence().getToks();
			
			//get profiles from inverted index
			List<ProfileGrid> profileGridListIndex = invertedIndex.getMatchedGridList(grid, annotType);
			
			System.out.println(profileGridListIndex.size());
			
			Map<String, Boolean> gridMap = new HashMap<String, Boolean>();
			gridMap.put(":" + annotType.toLowerCase(), true);
			for (int j=0; j<grid.size(); j++) {
				List<AnnotationGridElement> col = grid.get(j);
				for (AnnotationGridElement elem : col) {
					String tok = elem.getTok();
					if (tok.startsWith(":relation.")) {
						int index = tok.lastIndexOf("|");
						tok = tok.substring(0, index);
					}
					
					gridMap.put(tok, true);					
				}

			}
			
			//remove extra profiles added during processing
			if (profileGridList.size() > profileSize) {
				for (int j=profileSize; j<profileGridList.size(); j++) {
					ProfileGrid profileGrid = profileGridList.get(j);
					msaProfileMap.remove(profileGrid.getGrid());
					profileGridList.remove(j);
					j--;
				}
			}
			
			int gridTargetStart = -1;
			int gridTargetEnd = -1;
			
			/*
			if (evaluation) {
				int[] targetCoords = grid.getTargetCoords();
				gridTargetStart = targetCoords[0];
				gridTargetEnd = grid.get(targetCoords[0]).get(targetCoords[1]).getEndIndex();
			}
			*/
			
			int[] focusCoords = grid.getSequence().getFocusCoords();
			
			String toksStr = SequenceUtilities.getStrFromToks(toks);
			System.out.println("\n\nsent: " + i + " annotType: " + annotType + " | " + toksStr);
			
			if (pw != null)
				pw.println("\n\nsent: " + i + " annotType: " + annotType + " | " + toksStr);
			
			//if (verbose)
			//System.out.println(grid.toString() + "\n\n");
				

			int profileIndex = 0;
			boolean matched = false; //did any profile match?
			
			//loop through while there are still matches
			//extraction = false - find all profiles that match for gathering filtering statistics (we are training the system)
			//extraction = true - find all segments in the grid that match a profile (previously matched segments are nulled out)
			
			Map<String, Boolean> targetMatchMap = new HashMap<String, Boolean>();
			List<int[]> targetRangeList = new ArrayList<int[]>();

			//iterate if we are extracting and there are new extractions
			int matchCount = 0;
			List<ProfileGrid> profileGridList2 = new ArrayList<ProfileGrid>();
			Map<String, Boolean> profileGridMap2 = new HashMap<String, Boolean>();

			do {
				String gridStr = gson.toJson(grid.getSequence().getToks());
				
				//iterate through each profile
				matchCount = 0;

				for (ProfileGrid profileGridObj : profileGridListIndex) {
					AnnotationSequenceGrid profileGrid = profileGridObj.getGrid();
					List<String> profileToks = profileGrid.getSequence().getToks();
					
					List<String> profileElemToks = new ArrayList<String>();
					for (int j=0; j<profileGrid.size(); j++) {
						List<AnnotationGridElement> col = profileGrid.get(j);
						for (AnnotationGridElement elem : col) {
							String tok = elem.getTok();
							if (tok.startsWith(":relation.")) {
								int index = tok.lastIndexOf("|");
								tok = tok.substring(0, index);
							}
							
							profileElemToks.add(tok);
						}
					}
					

					
					
					
					String profileStr = gson.toJson(profileToks);
						
					
					//System.out.println("profile from inverted: " + profileStr);
					//System.out.println("profileGrid: " + profileGrid.toString());
					
					/*
					if (profileStr.equals("[\":start|start\",\":target\",\":i-org\"]") || 
						profileStr.equals("[\":target\",\":i-org\",\":token|string|(!:token|root|(!:token|category|(!:syntaxtreenode|cat|-lrb-\",\":i-org\",\":token|string|)!:token|root|)!:token|category|)!:syntaxtreenode|cat|-rrb-\"]")) {
						System.out.println("HERE: " + profileGrid.size() + ", " + grid.getActiveSize());
						System.out.println("grid: " + grid.toString());
						System.out.println("profileGrid: " + profileGrid.toString());
					}
					*/
					
					
					
					if ((profileGrid.size() - maxGaps) > grid.getActiveSize())
						continue;
					
					
					
					int found = 0;
					for (String tok : profileElemToks) {
						//System.out.println("profile tok: " + tok);
						if (tok.equals(":target"))
							continue;
						
						Boolean flag = gridMap.get(tok);
						if (flag != null) {
							found++;
						}
					}
					
					
					int minSize = profileElemToks.size() - 1;
					if (minSize > 3)
						minSize -= minSizeOffset;
					
					if (found < (profileElemToks.size() - 1 - minSizeOffset)) {
						//System.out.println("skipped!");
						continue;
					}
					
					//List<AnnotationSequenceGrid> targetProfileGridList = profileGridObj.getTargetGridList();
					
					
					//each profile has its own clone of grid
					//this allows each profile to null out columns without affecting other profiles
					//this do loop loops while the tokens of the profile matched, but the target didn't match
					//we need to perturb both the left and right contexts because the alignment may have been biased based on the gap penalty
					//the gap penalty is set to 0 so the alignment will be biased to the first possible token match
					//however, there may be a better match if the left or right context is forced to shift to the right
						
					
					int[] targetCoords = profileGrid.getTargetCoords();
					int extractStart = -1;
					int extractEnd = -1;
					
					//int[] indexes = MSAUtils.matchProfile(profileGrid, grid, sw, maxGaps, profileGrid.size()-1);
					
					
					Map<String, Object> matchMap = MSAUtils.matchProfile2(profileGrid, grid, sw, maxGaps, syntax, phrase, profileGrid.size()-1-minSizeOffset);

					
					List<int[]> indexesList = (List<int[]>) matchMap.get("indexesList");
					List<List<int[]>> matchCoords1List = (List<List<int[]>>) matchMap.get("matchCoords1List");
					List<List<int[]>> matchCoords2List = (List<List<int[]>>) matchMap.get("matchCoords2List");
					List<List<String>> align1List = (List<List<String>>) matchMap.get("align1List");
					List<List<String>> align2List = (List<List<String>>) matchMap.get("align2List");
					
					//System.out.println("indexesList: " + indexesList.size());
					
					
					//loop through all possible target indexes
					for (int j=0; j<indexesList.size(); j++) {
						int[] indexes = indexesList.get(j);
						
						//System.out.println("indexes[0]: " + indexes[0] + " indexes[1]: " + indexes[1]);
						
						
						//check if the indexes are within the focus coords
						if (indexes[0] >= 0 && indexes[1] >= 1 && indexes[0] < grid.size()) {
							
							//String profileGridStr = gson.toJson(profileGrid.getSequence().getToks());
							
							long annotIndex1 = grid.get(indexes[0]).get(0).getAnnot().getStart();
							long annotIndex2 = grid.get(indexes[1]-1).get(0).getAnnot().getStart();
							
							if (verbose) {
								//System.out.println("\n\nprofile matched: " + profileStr);
								//System.out.println("indexes: " + gson.toJson(indexes) + ", focusCoords: " + gson.toJson(focusCoords));
								//System.out.println("annotIndex1: " + annotIndex1 + ", annotIndex2: " + annotIndex2);
							}
									
							List<int[]> matchCoords1 = matchCoords1List.get(j);
							List<int[]> matchCoords2 = matchCoords2List.get(j);
							
							
							
							boolean targetMatch = false; //did any target profile match?
	
							/*
							if (!(((annotIndex1 >= focusCoords[0] && annotIndex1 <= focusCoords[1]) 
								|| (annotIndex2 >= focusCoords[0] && annotIndex2 <= focusCoords[1])))) {
								
								//profileMatched = true;
	
								int matchStart = matchCoords2.get(0)[0];
								int matchEnd = matchCoords2.get(matchCoords2.size()-1)[0] + 1;
								
								if (extractStart < matchStart && extractStart >= 0)
									matchStart = extractStart;
								if (extractEnd > matchEnd)
									matchEnd = extractEnd;
								
								for (int index = matchStart; index < matchEnd; index++) {
									System.out.println("focus: " + focusCoords[0] + ", " + focusCoords[1] + ", " + annotIndex1 + ", " + annotIndex2 + " nulling");
									grid.nullColumn(index);
								}
								
								break;
	
							}
							*/
		
								
							//List<String> align1 = sw.getAlignment1();
							//List<String> align2 = sw.getAlignment2();
							List<String> align1 = align1List.get(j);
							List<String> align2 = align2List.get(j);
							
							String align1Str = SequenceUtilities.getStrFromToks(align1);
							String align2Str = SequenceUtilities.getStrFromToks(align2);
							//List<int[]> matchCoords1 = sw.getMatchCoords1();
							//List<int[]> matchCoords2 = sw.getMatchCoords2();
							
							
							int profileDir = 0;
							if (targetCoords[0] < matchCoords1.get(0)[0]) {
								profileDir = 1;
							}
							else if (targetCoords[0] > matchCoords1.get(matchCoords1.size()-1)[0])
								profileDir = 2;
							
							
							
							AnnotationSequenceGrid targetGrid = grid.subGrid(indexes[0], indexes[1]);
							
							if (verbose) {
								//System.out.println("match profile: " + align1 + ", " + align2 + ", indexes[0]:" + indexes[0] + ", indexes[1]:" + indexes[1] + ", targetgrid: " + gson.toJson(targetGrid.getSequence().getToks()));
								//System.out.println("matchcoords1: " + gson.toJson(matchCoords1) + "\nmatchCoords2: " + gson.toJson(matchCoords2));
								//System.out.println("targetCoords[0]: " + targetCoords[0] + " targetCoords[1]: " + targetCoords[1]);
							}
		
							
							String targetProfileStr = "";
							int targetIndex = 0;
							List<String> targetProfileStrList = new ArrayList<String>();
							List<String> targetStrList = new ArrayList<String>();
							List<AnnotationSequenceGrid> targetGridList = new ArrayList<AnnotationSequenceGrid>();
							List<int[]> targetMatchIndexesList = new ArrayList<int[]>();
							List<List<int[]>> targetMatchCoords1List = new ArrayList<List<int[]>>();
							List<List<int[]>> targetMatchCoords2List = new ArrayList<List<int[]>>();
							
							
							String targetStr = "";
							int[] targetMatchIndexes = null;
							List<int[]> targetMatchCoords = null;
							
							
							//if (!evaluation)
							Map<AnnotationSequenceGrid, Boolean> targetGridMap = profileGridObj.getTargetGridMap();
							targetProfileGridList = new ArrayList<AnnotationSequenceGrid>();
							
							for (AnnotationSequenceGrid targetGrid2 : targetGridMap.keySet()) {
								boolean flag = targetGridMap.get(targetGrid2);
								if (flag) {
									targetProfileGridList.add(targetGrid2);
									//System.out.println("target added: " + gson.toJson(targetGrid2.getSequence().getToks()));
								}
							}
							
							//each profile has its own copy of targetGrid so nulling out columns won't affect other profiles
							targetGrid = grid.subGrid(indexes[0], indexes[1]);
								
							
							//iterate through each target profile
							for (AnnotationSequenceGrid targetProfileGrid : targetProfileGridList) {		
								targetMatch = MSAUtils.matchGrids(targetProfileGrid, targetGrid, sw, 0, targetProfileGrid.size(), syntax, phrase);
								targetProfileStr = gson.toJson(targetProfileGrid.getSequence().getToks());
								//System.out.println("targetProfile: " + targetProfileStr);
								
								
								//loop through because there may be multiple segments that match target profiles
								boolean targetWasMatched = false;
								while (targetMatch) {
									align1 = sw.getAlignment1();
									align2 = sw.getAlignment2();
									align1Str = SequenceUtilities.getStrFromToks(align1);
									align2Str = SequenceUtilities.getStrFromToks(align2);
									
									//if (gridStr.indexOf("HONG") >= 0) {
									//System.out.println("targetProfile: " + targetProfileStr);
									//}
									
									
									if (align1.size() == 0) {
										break;
									}
									
									
									//check to see if target match meets criteria
									List<int[]> targetMatchCoords1 = sw.getMatchCoords1();
									List<int[]> targetMatchCoords2 = sw.getMatchCoords2();
									
									targetMatchCoords = targetMatchCoords2;
		
									int targetStart = targetMatchCoords2.get(0)[0];
									int[] coords = targetMatchCoords2.get(targetMatchCoords2.size()-1);
									int targetEnd = targetGrid.get(coords[0]).get(coords[1]).getEndIndex();
									int gridEnd = targetGrid.size();
									
									
									extractStart = targetStart + indexes[0];
									extractEnd = targetEnd + indexes[0];
									
									
									
									targetStr = SequenceUtilities.getStrFromToks(targetGrid.getSequence().getToks().subList(targetStart, targetEnd));
	
									
									if (targetCoords[0] == 0)
										targetStart = 0;
									if (targetCoords[0] == profileGrid.size()-1)
										gridEnd = targetEnd;
									
									if (verbose) {
										//System.out.println("extractStart: " + extractStart + ", extractEnd: " + extractEnd);
										//System.out.println("targetStart: " + targetStart + " targetEnd: " + targetEnd);
									}
									
									boolean nullOut = false;
									
									if (extraction && targetMatchMap.get(extractStart + "|" + extractEnd) != null) {
										//System.out.println("Target already matched!");
										
										//null out first token of the match in grid to prevent the same match again
										//grid.nullColumn(matchCoords2.get(0)[0]);
										
										//break;
										//nullOut = true;
									}
									
									
									//check if extracted target is overlapped by existing target
									boolean rangeOverlapped = false;
									for (int[] range : targetRangeList) {
										if (range[0] <= extractStart && range[1] >= extractEnd) {
											rangeOverlapped = true;
											break;
										}
									}
									
									if (extraction && rangeOverlapped) {
										//System.out.println("Target already matched overlapped!");
										//break;
										nullOut = true;
									}
									
									
									if ((profileDir == 0 && (targetStart > maxGaps || (gridEnd - targetEnd) > maxGaps)) ||
											(profileDir == 1 && (gridEnd - targetEnd) > maxGaps) ||
											(profileDir == 2 && targetStart > maxGaps)){
										//System.out.println("too many gaps");
										nullOut = true;
									}
									
									if (nullOut) {
										//null out this occurrence of target
										List<AnnotationGridElement> col = targetGrid.get(coords[0]);
										AnnotationGridElement elem = col.get(coords[1]);
										
										if (profileMatch) {
											for (AnnotationGridElement elem2 : col) {
												elem2.setTok(":null");
											}
										}
										else
											elem.setTok(":null");
									}								
									else {
										targetWasMatched = true;
										
										if (verbose) {
											//System.out.println("targetProfileGrid: " + targetProfileGrid.toString() + "\ntargetGrid: " + targetGrid.toString() + "\ngrid: " + grid.toString());
											//System.out.println("target match :" + profileStr + ", " + targetProfileStr + ", sent: " + i + ", target: " + targetStr + ", " + gson.toJson(targetMatchCoords2));
										}
										
										targetMatchIndexes = new int[2];
										Annotation annot1 = targetGrid.get(targetMatchCoords2.get(0)[0]).get(targetMatchCoords2.get(0)[1]).getAnnot();
										//System.out.println("annot1: " + annot1.toString());
										targetMatchIndexes[0] = (int) targetGrid.get(targetMatchCoords2.get(0)[0]).get(targetMatchCoords2.get(0)[1]).getAnnot().getStart();
										int endIndex = targetMatchCoords2.size()-1;
										Annotation annot2 = targetGrid.get(targetMatchCoords2.get(endIndex)[0]).get(targetMatchCoords2.get(endIndex)[1]).getAnnot();
										//System.out.println("annot2: " + annot2.toString());
										targetMatchIndexes[1] = (int) targetGrid.get(targetMatchCoords2.get(endIndex)[0]).get(targetMatchCoords2.get(endIndex)[1]).getAnnot().getEnd();
										
										if (extraction) {
											targetMatchMap.put(extractStart + "|" + extractEnd, true);
											int[] range = new int[2];
											range[0] = extractStart;
											range[1] = extractEnd;
											targetRangeList.add(range);
											
											//null out this occurrence of target
											List<AnnotationGridElement> col = targetGrid.get(coords[0]);
											AnnotationGridElement elem = col.get(coords[1]);
											
											if (profileMatch) {
												for (AnnotationGridElement elem2 : col) {
													elem2.setTok(":null");
												}
											}
											else
												elem.setTok(":null");
										}
										
										break;
									}
									
									targetMatch = MSAUtils.matchGrids(targetProfileGrid, targetGrid, sw, maxGaps, targetProfileGrid.size(), syntax, phrase);
								}
								
								if (targetWasMatched) {
									targetProfileStrList.add(targetProfileStr);
									targetGridList.add(targetProfileGrid);
									targetStrList.add(targetStr);
									
									targetMatchIndexesList.add(targetMatchIndexes);
									targetMatchCoords1List.add(matchCoords1);
									targetMatchCoords2List.add(matchCoords2);
									
									
									//break out of matching more target profiles since we found one if extraction = true
									if (extraction)
										break;
									
									/*
									else {
										//if we're training, we want to find all target profiles that match in order to perform filtering
										targetProfileStrList.add(targetProfileStr);
										targetGridList.add(targetProfileGrid);
										//break;
									}
									*/
								}
								
								targetIndex++;
							}
	
							//matched target
							if (targetMatch || targetGridList.size() > 0) {
								matched = true;

								MSAProfile profile = msaProfileMap.get(profileGrid);

								
								for (int tIndex=0; tIndex<targetGridList.size(); tIndex++) {
									String targetStr2 = targetStrList.get(tIndex);

									Double targetProb = targetProbMap.get(targetStr2.toLowerCase());
									if (targetProb != null && targetProb < 0.0 && profileStr.indexOf(":" + annotType.toLowerCase()) < 0) {
										System.out.println("Removing low prob value: " + targetStr2);
										pw.println("Removing low prob value: " + targetStr2);
										matched = false;
										continue;
									}
									
									List<MSAProfile> targetList = new ArrayList<MSAProfile>();
									
									/*
									for (AnnotationSequenceGrid targetProfileGrid : targetGridList) {
										MSAProfile target = msaTargetProfileMap.get(targetProfileGrid);
										targetList.add(target);
									}
									*/
									
									MSAProfile target = msaTargetProfileMap.get(targetGridList.get(tIndex));
									targetList.add(target);
									
									/*
									ProfileMatch match = new ProfileMatch(profile, targetList, i, matchCoords1, matchCoords2, targetMatchIndexes,  
											targetStr, toksStr, grid.getSequence());
											*/
									
									
									ProfileMatch match = new ProfileMatch(profile, targetList, i, targetMatchCoords1List.get(tIndex), targetMatchCoords2List.get(tIndex), targetMatchIndexesList.get(tIndex),  
											targetStr2, toksStr, grid.getSequence());
									
									matchList.add(match);
									matchCount++;
									
									if (verbose) {
										System.out.println("MATCHED!: " + annotType + ", " + ", " + gson.toJson(matchCoords1) + ", " + gson.toJson(matchCoords2) + ", " + grid.getSequence().getDocID() + ":" + targetMatchIndexes[0] + ":" + targetMatchIndexes[1]);
										System.out.println("MATCHED PROFILE: " + profile.getProfileID() + "|" + profileStr);
										
										for (int k=0; k<targetList.size(); k++) {
											target = targetList.get(k);
											System.out.println("MATCHED TARGET PROFILE: " + target.getProfileID() + "|" + target.getProfileStr());
											System.out.println("MATCHED TARGET: " + targetStr2);
										}
										
										if (pw != null) {
											pw.println("MATCHED!: " + annotType + ", " + ", " + gson.toJson(matchCoords1) + ", " + gson.toJson(matchCoords2) + ", " + grid.getSequence().getDocID() + ":" + targetMatchIndexes[0] + ":" + targetMatchIndexes[1]);
											pw.println("MATCHED PROFILE: " + profile.getProfileID() + "|" + profileStr);
											
											for (int k=0; k<targetList.size(); k++) {
												target = targetList.get(k);
												pw.println("MATCHED TARGET PROFILE: " + target.getProfileID() + "|" + target.getProfileStr());
												pw.println("MATCHED TARGET: " + targetStr2);
											}
										}
									}
								}
								
								if (extraction && matched) {
									int start = targetMatchCoords.get(0)[0];
									int end = targetMatchCoords.get(targetMatchCoords.size()-1)[0]+1;
									//List<AnnotationGridElement> col = grid.get(indexes[0]);
									List<AnnotationGridElement> col = grid.get(extractStart);
									AnnotationGridElement elem = new AnnotationGridElement(":" + annotType.toLowerCase(), indexes[0] + start, indexes[0] + end, col.size());
									col.add(elem);
					
									//System.out.println(indexes[0] + ", " + extractStart);
									//System.out.println(grid.toString());
									
									//processGrid(profileGrid, grid, matchCoords1, matchCoords2);
									
									
									
									/*
									String profileGridStr2 = gson.toJson(profileGridObj.getGrid().getSequence());

									if (profileGridMap2.get(profileGridStr2) == null) {
										ProfileGrid profileGridObj2 = addProfileGrid(profileGridObj);
									
										if (profileGridObj2 != null) {
											profileGridMap2.put(profileGridStr2, true);
											profileGridList2.add(profileGridObj2);
											MSAProfile profile2 = profile.clone();
											profile2.setProfileStr(profileGridStr2);
											profile2.setProfileToks(profileGridObj.getGrid().getSequence().getToks());
											msaProfileMap.put(profileGridObj2.getGrid(), profile2);
											System.out.println("adding profile: " + profile2.getToks());
										}
									}
									*/
									
									
								}
							}

							//add the matched target as an annotation
							//we don't do this for extraction because there's only one target in each grid by definition
							/*
							if (profileMatched && extraction) {
								int start = targetMatchCoords.get(0)[0];
								int end = targetMatchCoords.get(targetMatchCoords.size()-1)[0]+1;
								List<AnnotationGridElement> col = grid.get(indexes[0]);
								AnnotationGridElement elem = new AnnotationGridElement(":" + annotType.toLowerCase(), indexes[0] + start, indexes[0] + end, col.size());
								col.add(elem);
							}
							*/
							
						}
					}
	
					profileIndex++;
				}
				
				profileGridList.addAll(profileGridList2);
				
				if (extraction)
					profileGridListIndex = invertedIndex.getMatchedGridList(grid, annotType);

				//System.out.println("matchCount: " + matchCount  + " gridListindex: " + profileGridListIndex.size());
			}
			while(extraction && matchCount > 0);
			
			
			if (!matched) {
				ProfileMatch match = new ProfileMatch(null, null, i, null, null, null, "", toksStr, grid.getSequence());
				noMatchList.add(match);
			}
		}
		
		return matchList;
	}
	
	private void processGrid(AnnotationSequenceGrid profileGrid, AnnotationSequenceGrid grid, List<int[]> matchCoords1, List<int[]> matchCoords2)
	{
		for (int i=0; i<matchCoords1.size(); i++) {
			int[] coords1 = matchCoords1.get(i);
			int[] coords2 = matchCoords2.get(i);
			AnnotationGridElement elem1 = profileGrid.get(coords1[0]).get(0);
			
			if (elem1.getTok().startsWith(":lookup")) {
				
				if (elem1.getTok().endsWith("person_first"))
					continue;
				
				List<AnnotationGridElement> col = grid.get(coords2[0]);

				
				for (int j=0; j<col.size(); j++) {
					AnnotationGridElement elem2 = col.get(j);
					String tok = elem2.getTok();
					if (tok.startsWith(":token|orth") || tok.startsWith(":syntaxtreenode")) {
						elem2.setTok("null");
						//System.out.println("PROCESS GRID: " + grid.toString());
					}
				}
			}
		}
	}
	
	private ProfileGrid addProfileGrid(ProfileGrid profile)
	{
		AnnotationSequenceGrid profileGrid = profile.getGrid();
		int i = -1;

		for (i=0; i<profileGrid.size(); i++) {
			List<AnnotationGridElement> col = profileGrid.get(i);
			
			if (col.get(0).getTok().startsWith(":lookup|majortype|person_first")) {
				break;
			}
		}
		
		ProfileGrid profile2 = null;
		
		if (i >= 0 && i < profileGrid.size()) {
			profile2 = new ProfileGrid(profile.getGrid().clone(), profile.getTargetGridMap());
			profile2.getGrid().get(i).get(0).setTok(":token|orth|upperinitial");
			profile2.getGrid().getSequence().getToks().set(i, ":token|orth|upperinitial");
		}
		
		return profile2;
	}
}
