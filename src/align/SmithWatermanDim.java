package align;

import java.util.*;

import msa.Annotation;
import msa.AnnotationSequence;
import nlputils.sequence.SequenceUtilities;

public class SmithWatermanDim
{
	private String tokenAnnotType = "Token";
	private MatrixElement[][] alignMatrix;
	private List<String> featureList;
	private double matchScore = 1.0;
	public static double targetMatchScore = 1000000.0;
	private double mismatchScore = -1.0;
	private double gapPenalty = -0.00001;
	private Map<String, Double> annotTypeScoreMap;
	
	private List<String> align1;
	private List<String> align2;
	private List<String> alignToks1;
	private List<String> alignToks2;
	private List<int[]> matchCoords1;
	private List<int[]> matchCoords2;
	private List<Integer> matchIndexes1;
	private List<Integer> matchIndexes2;
	private List<Integer> alignIndexes1;
	private List<Integer> alignIndexes2;
	private boolean verbose = false;
	private List<Map<String, Boolean>> matchCols;
	
	private boolean profileMatch = false;
	private boolean multiMatch = false;
	
	private Map<Integer, Boolean> targetRelationMap;
	
	private Map<Integer, Integer> relationIndexMap;
	


	public SmithWatermanDim()
	{
	}
	
	public void setTokenAnnotType(String tokenAnnotType)
	{
		this.tokenAnnotType = tokenAnnotType;
	}
	
	public List<String> getAlignment1()
	{
		return align1;
	}
	
	public List<String> getAlignment2()
	{
		return align2;
	}
	
	public List<String> getAlignToks1()
	{
		return alignToks1;
	}
	
	public List<String> getAlignToks2()
	{
		return alignToks2;
	}
	
	public List<Integer> getMatchIndexes1()
	{
		return matchIndexes1;
	}
	
	public List<Integer> getMatchIndexes2()
	{
		return matchIndexes2;
	}
	
	public List<Integer> getAlignIndexes1()
	{
		return alignIndexes1;
	}
	
	public List<Integer> getAlignIndexes2()
	{
		return alignIndexes2;
	}
	
	public List<int[]> getMatchCoords1()
	{
		return matchCoords1;
	}
	
	public List<int[]> getMatchCoords2()
	{
		return matchCoords2;
	}
	
	public void setVerbose(boolean verbose)
	{
		this.verbose = verbose;
	}
	
	public void setGapPenalty(double gapPenalty)
	{
		this.gapPenalty = gapPenalty;
	}
	
	public void setMismatchScore(double mismatchScore)
	{
		this.mismatchScore = mismatchScore;
	}
	
	public void setScoreMap(List<String> annotTypeNameList, List<Double> scoreList)
	{
		annotTypeScoreMap = new HashMap<String, Double>();
		
		for (int i=0; i<annotTypeNameList.size(); i++) {
			String annotType = annotTypeNameList.get(i);
			double score = scoreList.get(i);
			
			/*
			int index = annotType.indexOf("|");
			if (index >= 0)
				annotType = annotType.substring(0, index);
				*/
			
			//Double score = annotTypeScoreMap.get(annotType);
			//if (score == null)
			
			annotTypeScoreMap.put(annotType, score);
			System.out.println("type: " + annotType + ", score: " + score);
		}
	}
	
	public List<Map<String, Boolean>> getMatchCols()
	{
		return matchCols;
	}
	
	public void setProfileMatch(boolean profileMatch)
	{
		this.profileMatch = profileMatch;
	}
	
	public void setMultiMatch(boolean multiMatch)
	{
		this.multiMatch = multiMatch;
	}
	
	public double align(AnnotationSequenceGrid annotSeqGrid1, AnnotationSequenceGrid annotSeqGrid2)
	{
		return align(annotSeqGrid1, annotSeqGrid2, null);
	}
	
	public double align(AnnotationSequenceGrid annotSeqGrid1, AnnotationSequenceGrid annotSeqGrid2, Map<Integer, Integer> relationIndexMap)
	{
		if (annotSeqGrid1.size() == 0 || annotSeqGrid2.size() == 0)
			return -1.0;
		
		if (relationIndexMap == null)
			this.relationIndexMap = new HashMap<Integer, Integer>();
		else
			this.relationIndexMap = relationIndexMap;
		
		//create align matrix
		
		double finalScore = 0.0;
		int height = annotSeqGrid1.get(0).size();

		alignMatrix = new MatrixElement[annotSeqGrid1.size()+1][annotSeqGrid2.size()+1];
		for (int i=0; i<annotSeqGrid1.size()+1; i++) {
			for (int j=0; j<annotSeqGrid2.size()+1; j++) {
				alignMatrix[i][j] = new MatrixElement(height);

				if (i==0 || j == 0) {
					GridScore gridScore = new GridScore(i, j, "", "", 0.0, 0, 0);
					alignMatrix[i][j].addGridScore(gridScore);
					alignMatrix[i][j].addGridPointer(gridScore);
				}
			}
		}
		
		align1 = new ArrayList<String>();
		align2 = new ArrayList<String>();
		matchCoords1 = new ArrayList<int[]>();
		matchCoords2 = new ArrayList<int[]>();
		matchIndexes1 = new ArrayList<Integer>();
		matchIndexes2 = new ArrayList<Integer>();
		alignIndexes1 = new ArrayList<Integer>();
		alignIndexes2 = new ArrayList<Integer>();
		matchCols = new ArrayList<Map<String, Boolean>>();
		
		GridScore maxGridScore = null;
		
		for (int i=1; i<annotSeqGrid1.size()+1; i++) { 
			List<AnnotationGridElement> annotGridCol1 = annotSeqGrid1.get(i-1);
			for (int j=1; j<annotSeqGrid2.size()+1; j++) {
				List<AnnotationGridElement> annotGridCol2 = annotSeqGrid2.get(j-1);

				GridScore gridScoreDiag = getMaxGridScore(alignMatrix[i-1][j-1].getGridPointers());
				
				//System.out.println(i + ", " + j + ", " + gridScoreDiag.getScore() + ", " + gridScoreDiag.getTok1() + ", " + gridScoreDiag.getTok2());

				List<AnnotationGridElement[]> elemMatchList = matchGridColumns2(annotGridCol1, annotGridCol2);
				
				Map<String, GridScore> locScoreMap = new HashMap<String, GridScore>();

				boolean setCurrElem = false;
				for (AnnotationGridElement[] elemAry : elemMatchList) {					
					AnnotationGridElement elem1 = elemAry[0];
					AnnotationGridElement elem2 = elemAry[1];
					
					if (elem1 == null && elem2 == null)
						continue;
					
					/*
					if (elem1 == null) {
						elem1 = new AnnotationGridElement("", i, i+1);
					}
					if (elem2 == null) {
						elem2 = new AnnotationGridElement("", j, j+1);
					}
					*/
					
					String tok1 = elem1.getTok();
					String tok2 = elem2.getTok();
					
					int stepi = elem1.getEndIndex() - elem1.getStartIndex() - 1;
					int stepj = elem2.getEndIndex() - elem2.getStartIndex() - 1;
					int step = stepi;
					if (stepj > stepi)
						step = stepj;
					
					step++;
					
					int i2 = i + stepi;
					int j2 = j + stepj;
					
					if (stepi == 0 && stepj == 0)
						setCurrElem = true;

					//match
					//System.out.println("match: " + i2 + ", " + j2 + ", " + tok1 + ", " + tok2);
					String annotType = tok1;
					
					int index = tok1.indexOf("|");
					if (index >= 0) {
						int index2 = tok1.indexOf("|", index+1);
						
						if (index2 >= 0)
							annotType = tok1.substring(0, index2);
						else
							annotType = tok1.substring(0, index);
					}
						

					Double annotScore = annotTypeScoreMap.get(annotType);
					if (annotScore == null)
						System.out.println("annotType: " + annotType + ", tok: " + tok1);
					double score = matchScore * annotTypeScoreMap.get(annotType);
					//System.out.println("score: " + score);
					if (score < 0.0)
						continue;
					
					if (tok1.startsWith(":target"))
						score = targetMatchScore;
					
					GridScore gridScore = new GridScore(i, j, tok1, tok2, elem1.getRow(), elem2.getRow());
					gridScore.setScore((score * step) + gridScoreDiag.getScore());
					//gridScore.setScore(score + gridScoreDiag.getScore());
					
					if (multiMatch) {
						//System.out.println("col: " + tok1 + ", " + tok2);
						gridScore.addTok(tok1);
					}
				
					GridScore gridScore2 = locScoreMap.get(i2 + "|" + j2);
					if (gridScore2 == null) {
						locScoreMap.put(i2 + "|" + j2, gridScore);
					}
					else if (gridScore.getScore() > gridScore2.getScore()) {
						gridScore2.setScore(gridScore.getScore());
						//gridScore2.setAnnotIndex(k);
						gridScore2.setTok1(tok1);
						gridScore2.setTok2(tok2);
						
						if (multiMatch) {
							//System.out.println("matched: " + tok1 + ", " + tok2);
							gridScore2.addTok(tok1);
						}
					}
					else {
						if (multiMatch) {
							//System.out.println("matched2: " + tok1 + ", " + tok2);
							gridScore2.addTok(tok1);
						}
					}
				}
				
				if (elemMatchList.size() == 0 || !setCurrElem) {
					
					GridScore gridScore = new GridScore(i, j, "", "", 0.0, 0, 0);
					locScoreMap.put(i + "|" + j, gridScore);
					//alignMatrix[i][j].addGridScore(gridScore);
					//alignMatrix[i][j].addGridPointer(gridScore);
					
					
					
				}
				
				GridScore gridScoreDim1 = getMaxGridScore(alignMatrix[i-1][j].getGridPointers());
				//gridScoreDim1.setScore(gridScoreDim1.getScore() - gapPenalty);
				GridScore gridScoreDim2 = getMaxGridScore(alignMatrix[i][j-1].getGridPointers());
				//gridScoreDim2.setScore(gridScoreDim2.getScore() - gapPenalty);
				
				for (String key : locScoreMap.keySet())	{
					String[] parts = key.split("\\|");
					int i2 = Integer.parseInt(parts[0]);
					int j2 = Integer.parseInt(parts[1]);
					
					
					GridScore gridPointer = locScoreMap.get(key);
					
					if (profileMatch) {
						int hiValue = 0;
						int hiValue2 = 0;
						int row = gridPointer.getRow();
						List<AnnotationGridElement> col = annotSeqGrid1.get(row-1);
						Map<Double, Boolean> map = new HashMap<Double, Boolean>();
						double hiScore = 0.0;
						for (AnnotationGridElement elem2 : col) {
							Double score = annotTypeScoreMap.get(elem2.getTok());
							//if (elem2.getTok().equals(":negated-finding") || elem2.getTok().equals(":metamap-finding") || elem2.getTok().equals(":negated-finding-g")) {
							if (score != null && score >= 100.0 && score > hiScore) {
								//if (map.get(score) == null) {
								//	map.put(score, true);
									hiValue++;
									hiScore = score;
								//}
							}
						}
							
						//map = new HashMap<String, Boolean>();
						//int colNum = gridPointer.getCol();
						List<String> tokList = gridPointer.getTokList();
						//List<AnnotationGridElement> col2 = annotSeqGrid2.get(colNum-1);
						for (String tok : tokList) {
						//for (AnnotationGridElement elem2 : col2) {
							Double score = annotTypeScoreMap.get(tok);
							//System.out.println("tok: " + tok + " score: " + score);
							//if (elem2.getTok().equals(":negated-finding") || elem2.getTok().equals(":metamap-finding") || elem2.getTok().equals(":negated-finding-g")) {
							if (score != null && score == hiScore) {
								//if (map.get(tok) != null) {
									//map.put(elem2.getTok(), true);
									hiValue2++;
								//}
							}
						}
						
						//System.out.println("hiScore: " + hiScore + " hiValue:" + hiValue + " hiValue2:" + hiValue2);
						
						if (gridPointer.getTokList().size() < col.size()) {
							if ((hiValue > 0 && hiValue2 == 0) || hiValue == 0)
								gridPointer = new GridScore(i, j, "", "", 0.0, 0, 0);
						}
					}
					
					String tok1 = gridPointer.getTok1();
					String tok2 = gridPointer.getTok2();
					int annotIndex1 = gridPointer.getAnnotIndex1();
					int annotIndex2 = gridPointer.getAnnotIndex2();
					
					int dir = 1;
					double hiScore = gridScoreDim1.getScore() - gapPenalty;
					
					if (gridScoreDim2.getScore() - gapPenalty > hiScore) {
						hiScore = gridScoreDim2.getScore() - gapPenalty;
						dir = 2;
					}
					if (gridPointer.getScore() > hiScore) {
						hiScore = gridPointer.getScore();
						dir = 3;
					}
					
					GridScore highGridScore = null;
					

					if (dir == 1) {
						if (verbose)
							System.out.println("dir=1, setting: " + i2 + ", " + j + " score: " + hiScore + ", tok1=" + tok1 + ", tok2=" + tok2 + ",index1=" + annotIndex1 + ", index2=" + annotIndex2);
						highGridScore = new GridScore(i2, j, tok1, tok2, annotIndex1, annotIndex2);
						//highGridScore.setTokMulti(gridPointer.getTokMulti());
						alignMatrix[i2][j].addGridPointer(highGridScore);
						highGridScore.setNext(gridScoreDim1);
					}
					else if (dir == 2) {
						if (verbose)
							System.out.println("dir=2, setting: " + i + ", " + j2 + " score: " + hiScore + ", tok1=" + tok1 + ", tok2=" + tok2 + ",index1=" + annotIndex1 + ", index2=" + annotIndex2);
						highGridScore = new GridScore(i, j2, tok1, tok2, annotIndex1, annotIndex2);
						//highGridScore.setTokMulti(gridPointer.getTokMulti());
						alignMatrix[i][j2].addGridPointer(highGridScore);
						highGridScore.setNext(gridScoreDim2);
					}
					else {
						highGridScore = gridPointer;
						highGridScore.setNext(gridScoreDiag);

						alignMatrix[i2][j2].addGridPointer(highGridScore);
						if (verbose)
							System.out.println("dir=3, setting: " + i2 + ", " + j2 + " score: " + hiScore + ", tok1=" + tok1 + ", tok2=" + tok2 + ",index1=" + annotIndex1 + ", index2=" + annotIndex2);
					}
					
					highGridScore.setDirection(dir);
					highGridScore.setScore(hiScore);
					alignMatrix[i][j].addGridScore(highGridScore);
					
					if (verbose) {
						System.out.println("i=" + i + ", j=" + j + ", dir=" + dir + ", i2=" + i2 + ", j2=" + j2 
							+ ", tok1=" + tok1 + ", tok2=" + tok2 + ", score=" + highGridScore.getScore());
						System.out.println("dim1=" + gridScoreDim1.getScore() + ", dim2=" + gridScoreDim2.getScore() 
							+ ", match=" + gridScoreDiag.getScore());
					}					

					
					if (maxGridScore == null || hiScore > maxGridScore.getScore()) {
						if (verbose) {
							if (maxGridScore != null)
								System.out.println("highScore: " + hiScore + ", i=" + highGridScore.getRow() + ", j=" + highGridScore.getCol() + ", old score=" + maxGridScore.getScore());
						}
						maxGridScore = new GridScore(highGridScore);
						if (multiMatch)
							maxGridScore.setTokMulti(highGridScore.getTokMulti());
						if (verbose)
							System.out.println("highScore: " + maxGridScore.getScore() + "\n");
					}
				}
			}
		}
		
		//back track
		alignToks1 = new ArrayList<String>();
		alignToks2 = new ArrayList<String>();
		GridScore currGridScore = maxGridScore;
				
		while (currGridScore != null && currGridScore.getScore() > 0.0) {
			int direction = currGridScore.getDirection();
			if (direction == 1) {
				align1.add(0, currGridScore.getTok1());
				align2.add(0, "|||");
				if (multiMatch)
					alignToks1.add(0, currGridScore.getTokMulti());
				else
					alignToks1.add(0, currGridScore.getTok1());
			}
			else if (direction == 2) {
				align1.add(0, "|||");
				align2.add(0, currGridScore.getTok2());
				
				if (multiMatch)
					alignToks2.add(0, currGridScore.getTokMulti());
				else
					alignToks2.add(0, currGridScore.getTok2());
			}
			else {
				align1.add(0, currGridScore.getTok1());
				align2.add(0, currGridScore.getTok2());
				
				if (multiMatch) {
					alignToks1.add(0, currGridScore.getTokMulti());
					alignToks2.add(0, currGridScore.getTokMulti());
				}
				else {
					alignToks1.add(0, currGridScore.getTok1());
					alignToks2.add(0, currGridScore.getTok2());
				}
				
				matchIndexes1.add(0, alignToks1.size()-1);
				matchIndexes2.add(0, alignToks2.size()-1);
				alignIndexes1.add(0, align1.size()-1);
				alignIndexes2.add(0, align2.size()-1);
				
				int[] match1 = new int[2];
				match1[0] = currGridScore.getRow()-1;
				match1[1] = currGridScore.getAnnotIndex1();
				
				int[] match2 = new int[2];
				match2[0] = currGridScore.getCol()-1;
				match2[1] = currGridScore.getAnnotIndex2();
				
				matchCoords1.add(0, match1);
				matchCoords2.add(0, match2);
				
				finalScore += currGridScore.getScore();
			}
				
			currGridScore = currGridScore.getNext();
		}
		
		for (int i=0; i<matchIndexes1.size(); i++) {
			int index = matchIndexes1.get(i);
			int index2 = alignIndexes1.get(i);
			matchIndexes1.set(i, alignToks1.size()-1-index);
			alignIndexes1.set(i,  align1.size()-1-index2);
		}
		
		for (int i=0; i<matchIndexes2.size(); i++) {
			int index = matchIndexes2.get(i);
			int index2 = alignIndexes2.get(i);
			matchIndexes2.set(i, alignToks2.size()-1-index);
			alignIndexes2.set(i,  align2.size()-1-index2);
		}
		
		return finalScore;
	}
	
	public List<Double> matchGridColumns(List<AnnotationGridElement> annotGridCol1, List<AnnotationGridElement> annotGridCol2)
	{
		List<Double> scoreList = new ArrayList<Double>();
		for (int i=0; i<annotGridCol1.size(); i++) {
			AnnotationGridElement elem1 = annotGridCol1.get(i);
			AnnotationGridElement elem2 = annotGridCol2.get(i);
			
			if (elem1 != null && elem2 != null) {
			
				String tok1 = elem1.getTok();
				String tok2 = elem2.getTok();
				
				if (tok1.equals(tok2) && !tok1.equals(":null")) {
					double score = getScore(tok1, tok2);
					scoreList.add(score);
					
					if (verbose)
						System.out.println("matched: " + tok1 + ", score=" + score);
				}
				else
					scoreList.add(0.0);
			}
			else
				scoreList.add(0.0);
		}
		
		return scoreList;
	}
	
	private List<AnnotationGridElement[]> matchGridColumns2(List<AnnotationGridElement> annotGridCol1, List<AnnotationGridElement> annotGridCol2)
	{
		List<AnnotationGridElement[]> elemMatchList = new ArrayList<AnnotationGridElement[]>();
		
		Map<String, List<AnnotationGridElement>> elemMap = new HashMap<String, List<AnnotationGridElement>>();
		
		//Map<Integer, Integer> relationIndexMap = new HashMap<Integer, Integer>();
		
		for (AnnotationGridElement elem : annotGridCol2) {
			
			String tok = elem.getTok();
				
			//if (elem.getAnnot().getAnnotationType().startsWith("Relation.")) {
			if (tok.indexOf(":relation.") >= 0) {
				int index = tok.lastIndexOf("|");
				tok = tok.substring(0, index);
			}
			
			List<AnnotationGridElement> elemList = elemMap.get(tok);
			if (elemList == null) {
				elemList = new ArrayList<AnnotationGridElement>();
				elemMap.put(tok, elemList);
			}
			
			elemList.add(elem);
		}
		
		for (AnnotationGridElement elem : annotGridCol1) {

			String tok = elem.getTok();
			int relationIndex1 = 0;
			
			//if (elem.getAnnot().getAnnotationType().startsWith("Relation.")) {
			if (tok.indexOf(":relation.") >= 0) {
				int index = tok.lastIndexOf("|");
				relationIndex1 = Integer.parseInt(tok.substring(index+1));
				tok = tok.substring(0, index);
			}
			
			List<AnnotationGridElement> elemList = elemMap.get(tok);
			if (elemList != null) {
				for (AnnotationGridElement elem2 : elemList) {	
					String tok2 = elem2.getTok();
					int relationIndex2 = -1;
					
					//if (elem2.getAnnot().getAnnotationType().startsWith("Relation.")) {
					if (tok.indexOf(":relation.") >= 0) {
						int index = tok2.lastIndexOf("|");
						relationIndex2 = Integer.parseInt(tok2.substring(index+1));
						tok2 = tok2.substring(0, index);
					}
					
					if (relationIndex1 != 0) {
						Integer indexMapping = relationIndexMap.get(relationIndex1);
						if (indexMapping == null) {
							relationIndexMap.put(relationIndex1, relationIndex2);
						}
						else if (relationIndex2 != indexMapping) {
							continue;
						}
					}
					
					AnnotationGridElement[] elemAry = new AnnotationGridElement[2];
					elemAry[0] = elem;
					elemAry[1] = elem2;
					elemMatchList.add(elemAry);
					
					if (verbose)
					//if (elem.getAnnot().getAnnotationType().startsWith("Relation."))
						System.out.println("matched: " + elem.getTok());
				}
			}
		}
		
		return elemMatchList;
	}
	
	private GridScore getMaxGridScore(List<GridScore> gridScoreList)
	{
		GridScore maxGridScore = null;
		for (GridScore gridScore : gridScoreList) {
			if (gridScore == null)
				continue;
			
			double score = gridScore.getScore();
			
			/*
			if (gridScore.getTok1().length() > 0)
				score += 1.0;
			if (gridScore.getTok2().length() > 0)
				score += 1.0;
				*/
			
			if (maxGridScore == null || score > maxGridScore.getScore()) {
				maxGridScore = gridScore;
			}
		}
		
		return maxGridScore;
	}
	
	private double getScore(String tok1, String tok2)
	{
		if (tok1.equals(tok2)) {
			if (tok1.startsWith(":target"))
				return targetMatchScore;
			
			return matchScore;
		}
		
		return mismatchScore;
	}
	
	/*
	private List<List<AnnotationGridElement>> getSequenceGrid(AnnotationSequence seq)
	{
		List<List<AnnotationGridElement>> annotSeqGrid = new ArrayList<List<AnnotationGridElement>>();

		Map<String, List<Annotation>> annotMap = seq.getAnnotMap();
		List<Annotation> tokAnnotList = annotMap.get(tokenAnnotType);
		
		for (int i=0; i<tokAnnotList.size(); i++) {
			Annotation tokAnnot = tokAnnotList.get(i);
			List<AnnotationGridElement> gridCol = new ArrayList<AnnotationGridElement>();
			gridCol.add(new AnnotationGridElement("", i, i));
			annotSeqGrid.add(gridCol);
		}
		
		//insert other annot types
		for (String annotType : annotMap.keySet()) {
			if (annotType.equals(tokenAnnotType))
				continue;
			
			List<Annotation> annotList = annotMap.get(annotType);
			for (int i=0; i<annotList.size(); i++) {
				Annotation annot = annotList.get(i);
				
				for (int j=0; j<annotSeqGrid.size(); j++) {
					List<AnnotationGridElement> gridCol = annotSeqGrid.get(j);
					AnnotationGridElement tokAnnotGridElem = gridCol.get(0);
					Annotation tokAnnot = tokAnnotGridElem.getAnnot();
					if (annot.getStart() == tokAnnot.getStart()) {
						int endIndex = getEndIndex(tokAnnotList, annot, j);
						gridCol.add(j, new AnnotationGridElement("", j, endIndex));
						break;
					}
				}
			}
		}
		
		return annotSeqGrid;
	}
	*/
	
	
	private int getEndIndex(List<Annotation> tokAnnotList, Annotation annot, int startIndex)
	{
		for (int i=startIndex+1; i<tokAnnotList.size(); i++) {
			Annotation tokAnnot = tokAnnotList.get(i);
			if (tokAnnot.getEnd() == annot.getEnd()) {
				return i;
			}
		}
		
		return -1;
	}
	
	public static void main(String[] args)
	{
		AnnotationSequenceGrid grid1 = new AnnotationSequenceGrid();
		AnnotationSequenceGrid grid2 = new AnnotationSequenceGrid();
		
		/*
		List<AnnotationGridElement> col = new ArrayList<AnnotationGridElement>();
		AnnotationGridElement gridElem = new AnnotationGridElement("left", 0, 1);
		col.add(gridElem);
		gridElem = new AnnotationGridElement(":target", 0, 3);
		col.add(gridElem);
		grid1.add(col);
		
		col = new ArrayList<AnnotationGridElement>();
		gridElem = new AnnotationGridElement("lower", 1, 2);
		col.add(gridElem);
		col.add(null);
		grid1.add(col);
		
		col = new ArrayList<AnnotationGridElement>();
		gridElem = new AnnotationGridElement("lobe", 2, 3);
		col.add(gridElem);
		col.add(null);
		grid1.add(col);
		
		col = new ArrayList<AnnotationGridElement>();
		gridElem = new AnnotationGridElement("of", 3, 4);
		col.add(gridElem);
		col.add(null);
		grid1.add(col);
		
		col = new ArrayList<AnnotationGridElement>();
		gridElem = new AnnotationGridElement("lung", 5, 6);
		col.add(gridElem);
		gridElem = new AnnotationGridElement("metamap:disease", 5, 7);
		col.add(gridElem);
		grid1.add(col);
		
		col = new ArrayList<AnnotationGridElement>();
		gridElem = new AnnotationGridElement("stem", 6, 7);
		col.add(gridElem);
		col.add(null);
		grid1.add(col);
		
		col = new ArrayList<AnnotationGridElement>();
		gridElem = new AnnotationGridElement("nodule", 7, 8);
		col.add(gridElem);
		col.add(null);
		grid1.add(col);
		
		
		
		col = new ArrayList<AnnotationGridElement>();
		gridElem = new AnnotationGridElement("RLL", 0, 1);
		col.add(gridElem);
		gridElem = new AnnotationGridElement("metamap:anatomy", 0, 1);
		col.add(gridElem);
		grid2.add(col);
		
		col = new ArrayList<AnnotationGridElement>();
		gridElem = new AnnotationGridElement("of", 1, 2);
		col.add(gridElem);
		col.add(null);
		grid2.add(col);
		
		col = new ArrayList<AnnotationGridElement>();
		gridElem = new AnnotationGridElement("heart", 2, 3);
		col.add(gridElem);
		gridElem = new AnnotationGridElement(":target", 2, 4);
		col.add(gridElem);
		grid2.add(col);
		
		col = new ArrayList<AnnotationGridElement>();
		gridElem = new AnnotationGridElement("valve", 3, 4);
		col.add(gridElem);
		col.add(null);
		grid2.add(col);
		
		col = new ArrayList<AnnotationGridElement>();
		gridElem = new AnnotationGridElement("nodule", 4, 5);
		col.add(gridElem);
		col.add(null);
		grid2.add(col);
		*/
		
		
		/*
		List<AnnotationGridElement> col = new ArrayList<AnnotationGridElement>();
		AnnotationGridElement gridElem = new AnnotationGridElement("the", 0, 1);
		col.add(gridElem);
		col.add(null);
		grid1.add(col);
		
		
		col = new ArrayList<AnnotationGridElement>();
		gridElem = new AnnotationGridElement("lower", 1, 2);
		col.add(gridElem);
		gridElem = new AnnotationGridElement("metamap:anatomy", 1, 3);
		col.add(gridElem);
		grid1.add(col);
		
		col = new ArrayList<AnnotationGridElement>();
		gridElem = new AnnotationGridElement("lobe", 2, 3);
		col.add(gridElem);
		col.add(null);
		grid1.add(col);
		
		
		col = new ArrayList<AnnotationGridElement>();
		gridElem = new AnnotationGridElement("lung", 3, 4);
		col.add(gridElem);
		col.add(null);
		grid1.add(col);
		

		
		col = new ArrayList<AnnotationGridElement>();
		gridElem = new AnnotationGridElement("the", 0, 1);
		col.add(gridElem);
		col.add(null);
		grid2.add(col);
		
		col = new ArrayList<AnnotationGridElement>();
		gridElem = new AnnotationGridElement("upper", 1, 2);
		col.add(gridElem);
		gridElem = new AnnotationGridElement("metamap:finding", 1, 3);
		col.add(gridElem);
		grid2.add(col);
		
		col = new ArrayList<AnnotationGridElement>();
		gridElem = new AnnotationGridElement("bound", 2, 3);
		col.add(gridElem);
		col.add(null);
		grid2.add(col);
		
		col = new ArrayList<AnnotationGridElement>();
		gridElem = new AnnotationGridElement("lung", 3, 4);
		col.add(gridElem);
		col.add(null);
		grid2.add(col);
		*/
		
		
		
		SmithWatermanDim sw = new SmithWatermanDim();
		sw.setVerbose(true);
		sw.align(grid1, grid2);
		
		List<String> align1 = sw.getAlignment1();
		List<String> align2 = sw.getAlignment2();
		
		System.out.println(SequenceUtilities.getStrFromToks(align1));
		System.out.println(SequenceUtilities.getStrFromToks(align2));
	}
}
