package msa;

import java.sql.Connection;
import java.util.*;

import com.google.gson.Gson;

import align.AnnotationGridElement;
import align.AnnotationSequenceGrid;
import align.SmithWatermanDim;
import msa.db.MSADBException;
import msa.db.MSADBInterface;
import nlputils.sequence.SequenceUtilities;

public class MSAUtils
{
	public static int countGaps(List<String> toks, String gapStr)
	{
		int count = 0;
		for (String tok : toks) {
			if (tok.equals(gapStr))
				count++;
		}
		
		return count;
	}
	
	public static int countPhrase(List<String> toks)
	{
		int count = 0;
		for (String tok : toks) {
			if (tok.startsWith(":syntaxtreenode"))
				count++;
		}
		
		return count;
	}
	
	public static int countSyntax(List<String> toks)
	{
		int count = 0;
		for (String tok : toks) {
			String[] parts = tok.split("\\!");
			boolean isSyntax = true;
			for (int i=0; i<parts.length; i++) {
				if (!(parts[i].startsWith(":token|category") || parts[i].startsWith(":token|orth") || parts[i].startsWith(":syntaxtreenode")))
					isSyntax = false;
			}

			if (isSyntax)
				count++;
		}
		
		return count;
	}
	
	public static List<String> removeGaps(List<String> toks, String gapStr)
	{
		List<String> toks2 = new ArrayList<String>();
		for (String tok : toks) {
			if (!tok.equals(gapStr))
				toks2.add(tok);
		}
		
		return toks2;
	}
	
	public static String getAnnotType(String docNamespace, String docTable, long docID, long start, long end, String provenance, MSADBInterface db)
	{
		try {
			List<Annotation> annotList = db.getAnnotations(docNamespace, docTable, docID, start, end, null, provenance);
			if (annotList.size() > 0) {
				return annotList.get(0).getAnnotationType();
			}
			
			System.out.println("get docID: " + docID + " annot: start: " + start + " end: " + end + " provenance: " + provenance);
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		
		return null;
	}
	
	public static List<String> getGoldAnnotationTypes(MSADBInterface db, List<AnnotationSequenceGrid> gridList, String docNamespace, String docTable, String annotType, String provenance) throws MSADBException
	{
		List<String> goldAnnotTypeList = new ArrayList<String>();
		for (AnnotationSequenceGrid grid : gridList) {
			int[] targetCoords = grid.getTargetCoords();
			List<AnnotationGridElement> col = grid.get(targetCoords[0]);
			AnnotationGridElement elem = col.get(targetCoords[1]);
			Annotation targetAnnot = grid.get(targetCoords[0]).get(targetCoords[1]).getAnnot();
			List<Annotation> annotList = db.getAnnotations(docNamespace, docTable, grid.getSequence().getDocID(), targetAnnot.getStart(), targetAnnot.getEnd(), null, provenance);
			if (annotList.size() > 0) {
				goldAnnotTypeList.add(annotList.get(0).getAnnotationType());
			}
			else {
				goldAnnotTypeList.add(null);
				System.out.println("not found: " + grid.getSequence().getDocID() + ", " + grid.getSequence().getStart());
			}
			
		}
		
		return goldAnnotTypeList;
	}
	
	public static List<String> getGoldAnnotationTypes2(MSADBInterface db, List<AnnotationSentence2> sentList, String docNamespace, String docTable, String provenance) throws MSADBException
	{
		List<String> goldAnnotTypeList = new ArrayList<String>();
		for (AnnotationSentence2 sent : sentList) {
			int targetIndex = sent.getSequence().getTargetIndexList().get(sent.getSentIndex());
			Annotation targetAnnot = sent.getSequence().getAnnotList().get(targetIndex);
			List<Annotation> annotList = db.getAnnotations(docNamespace, docTable, sent.getSequence().getDocID(), targetAnnot.getStart(), targetAnnot.getEnd(), null, provenance);
			if (annotList.size() > 0) {
				goldAnnotTypeList.add(annotList.get(0).getAnnotationType());
			}
			else {
				goldAnnotTypeList.add(null);
				System.out.println("not found: " + sent.getSequence().getDocID() + ", " + sent.getSequence().getStart());
			}
			
		}
		
		return goldAnnotTypeList;
	}
	
	public static List<String> getAnnotationTypeNameList(List<Map<String, Object>> annotFilterList, String tokType)
	{
		List<String> annotTypeNameList = new ArrayList<String>();
		
		for (Map<String, Object> map : annotFilterList) {
			String annotType = (String) map.get("annotType");
			List<String> features = (List<String>) map.get("features");
			
			if (features == null) {
				annotTypeNameList.add(":target");
				continue;
			}

			
			for (String feature : features) {
				String featureName = ":" + annotType;
				if (!feature.equals("$annotTypeName")) {
					featureName += "|" + feature;
				}
				
				featureName = featureName.toLowerCase();
				if (featureName.equals(tokType))
					annotTypeNameList.add(0, featureName);
				else
					annotTypeNameList.add(featureName);
			}
		}
		
		return annotTypeNameList;
	}
	
	public static boolean matchGrids(AnnotationSequenceGrid grid1, AnnotationSequenceGrid grid2, SmithWatermanDim sw, int maxGaps, int minSize, int syntax, int phrase)
	{
		if (grid1.size() == 0 || grid2.size() == 0)
			return false;
		
		Gson gson = new Gson();
		
		sw.align(grid1, grid2);
		List<Integer> matchIndexes1 = sw.getMatchIndexes1();
		List<Integer> matchIndexes2 = sw.getMatchIndexes2();
		List<int[]> matchCoords1 = sw.getMatchCoords1();
		List<int[]> matchCoords2 = sw.getMatchCoords2();
		List<String> align1 = sw.getAlignment1();
		List<String> align2 = sw.getAlignment2();
		List<Integer> alignIndexes1 = sw.getAlignIndexes1();
		List<Integer> alignIndexes2 = sw.getAlignIndexes2();
		
		int gaps1 = countGaps(align1, "|||");
		int gaps2 = countGaps(align2, "|||");
		int syntax1 = countSyntax(align1);
		int syntax2 = countSyntax(align2);
		int phrase1 = countPhrase(align1);
		int phrase2 = countPhrase(align2);
		
		if (gaps1 <= maxGaps && gaps2 <= maxGaps && syntax1 <= syntax && syntax2 <= syntax && phrase1 <= phrase && phrase2 <= phrase && 
			matchCoords1.size() >= minSize && matchCoords2.size() >= minSize) {
			
			/*
			System.out.println("\nmatch grids\ngrid1:\n" + grid1.toString());
			System.out.println("grid2:\n" + grid2.toString());
			System.out.println("align1: " + gson.toJson(align1));
			System.out.println("align2: " + gson.toJson(align2));
			System.out.println("matchindexes1: " + gson.toJson(matchIndexes1));
			System.out.println("matchindexes2: " + gson.toJson(matchIndexes2));
			System.out.println("alignindexes1: " + gson.toJson(alignIndexes1));
			System.out.println("alignindexes2: " + gson.toJson(alignIndexes2));
			System.out.println("coords1: " + gson.toJson(matchCoords1));
			System.out.println("coords2: " + gson.toJson(matchCoords2));
			*/
			
			
			return true;
		}
		
		return false;
	}
	
	public static Map<String, Object> matchProfile2(AnnotationSequenceGrid profileGrid, AnnotationSequenceGrid grid, SmithWatermanDim sw, int maxGaps, int syntax, int phrase, int minSize)
	{
		
		Map<String, Object> map = new HashMap<String, Object>();
		
		List<int[]> indexList = new ArrayList<int[]>();
		List<List<int[]>> matchCoords1List1 = new ArrayList<List<int[]>>();
		List<List<int[]>> matchCoords2List1 = new ArrayList<List<int[]>>();
		List<List<String>> align1List1 = new ArrayList<List<String>>();
		List<List<String>> align2List1 = new ArrayList<List<String>>();
		
		int[] targetCoords = profileGrid.getTargetCoords();
		AnnotationSequenceGrid left = profileGrid.subGrid(0, targetCoords[0]);
		AnnotationSequenceGrid right = profileGrid.subGrid(targetCoords[0]+1, profileGrid.size());
		
		int gridSize = profileGrid.size()-1;
		
		Gson gson = new Gson();
		
		String profileStr = gson.toJson(profileGrid.getSequence().getToks());

		
		
		//match the left
		AnnotationSequenceGrid grid2 = grid.clone();
		boolean match = false;
		List<Integer> targetStartList = new ArrayList<Integer>();
		boolean startMatch = false;
		
		int syntax1 = 0;
		int syntax2 = 0;
		int phrase1 = 0;
		int phrase2 = 0;
		
		if (left.size() > 0) {
			do {
				match = true;
				
				double score = sw.align(left, grid2);
				
				List<Integer> matchIndexes1 = sw.getMatchIndexes1();
				List<Integer> matchIndexes2 = sw.getMatchIndexes2();
				List<int[]> matchCoords1 = sw.getMatchCoords1();
				List<int[]> matchCoords2 = sw.getMatchCoords2();
				List<String> align1 = sw.getAlignment1();
				List<String> align2 = sw.getAlignment2();
				List<Integer> alignIndexes1 = sw.getAlignIndexes1();
				List<Integer> alignIndexes2 = sw.getAlignIndexes2();
				
				int gaps1 = countGaps(align1, "|||");
				int gaps2 = countGaps(align2, "|||");
				syntax1 = countSyntax(align1);
				syntax2 = countSyntax(align2);
				phrase1 = countPhrase(align1);
				phrase2 = countPhrase(align2);
				
				
				
				/*
				//if (profileStr.equals("[\":i-per\",\":target\",\":token|string|(\",\":lookup|majortype|location\",\":token|string|)\",\":number|number\"]")) {
				//if (profileStr.equals("[\":start|start\",\":target\",\":token|category|cd!:number|number!:syntaxtreenode|cat|cd\",\":number|number\",\":token|category|cd!:number|number\"]")) {
				//if (gaps1 == 0 && gaps2 == 0) {
					System.out.println("profile: " + profileStr);
					System.out.println("grid2: " + grid2.toString());
					System.out.println("left: " + left.toString());
					System.out.println("align1: " + gson.toJson(align1));
					System.out.println("align2: " + gson.toJson(align2));
					System.out.println("matchindexes1: " + gson.toJson(matchIndexes1));
					System.out.println("matchindexes2: " + gson.toJson(matchIndexes2));
					System.out.println("coords1: " + gson.toJson(matchCoords1));
					System.out.println("coords2: " + gson.toJson(matchCoords2));
					System.out.println("gaps1: " + gaps1 + ", gaps2: " + gaps2 + ", syntax1: " + syntax1 + ", syntax2: " + syntax2 + "\n\n");
				//}
				*/
				 
				
					
				
				
				if (gaps1 <= maxGaps && gaps2 <= maxGaps && syntax1 <= syntax && syntax2 <= syntax && phrase1 <= phrase && phrase2 <= phrase && 
					matchCoords1.size() > 0) {
					match = true;
					startMatch = true;
					int[] coords = matchCoords2.get(matchCoords2.size()-1);
					AnnotationGridElement elem = grid.get(coords[0]).get(coords[1]);
					targetStartList.add(elem.getEndIndex());
					
					matchCoords1List1.add(matchCoords1);
					matchCoords2List1.add(matchCoords2);
					
					align1List1.add(align1);
					align2List1.add(align2);
				}
				
				if (matchCoords2.size() > 0)
					grid2.nullColumn(matchCoords2.get(0)[0]);
				else
					match = false;
			}
		while (match);
		}
		
		if (targetStartList.size() == 0) {
			targetStartList.add(0);
			matchCoords1List1.add(new ArrayList<int[]>());
			matchCoords2List1.add(new ArrayList<int[]>());
			align1List1.add(new ArrayList<String>());
			align2List1.add(new ArrayList<String>());
		}
		
		
		List<Integer> targetEndList = new ArrayList<Integer>();
		boolean endMatch = false;
		
		List<List<int[]>> matchCoords1List2 = new ArrayList<List<int[]>>();
		List<List<int[]>> matchCoords2List2 = new ArrayList<List<int[]>>();
		List<List<String>> align1List2 = new ArrayList<List<String>>();
		List<List<String>> align2List2 = new ArrayList<List<String>>();

		if (right.size() > 0) {
	
			//match the right
			grid2 = grid.clone();
			do {
				match = true;
				
				double score = sw.align(right, grid2);
				
				List<Integer> matchIndexes1 = sw.getMatchIndexes1();
				List<Integer> matchIndexes2 = sw.getMatchIndexes2();
				List<int[]> matchCoords1 = sw.getMatchCoords1();
				List<int[]> matchCoords2 = sw.getMatchCoords2();
				
				for (int i=0; i<matchCoords1.size(); i++) {
					int[] coords = matchCoords1.get(i);
					coords[0] += left.size() + 1;
				}
				
				List<String> align1 = sw.getAlignment1();
				List<String> align2 = sw.getAlignment2();
				List<Integer> alignIndexes1 = sw.getAlignIndexes1();
				List<Integer> alignIndexes2 = sw.getAlignIndexes2();
				
				int gaps1 = countGaps(align1, "|||");
				int gaps2 = countGaps(align2, "|||");
				syntax1 += countSyntax(align1);
				syntax2 += countSyntax(align2);
				phrase1 += countPhrase(align1);
				phrase2 += countPhrase(align2);
				
				int targetStart = 0;
				int targetEnd = 0;
				
				
				
				/*
				//if (profileStr.equals("[\":i-per\",\":target\",\":token|string|(\",\":lookup|majortype|location\",\":token|string|)\",\":number|number\"]")) {
				//if (profileStr.equals("[\":token|orth|lowercase\",\":token|orth|lowercase\",\":lookup|majortype|jobtitle\",\":target\",\":i-per\"]")) {
				//	System.out.println("HERE");
				//if (profileStr.equals("[\":start|start\",\":target\",\":token|category|cd!:number|number!:syntaxtreenode|cat|cd\",\":number|number\",\":token|category|cd!:number|number\"]")) {
				//if (gaps1 == 0 && gaps2 == 0) {
					System.out.println("profile: " + profileStr);
					System.out.println("grid2: " + grid2.toString());
					System.out.println("right: " + right.toString());
				System.out.println("right align1: " + gson.toJson(align1));
				System.out.println("right align2: " + gson.toJson(align2));
				System.out.println("right matchindexes1: " + gson.toJson(matchIndexes1));
				System.out.println("right matchindexes2: " + gson.toJson(matchIndexes2));
				System.out.println("right coords1: " + gson.toJson(matchCoords1));
				System.out.println("right coords2: " + gson.toJson(matchCoords2));
				System.out.println("gaps1: " + gaps1 + ", gaps2: " + gaps2 + ", syntax1: " + syntax1 + ", syntax2: " + syntax2 + "\n\n");
				//}
				*/
				
				

				
				if (gaps1 <= maxGaps && gaps2 <= maxGaps && syntax1 <= syntax && syntax2 <= syntax && phrase1 <= phrase && phrase2 <= phrase && 
					matchCoords1.size() > 0) {
					endMatch = true;
					targetEndList.add(matchCoords2.get(0)[0]);
					
					matchCoords1List2.add(matchCoords1);
					matchCoords2List2.add(matchCoords2);
					
					align1List2.add(align1);
					align2List2.add(align2);
				}			
				
				if (matchCoords2.size() > 0)
					grid2.nullColumn(matchCoords2.get(0)[0]);
				else
					match = false;
			}
			while (match);
		}
		
		List<List<int[]>> matchCoords1List = new ArrayList<List<int[]>>();
		List<List<int[]>> matchCoords2List = new ArrayList<List<int[]>>();
		List<List<String>> align1List = new ArrayList<List<String>>();
		List<List<String>> align2List = new ArrayList<List<String>>();
		
		if (!startMatch && !endMatch) {
			int[] indexes = new int[2];
			indexes[0] = -1;
			indexes[1] = -1;
			indexList.add(indexes);
			
			matchCoords1List.add(new ArrayList<int[]>());
			matchCoords2List.add(new ArrayList<int[]>());
			align1List.add(new ArrayList<String>());
			align2List.add(new ArrayList<String>());
		}
		else {
		
			if (targetEndList.size() == 0) {
				targetEndList.add(grid2.size());
				matchCoords1List2.add(new ArrayList<int[]>());
				matchCoords2List2.add(new ArrayList<int[]>());
				align1List2.add(new ArrayList<String>());
				align2List2.add(new ArrayList<String>());
			}
			
			for (int i=0; i<targetStartList.size(); i++) {
				int start = targetStartList.get(i);
				
				for (int j=0; j<targetEndList.size(); j++) {
					int end = targetEndList.get(j);
	
					if (end > start) {
						int[] indexes = new int[2];
						indexes[0] = start;
						indexes[1] = end;
						
						
						List<int[]> matchCoords1 = new ArrayList<int[]>();
						matchCoords1.addAll(matchCoords1List1.get(i));
						matchCoords1.addAll(matchCoords1List2.get(j));
						
						List<int[]> matchCoords2 = new ArrayList<int[]>();
						matchCoords2.addAll(matchCoords2List1.get(i));
						matchCoords2.addAll(matchCoords2List2.get(j));
						
						if (matchCoords1.size() != gridSize || matchCoords1.size() < minSize || matchCoords2.size() < minSize)
							continue;

						indexList.add(indexes);
						matchCoords1List.add(matchCoords1);
						matchCoords2List.add(matchCoords2);
						
						List<String> align1 = new ArrayList<String>();
						align1.addAll(align1List1.get(i));
						align1.addAll(align1List2.get(j));
						align1List.add(align1);
						
						List<String> align2 = new ArrayList<String>();
						align2.addAll(align2List1.get(i));
						align2.addAll(align2List2.get(j));
						align2List.add(align2);
					}
				}
			}
		}
		
		map.put("indexesList", indexList);
		map.put("matchCoords1List", matchCoords1List);
		map.put("matchCoords2List", matchCoords2List);
		map.put("align1List", align1List);
		map.put("align2List", align2List);
		
		return map;
	}
	
	public static int[] matchProfile(AnnotationSequenceGrid grid1, AnnotationSequenceGrid grid2, SmithWatermanDim sw, int maxGaps, int syntax, int phrase, int minSize)
	{
		int[] indexes = new int[2];
		indexes[0] = -1;
		indexes[1] = -1;
		
		if (grid1.size() == 0 || grid2.size() == 0)
			return indexes;
		
		Gson gson = new Gson();
		
		int[] targetCoords = grid1.getTargetCoords();
		//Annotation targetAnnot = grid1.get(targetCoords[0]).get(targetCoords[1]).getAnnot();
		int targetIndex = targetCoords[0];
		
		//System.out.println("\n\ngrid1:\n" + grid1.toString());
		//System.out.println("grid2:\n" + grid2.toString());
		
		double score = sw.align(grid1, grid2);
		
		List<Integer> matchIndexes1 = sw.getMatchIndexes1();
		List<Integer> matchIndexes2 = sw.getMatchIndexes2();
		List<int[]> matchCoords1 = sw.getMatchCoords1();
		List<int[]> matchCoords2 = sw.getMatchCoords2();
		List<String> align1 = sw.getAlignment1();
		List<String> align2 = sw.getAlignment2();
		List<Integer> alignIndexes1 = sw.getAlignIndexes1();
		List<Integer> alignIndexes2 = sw.getAlignIndexes2();
		
		int gaps2 = countGaps(align2, "|||");
		int syntax2 = countSyntax(align2);
		int phrase2 = countPhrase(align2);
		
		int gaps1 = 0;
		int syntax1 = 0;
		int phrase1 = 0;
		int targetStart = 0;
		int targetEnd = 0;
		
		
		
		/*
		System.out.println("align1: " + gson.toJson(align1));
		System.out.println("align2: " + gson.toJson(align2));
		System.out.println("matchindexes1: " + gson.toJson(matchIndexes1));
		System.out.println("matchindexes2: " + gson.toJson(matchIndexes2));
		System.out.println("coords1: " + gson.toJson(matchCoords1));
		System.out.println("coords2: " + gson.toJson(matchCoords2));
		 */		
		
		 	
		
		int gridSize = grid1.size()-1;
		
		if (matchIndexes1.size() == gridSize) {
			
			
			//System.out.println("\nmatch profile\ngrid1:\n" + grid1.toString());
			//System.out.println("grid2:\n" + grid2.toString());
			/*
			System.out.println("align1: " + gson.toJson(align1));
			System.out.println("align2: " + gson.toJson(align2));
			System.out.println("matchindexes1: " + gson.toJson(matchIndexes1));
			System.out.println("matchindexes2: " + gson.toJson(matchIndexes2));
			System.out.println("alignindexes1: " + gson.toJson(alignIndexes1));
			System.out.println("alignindexes2: " + gson.toJson(alignIndexes2));
			System.out.println("coords1: " + gson.toJson(matchCoords1));
			System.out.println("coords2: " + gson.toJson(matchCoords2));
			*/
			
		
			if (targetIndex > 0 && targetIndex < grid1.size()-1) {
				int start1 = 0;
				
				int targetCoordIndex = getTargetCoordIndex(matchCoords1, targetIndex);
				
				int end1 = alignIndexes1.get(targetCoordIndex)+1;
				
				int start2 = alignIndexes1.get(targetCoordIndex+1);
				int end2 = align1.size();
				
				int[] coords2 = matchCoords2.get(targetCoordIndex);
				targetStart = grid2.get(coords2[0]).get(coords2[1]).getEndIndex();
				targetEnd = matchCoords2.get(targetCoordIndex+1)[0];

				
				int[] gapAry = countGapsProfile(align1, start1, end1, start2, end2);
				gaps1 = gapAry[0];
				syntax1 = gapAry[1];
			}
			else {
				gaps1 = countGaps(align1, "|||");
				syntax1 = countSyntax(align1);
				phrase1 = countPhrase(align1);
				
				if (targetIndex == 0) {
					targetStart = 0;
					int matchIndex = 0;
					
					targetEnd = matchCoords2.get(matchIndex)[0];
				}
				else {					
					targetStart = matchCoords2.get(matchCoords2.size()-1)[0] + 1;
					targetEnd = grid2.size();
				}
			}
			
			if (gaps1 <= maxGaps && gaps2 <= maxGaps && syntax1 <= syntax && syntax2 <= syntax && phrase1 <= phrase && phrase2 <= phrase && 
				matchCoords1.size() >= minSize && matchCoords2.size() >= minSize) {
				
				indexes[0] = targetStart;
				indexes[1] = targetEnd;
			}
		}
		
		
		return indexes;
	}
	
	public static int[] countGapsProfile(List<String> align, int start1, int end1, int start2, int end2)
	{		
		int[] gapAry = new int[2];
		List<String> left = align.subList(start1, end1);
		List<String> right = align.subList(start2, end2);
		
		int gaps1 = MSAUtils.countGaps(left, "|||");
		int gaps2 = MSAUtils.countGaps(right, "|||");
		int syntax1 = countSyntax(left);
		int syntax2 = countSyntax(right);
		int phrase1 = countPhrase(left);
		int phrase2 = countPhrase(right);
		
		gapAry[0] = gaps1 + gaps2;
		gapAry[1] = syntax1 + syntax2;
		
		return gapAry;
	}
	
	public static int getTargetCoordIndex(List<int[]> matchCoords, int targetIndex)
	{
		int targetCoordIndex = -1;
		for (int i=0; i<matchCoords.size(); i++) {
			if (matchCoords.get(i)[0] >= targetIndex) {
				targetCoordIndex = i-1;
				break;
			}
		}
		
		if (targetCoordIndex == -1 && targetIndex >= matchCoords.get(matchCoords.size()-1)[0]) {
			targetCoordIndex = matchCoords.size()-1;
		}
		
		return targetCoordIndex;
	}
	
	public static void nullTargets(List<AnnotationSequenceGrid> gridList)
	{
		for (AnnotationSequenceGrid grid : gridList) {
			int[] targetCoords = grid.getTargetCoords();
			
			if (targetCoords != null) {
				grid.get(targetCoords[0]).get(targetCoords[1]).setTok(":null");
			}
		}
	}
	
	public static int[] matchAnswer(Map<String, String> ansMap, long docID, int start, int end)
	{
		String ansType = ansMap.get(docID + "|" + start + "|" + end);
		if (ansType == null) {
			ansType = ansMap.get(docID + "|" + start+1 + "|" + end+1);
			if (ansType != null) {
				start++;
				end++;
			}
		}
			
		if (ansType == null) {
			ansType = ansMap.get(docID + "|" + (start-1) + "|" + (end-1));
			if (ansType != null) {
				start--;
				end--;
			}
		}
		
		if (ansType == null) {
			ansType = ansMap.get(docID + "|" + (start-1) + "|" + (end));
			if (ansType != null) {
				start--;
			}
		}
		
		if (ansType == null) {
			ansType = ansMap.get(docID + "|" + (start) + "|" + (end-1));
			if (ansType != null) {
				end--;
			}
		}
		
		if (ansType == null) {
			ansType = ansMap.get(docID + "|" + (start+1) + "|" + (end));
			if (ansType != null) {
				start++;
			}
		}
		
		if (ansType == null) {
			ansType = ansMap.get(docID + "|" + (start) + "|" + (end+1));
			if (ansType != null) {
				end++;
			}
		}
		
		
		int[] indexes = new int[2];
		indexes[0] = -1;
		indexes[1] = -1;
		
		if (ansType != null) {
			indexes[0] = start;
			indexes[1] = end;
		}
		
		return indexes;
	}
	
	public static List<Long> getDocIDList(Connection docDBConn, String docDBQuery) throws Exception
	{
		java.sql.Statement stmt = docDBConn.createStatement();
		java.sql.ResultSet rs = stmt.executeQuery(docDBQuery);
		
		List<Long> docIDList = new ArrayList<Long>();
		
		while (rs.next()) {
			docIDList.add(rs.getLong(1));
		}
		
		stmt.close();
		
		return docIDList;
	}
	
	public static List<AnnotationSequenceGrid> getTargetGridList(List<AnnotationSequenceGrid> gridList)
	{
		List<AnnotationSequenceGrid> targetGridList = new ArrayList<AnnotationSequenceGrid>();
		for (AnnotationSequenceGrid grid : gridList) {
			int[] targetCoords = grid.getTargetCoords();
			AnnotationGridElement elem = grid.get(targetCoords[0]).get(targetCoords[1]);
			int start = elem.getStartIndex();
			int end = elem.getEndIndex();
			
			//System.out.println("Grid: " + grid.toString());
			
			
			AnnotationSequenceGrid targetGrid = grid.subGrid(start, end);
			targetGrid.removeElement(targetCoords[0]-start, targetGrid.get(targetCoords[0]-start).size()-1);
			targetGridList.add(targetGrid);
		}
		
		return targetGridList;
	}
}
