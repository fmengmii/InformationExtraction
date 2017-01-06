package align;

import java.util.*;
import msa.*;

public class GenAnnotationGrid
{
	private List<String> annotTypeNameList;
	private String tokType;
	
	public GenAnnotationGrid()
	{
	}
	
	public GenAnnotationGrid(List<String> annotTypeNameList, String tokType)
	{
		this.annotTypeNameList = annotTypeNameList;
		this.tokType = tokType;
	}
	
	public void setAnnotTypeNameList(List<String> annotTypeNameList)
	{
		this.annotTypeNameList = annotTypeNameList;
	}
	
	public void setTokType(String tokType)
	{
		this.tokType = tokType;
	}
	
	public AnnotationSequenceGrid toAnnotSeqGrid(List<String> toks, boolean startEnd)
	{
		AnnotationSequence seq = new AnnotationSequence();
		List<String> toks2 = new ArrayList<String>();
		for (String tok : toks)
			toks2.add(tok);
		
		seq.setToks(toks2);
		AnnotationSequenceGrid grid = new AnnotationSequenceGrid(seq);
		
		int tokIndex = 0;
		if (startEnd) {
			toks2.add(0, ":start|start");
			toks2.add(":end|end");
		}
		for (String tok : toks2) {
			
			/*
			String annotType = tok;
			int annotTypeIndex = -1;
			int index = tok.lastIndexOf("|");
			if (index >= 0) {
				annotType = tok.substring(0, index);
				annotTypeIndex = getAnnotTypeIndex(annotType);
			}
			
			if (annotTypeIndex == -1) {
				annotTypeIndex = getAnnotTypeIndex(tok);
			}
			
			if (annotTypeIndex >= 0) {
				List<AnnotationGridElement> col = new ArrayList<AnnotationGridElement>();
				*/
			
				/*
				for (int i=0; i<annotTypeNameList.size(); i++)
					col.add(null);
					*/
			
			List<AnnotationGridElement> col = new ArrayList<AnnotationGridElement>();
			
			String[] parts = tok.split("\\!");
			//System.out.println("tok: " + tok);
			
			for (int i=0; i<parts.length; i++) {
				//System.out.println("parts: " + parts[i]);
				AnnotationGridElement elem = new AnnotationGridElement(parts[i], tokIndex, tokIndex+1, 0);
				col.add(elem);
			}
			
			grid.add(col);
			
			if (tok.equals(":target")) {
				int[] targetCoords = new int[2];
				targetCoords[0] = grid.size()-1;
				targetCoords[1] = col.size()-1;
				grid.setTargetCoords(targetCoords);
				}
				/*
			}
			else
				System.out.println("ToAnnotSeqGrid: annot type index not found: " + annotType);
				*/
		
			tokIndex++;
		}
		
		//System.out.println(grid.toString());
		
		//toks.remove(0);
		//toks.remove(toks.size()-1);
		
		postProcess(grid, "Lookup");
		
		return grid;
	}
	
	private int getAnnotTypeIndex(String annotType)
	{
		int index = -1;
		for (int i=0; i<annotTypeNameList.size(); i++) {
			if (annotType.equals(annotTypeNameList.get(i))) {
				index = i;
				break;
			}
		}
		
		return index;
	}
	
	public List<AnnotationSequenceGrid> toAnnotSeqGrid(AnnotationSequence seq, boolean requireTarget, boolean exposeTarget, boolean exposeTargetLabel, boolean startEnd)
	{
		List<AnnotationSequenceGrid> gridList = new ArrayList<AnnotationSequenceGrid>();
		
		AnnotationSequenceGrid seqGrid = new AnnotationSequenceGrid(seq);
		Map<String, List<Annotation>> annotMap = seq.getAnnotMap();
		
		List<Annotation> tokAnnotList = null;
		
		List<int[]> targetIndexList = new ArrayList<int[]>();
		List<int[]> targetCoordList = new ArrayList<int[]>();
		List<Annotation> targetAnnotList = new ArrayList<Annotation>();
		List<String> targetStrList = new ArrayList<String>();
		
		List<int[]> targetIndexList2 = new ArrayList<int[]>();
		List<int[]> targetCoordList2 = new ArrayList<int[]>();
		List<Annotation> targetAnnotList2 = new ArrayList<Annotation>();
		List<String> targetStrList2 = new ArrayList<String>();
		
		int targetIndex = -1;
		
		for (int i=0; i<annotTypeNameList.size(); i++) {
			String annotType = annotTypeNameList.get(i);
			
			//System.out.println("annotType: " + annotType);
			
			
			List<Annotation> annotList = annotMap.get(annotType);
			if (annotList == null) {
				continue;
			}
				
				/*
				for (int j=0; j<seqGrid.size(); j++) {
					List<AnnotationGridElement> col = seqGrid.get(j);
					col.add(null);
				}
				
				continue;
			}
			*/
			
			if (annotType.equals(tokType)) {
				tokAnnotList = annotList;
				
				if (startEnd) {
					Annotation annotFirst = annotList.get(0);
					long start = annotFirst.getStart()-1;
					Annotation annot = new Annotation(-1, ":start|start", start, start+1, ":start|start", null);
					annotList.add(0, annot);
					Annotation annotLast = annotList.get(annotList.size()-1);
					start = annotLast.getEnd();
					annot = new Annotation(-1, ":end|end", start, start+1, ":end|end", null);
					annotList.add(annot);
					
					seq.getToks().add(0, ":start|start");
					seq.getToks().add(":end|end");
				}
			}
			
			int index = 0;
			List<AnnotationGridElement> col = null;
			for (Annotation annot : annotList) {
				
				int[] indexes = null;
				int startIndex = index;
				int endIndex = index+1;
				String label = null;
				
				if (annotType.equals(tokType)) {
					col = new ArrayList<AnnotationGridElement>();
					seqGrid.add(col);
				}
				else {
					//System.out.println("docid: " + annot.getDocID() + ", annot start: " + annot.getStart() + ", annot end: " + annot.getEnd() + ", annot label: " + annot.getLabel());
					indexes = getIndexes(annot, tokAnnotList);
					startIndex = indexes[0];
					endIndex = indexes[1];
					col = seqGrid.get(startIndex);
					
					/*
					//multiple annotations with same start index - overlapping annotations
					if (startIndex == index-1) {
						//System.out.println("overlap: startIndex=" + startIndex + ", endIndex=" + endIndex + ", annotType=" + annotType);

						List<AnnotationGridElement> lastCol = seqGrid.get(index-1);
						AnnotationGridElement lastElem = lastCol.get(lastCol.size()-1);
						
						label = lastElem.getTok() + "|" + annot.getLabel();
						lastElem.setTok(lastElem.getTok() + "|" + annot.getLabel());
						if (endIndex > lastElem.getEndIndex())
							lastElem.setEndIndex(endIndex);
						
					}
					else {
						col = seqGrid.get(startIndex);
						
						for (int j=index; j<startIndex; j++) {
							seqGrid.get(j).add(null);
						}
						
					}
					
					index = startIndex;
					*/
				}
				
//				if (label == null) {
					
				if (annotType.equals(":target")) {
					
					//System.out.println("docid: " + annot.getDocID() + ", annot start: " + annot.getStart() + ", annot end: " + annot.getEnd() + ", annot label: " + annot.getLabel());
					
					/*
					if (targetIndex == -1)
						targetIndex = i;
						*/
					
					targetIndexList.add(indexes);
					targetAnnotList.add(annot);
					//System.out.println("adding target: " + annot.getAnnotationType() + ", " + annot.getValue() + ", " + startIndex + ", " + endIndex);
					//col.add(null);
					
					int[] targetCoords = new int[2];
					targetCoords[0] = startIndex;
					targetCoords[1] = col.size();
					targetCoordList.add(targetCoords);
					targetStrList.add(annotType);
					
					if (exposeTargetLabel) {
						label = ":" + annot.getAnnotationType().toLowerCase();					
						AnnotationGridElement gridElem = new AnnotationGridElement(annot, label, startIndex, endIndex, col.size());
						col.add(gridElem);
					}
					
				}
				else if (annotType.equals(":target2")) {					
					targetIndexList2.add(indexes);
					targetAnnotList2.add(annot);
					System.out.println("adding target2: " + annot.getAnnotationType() + ", " + annot.getValue() + ", " + startIndex + ", " + endIndex);
					//col.add(null);
					
					int[] targetCoords = new int[2];
					targetCoords[0] = startIndex;
					targetCoords[1] = col.size();
					targetCoordList2.add(targetCoords);
					targetStrList2.add(annotType);
				}
				else {
				
					label = annot.getLabel();
					if (label == null)
						label = annot.getValue();
				
					AnnotationGridElement gridElem = new AnnotationGridElement(annot, label, startIndex, endIndex, col.size());
					col.add(gridElem);
					
					//System.out.println("annotType=" + annotType + ", tok=" + label + ", startIndex=" + startIndex + ", endIndex=" + endIndex + ", start=" + annot.getStart() 
					//+ ", end=" + annot.getEnd());
				}
//			}
			
				index++;
			}
			
			/*
			for (int j=index; j<seqGrid.size(); j++)
				seqGrid.get(j).add(null);
				*/
		}
		
		//there was no second target
		if (targetIndexList2.size() == 0)
			targetIndexList2.add(null);
		
		if (requireTarget) {
			for (int i=0; i<targetIndexList.size(); i++) {
				int[] indexes = targetIndexList.get(i);
				//AnnotationSequenceGrid seqGrid2 = seqGrid.clone();
				String annotType = targetStrList.get(i);
								
				Annotation annot = targetAnnotList.get(i);
				//List<AnnotationGridElement> col = seqGrid2.get(indexes[0]);
				//AnnotationGridElement gridElem = new AnnotationGridElement(annot, annot.getLabel(), indexes[0], indexes[1], col.size());
				//AnnotationGridElement gridElem = new AnnotationGridElement(annot, annotType, indexes[0], indexes[1], col.size());
				//col.add(gridElem);
				
				//if (!exposeTarget)
				//	gridElem.setTok(":null");
				
				//seqGrid2.setTargetCoords(targetCoordList.get(i));
				//gridList.add(seqGrid2);
				
				for (int j=0; j<targetIndexList2.size(); j++) {
					
					//:target
					AnnotationSequenceGrid seqGrid2 = seqGrid.clone();
					seqGrid2.setTargetCoords(targetCoordList.get(i));
					List<AnnotationGridElement> col = seqGrid2.get(indexes[0]);
					
					if (!exposeTargetLabel) {
						AnnotationGridElement gridElem = new AnnotationGridElement(annot, annotType, indexes[0], indexes[1], col.size());
						col.add(gridElem);

						if (!exposeTarget)
							gridElem.setTok(":null");
					}
					else {
						col.get(col.size()-1).setTok(":target");
					}
					
					//:target2
					int[] indexes2 = targetIndexList2.get(j);
					if (indexes2 != null) {
						seqGrid2.setTargetCoords2(targetCoordList2.get(j));
						String annotType2 = targetStrList2.get(j);
						Annotation annot2 = targetAnnotList2.get(j);
						List<AnnotationGridElement> col2 = seqGrid2.get(indexes[0]);
						AnnotationGridElement gridElem2 = new AnnotationGridElement(annot2, annotType2, indexes2[0], indexes2[1], col2.size());
						col2.add(gridElem2);
						
						if (!exposeTarget)
							gridElem2.setTok(":null");
					}
					
					postProcess(seqGrid2, ":lookup");
					gridList.add(seqGrid2);
					//System.out.println("SeqGrid2: " + seqGrid2.toString());
				}				
			}
		}
		else {
			postProcess(seqGrid, ":lookup");
			gridList.add(seqGrid);
			//System.out.println("SeqGrid: " + seqGrid.toString());
		}
		
		//tokAnnotList.remove(0);
		//tokAnnotList.remove(tokAnnotList.size()-1);
		
		return gridList;
	}
	
	private int[] getIndexes(Annotation annot, List<Annotation> tokAnnotList)
	{
		int[] ret = new int[2];
		ret[0] = -1;
		ret[1] = -1;
		
		long annotStart = annot.getStart();
		long annotEnd = annot.getEnd();
		long lastEnd = -1;
		for (int i=0; i<tokAnnotList.size(); i++) {
			Annotation annot2 = tokAnnotList.get(i);
			long start = annot2.getStart();
			long end = annot2.getEnd();
			
			if (i==0)
				start = 0;
			if (i == tokAnnotList.size()-1)
				end = Long.MAX_VALUE;
				
			if (annotStart >= start && annotStart <= end || (annotStart > lastEnd && annotStart < start))
				ret[0] = i;
			if (annotEnd > start && annotEnd <= end) {
				ret[1] = i+1;
				break;
			}
			
			if (annotEnd > lastEnd && annotEnd <= start) {
				ret[1] = i;
				break;
			}
			
			lastEnd = end;
		}
		
		if (ret[1] == -1 && annotEnd > lastEnd) {
			ret[1] = tokAnnotList.size();
		}
		
		if (ret[0] == -1 || ret[1] == -1) {
			for (Annotation annot2 : tokAnnotList) {
				System.out.println("tok=" + annot2.getValue() + ", annotType=" + annot2.getAnnotationType() + ", start=" + annot2.getStart() + ", end=" + annot2.getEnd());
			}
			
			System.out.println("tok=" + annot.getValue() + ", annotType=" + annot.getAnnotationType() + ", start=" + annot.getStart() + ", end=" + annot.getEnd());
		}
		
		return ret;
	}
	
	public void postProcess(AnnotationSequenceGrid grid, String annotType)
	{
		for (int i=0; i<grid.size(); i++) {
			List<AnnotationGridElement> col = grid.get(i);
			
			for (int j=0; j<col.size(); j++) {
				AnnotationGridElement elem = col.get(j);
				String tok = elem.getTok();
				int start = elem.getStartIndex();
				int end = elem.getEndIndex();
				Annotation annot = elem.getAnnot();
				
				if (tok.startsWith(annotType) && (end-start > 1)) {
					//System.out.println("adding: " + tok + ", " + start + ", " + end);
					for (int k=start; k<end; k++) {
						List<AnnotationGridElement> col2 = grid.get(k);
						Annotation tokAnnot = col2.get(0).getAnnot();
						
						Annotation annot2 = new Annotation(-1, annot.getAnnotationType(), tokAnnot.getStart(), tokAnnot.getEnd(), annot.getValue(), annot.getFeatureMap());
						AnnotationGridElement elem2 = new AnnotationGridElement(annot2, tok, k, k+1, col2.size()-1);
						col2.add(col2.size()-1, elem2);
						if (k == i)
							j++;
					}
				}
			}
		}
		
	}
}
