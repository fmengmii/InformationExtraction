package align;

import java.util.*;

import msa.Annotation;
import msa.AnnotationSequence;

public class AnnotationSequenceGrid
{
	private List<List<AnnotationGridElement>> gridElementList;
	private AnnotationSequence sequence;
	private int[] targetCoords;
	private int[] targetCoords2;
	private int[] focusCoords;
	private int size = 0;

	public AnnotationSequenceGrid()
	{
		gridElementList = new ArrayList<List<AnnotationGridElement>>();
	}
	
	public AnnotationSequenceGrid(AnnotationSequence sequence)
	{
		gridElementList = new ArrayList<List<AnnotationGridElement>>();
		this.sequence = sequence;
	}
	
	public AnnotationSequenceGrid(AnnotationSequence sequence, List<List<AnnotationGridElement>> gridElementList)
	{
		this(sequence);
		this.gridElementList = gridElementList;
		size = gridElementList.size();
	}
	
	public AnnotationSequence getSequence()
	{
		return sequence;
	}
	
	public void add(List<AnnotationGridElement> col)
	{
		gridElementList.add(col);
		size++;
	}
	
	public List<AnnotationGridElement> get(int index)
	{
		return gridElementList.get(index);
	}
	
	public int size()
	{
		return gridElementList.size();
		//return size;
	}
	
	public int getActiveSize()
	{
		return size;
	}
	
	public int[] getTargetCoords() {
		return targetCoords;
	}

	public void setTargetCoords(int[] targetCoords) {
		this.targetCoords = targetCoords;
	}
	
	public int[] getFocusCoords() {
		return focusCoords;
	}

	public void setFocusCoords(int[] focusCoords) {
		this.focusCoords = focusCoords;
	}
	
	public void removeRow(int row)
	{
		for (List<AnnotationGridElement> col : gridElementList) {
			for (int i=row+1; i<col.size(); i++) {
				AnnotationGridElement elem = col.get(i);
				elem.setRow(elem.getRow()-1);
			}
			
			col.remove(row);
		}
	}
	
	public void removeElement(int col, int row)
	{
		List<AnnotationGridElement> col2 = gridElementList.get(col);
		for (int i=row+1; i<col2.size(); i++) {
			AnnotationGridElement elem = col2.get(i);
			elem.setRow(elem.getRow()-1);
		}
		
		col2.remove(row);
	}
	
	public void setSequence(AnnotationSequence sequence)
	{
		this.sequence = sequence;
	}
	
	public void setColumn(int index, List<AnnotationGridElement> col)
	{
		gridElementList.set(index, col);
	}
	
	public void nullColumn(int index)
	{
		List<AnnotationGridElement> col2 = new ArrayList<AnnotationGridElement>();
		AnnotationGridElement elem = gridElementList.get(index).get(0);
		elem.setTok(":null");
		col2.add(elem);
		gridElementList.set(index, col2);
		//size--;
	}
	
	public AnnotationSequenceGrid clone()
	{
		AnnotationSequence sequence2 = sequence.clone();
		
		AnnotationSequenceGrid grid = new AnnotationSequenceGrid(sequence2);
		grid.setTargetCoords(targetCoords);
		for (List<AnnotationGridElement> col : gridElementList) {
			List<AnnotationGridElement> col2 = new ArrayList<AnnotationGridElement>();
			
			for (AnnotationGridElement gridElem : col) {
				if (gridElem == null)
					col2.add(null);
				else {
					//AnnotationGridElement gridElem2 = new AnnotationGridElement(gridElem.getAnnot(), gridElem.getTok(), gridElem.getStartIndex(), gridElem.getEndIndex());
					AnnotationGridElement gridElem2 = gridElem.clone();
					col2.add(gridElem2);
				}
			}
			
			grid.add(col2);
		}
		
		return grid;
	}
	
	public AnnotationSequenceGrid subGrid(int start, int end)
	{
		if (start > end)
			return new AnnotationSequenceGrid();
		
		List<String> toks2 = sequence.getToks().subList(start, end);
		AnnotationSequence sequence2 = new AnnotationSequence();
		sequence2.setToks(toks2);
		sequence2.setDocID(sequence.getDocID());
		List<List<AnnotationGridElement>> gridElementList2 = new ArrayList<List<AnnotationGridElement>>();
		for (int i=start; i<end; i++) {
			List<AnnotationGridElement> col = gridElementList.get(i);
			List<AnnotationGridElement> col2 = new ArrayList<AnnotationGridElement>();
			
			for (AnnotationGridElement elem : col) {
				AnnotationGridElement elem2 = null;
				if (elem != null)
					elem2 = elem.clone();
				col2.add(elem2);
			}
			
			gridElementList2.add(col2);
		}
		
		//adjust indexes and filter out annotations that cross over the end index
		for (List<AnnotationGridElement> col : gridElementList2) {
			for (int i=0; i<col.size(); i++) {
				AnnotationGridElement elem = col.get(i);
				if (elem == null)
					continue;
				
				int endIndex = elem.getEndIndex();
				/*
				if (endIndex > end) {
					col.set(i, null);
				}
				*/
				
				if (endIndex <= end) {
					elem.setStartIndex(elem.getStartIndex() - start);
					elem.setEndIndex(endIndex - start);
				}
				else {
					for (int j=i+1; j<col.size(); j++) {
						AnnotationGridElement elem2 = col.get(j);
						elem2.setRow(elem2.getRow()-1);
					}
					
					col.remove(i);
					i--;
				}
			}
		}
		
		AnnotationSequenceGrid grid = new AnnotationSequenceGrid(sequence2, gridElementList2);
		
		return grid;
	}
	
	public AnnotationSequenceGrid append(AnnotationSequenceGrid grid)
	{
		AnnotationSequenceGrid grid2 = this.clone();
		AnnotationSequence sequence2 = new AnnotationSequence();
		List<String> toks2 = sequence.copyToks();
		sequence2.setToks(toks2);
		sequence2.setDocID(sequence.getDocID());
		toks2.addAll(grid.getSequence().copyToks());
		
		grid2.setSequence(sequence2);
		
		int base = gridElementList.size();
		for (int i=0; i<grid.gridElementList.size(); i++) {
			List<AnnotationGridElement> col = grid.gridElementList.get(i);
			
			List<AnnotationGridElement> col2 = new ArrayList<AnnotationGridElement>();
			grid2.add(col2);
			
			for (AnnotationGridElement elem : col) {
				AnnotationGridElement elem2 = null;
				if (elem != null) {
					elem2 = elem.clone();
					elem2.setStartIndex(base + elem.getStartIndex());
					elem2.setEndIndex(base + elem.getEndIndex());
				}
				
				col2.add(elem2);
			}
		}
		
		return grid2;
	}
	
	public String toString()
	{
		StringBuffer strBuf = new StringBuffer();
		for (int i=0; i<gridElementList.size(); i++) {
			strBuf.append("row " + i + ": ");
			List<AnnotationGridElement> col = gridElementList.get(i);
			
			for (AnnotationGridElement elem : col) {
				Annotation annot = elem.getAnnot();
				String label = ":null";
				
				int start = -1;
				int end = -1;
				int row = -1;
				if (elem != null) {
					label = elem.getTok();
					start = elem.getStartIndex();
					end = elem.getEndIndex();
					row = elem.getRow();
				}
				strBuf.append(label + "(" + start + "," + end + "," + row + ") ");
			}
			
			strBuf.append("\n");
		}
		
		return strBuf.toString();
	}

	public int[] getTargetCoords2() {
		return targetCoords2;
	}

	public void setTargetCoords2(int[] targetCoords2) {
		this.targetCoords2 = targetCoords2;
	}
}
