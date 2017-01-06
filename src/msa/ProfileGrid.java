package msa;

import java.util.*;
import align.AnnotationSequenceGrid;

public class ProfileGrid
{
	private AnnotationSequenceGrid grid;
	private List<AnnotationSequenceGrid> targetGridList;
	
	public ProfileGrid()
	{
		targetGridList = new ArrayList<AnnotationSequenceGrid>();
	}
	
	public ProfileGrid(AnnotationSequenceGrid grid)
	{
		this.grid = grid;
	}
	
	public ProfileGrid(AnnotationSequenceGrid grid, List<AnnotationSequenceGrid> targetGridList)
	{
		this(grid);
		this.targetGridList = targetGridList;
	}

	public AnnotationSequenceGrid getGrid() {
		return grid;
	}

	public void setGrid(AnnotationSequenceGrid grid) {
		this.grid = grid;
	}

	public List<AnnotationSequenceGrid> getTargetGridList() {
		return targetGridList;
	}

	public void setTargetGridList(List<AnnotationSequenceGrid> targetGridList) {
		this.targetGridList = targetGridList;
	}
	
	public void addTarget(AnnotationSequenceGrid targetGrid)
	{
		targetGridList.add(targetGrid);
	}
}
