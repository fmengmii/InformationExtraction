package msa;

import java.util.*;
import align.AnnotationSequenceGrid;

public class ProfileGrid
{
	private AnnotationSequenceGrid grid;
	private Map<AnnotationSequenceGrid, Boolean> targetGridMap;
	
	public ProfileGrid()
	{
		targetGridMap = new HashMap<AnnotationSequenceGrid, Boolean>();
	}
	
	public ProfileGrid(AnnotationSequenceGrid grid)
	{
		this();
		this.grid = grid;
	}
	
	public ProfileGrid(AnnotationSequenceGrid grid, Map<AnnotationSequenceGrid, Boolean> targetGridMap)
	{
		this(grid);
		this.targetGridMap = targetGridMap;
	}

	public AnnotationSequenceGrid getGrid() {
		return grid;
	}

	public void setGrid(AnnotationSequenceGrid grid) {
		this.grid = grid;
	}

	public Map<AnnotationSequenceGrid, Boolean> getTargetGridMap() {
		return targetGridMap;
	}

	public void setTargetGridMap(Map<AnnotationSequenceGrid, Boolean> targetGridMap) {
		this.targetGridMap = targetGridMap;
	}
	
	public void addTarget(AnnotationSequenceGrid targetGrid)
	{
		targetGridMap.put(targetGrid, true);
	}
}
