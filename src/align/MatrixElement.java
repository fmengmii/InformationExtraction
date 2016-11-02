package align;

import java.util.*;

public class MatrixElement
{
	private List<GridScore> gridScoreList;
	private List<GridScore> gridPointers;
	private List<GridScore> gridPointersDim2;
	private List<GridScore> gridPointersDiag;
	private double score = 0.0;
	
	
	public MatrixElement(int height)
	{
		gridScoreList = new ArrayList<GridScore>();
		gridPointers = new ArrayList<GridScore>();
		gridPointersDim2 = new ArrayList<GridScore>();
		gridPointersDiag = new ArrayList<GridScore>();
		
		/*
		for (int i=0; i<height; i++)
			gridPointers.add(null);
			*/
	}
	
	public void addGridScore(GridScore gridScore)
	{
		gridScoreList.add(gridScore);
	}
	
	public void addAllGridScore(List<GridScore> gridScoreList)
	{
		this.gridScoreList.addAll(gridScoreList);
	}
	
	public GridScore getGridScore(int index)
	{
		return gridScoreList.get(index);
	}
	
	public void addGridPointer(GridScore gridScore)
	{
		gridPointers.add(gridScore);
	}
	
	public void addGridPointer(int index, GridScore gridScore)
	{
		gridPointers.set(index, gridScore);
	}
	
	public void addGridPointerDim2(GridScore gridScore)
	{
		gridPointersDim2.add(gridScore);
	}
	
	public void addGridPointerDiag(GridScore gridScore)
	{
		gridPointersDiag.add(gridScore);
	}
	
	public List<GridScore> getGridScoreList()
	{
		return gridScoreList;
	}
	
	public List<GridScore> getGridPointers()
	{
		return gridPointers;
	}
	
	public List<GridScore> getGridPointersDim2()
	{
		return gridPointersDim2;
	}
	
	public List<GridScore> getGridPointersDiag()
	{
		return gridPointersDiag;
	}
	
	public double getScore()
	{
		return score;
	}
	
	public void setScore(double score)
	{
		this.score = score;
	}
}
