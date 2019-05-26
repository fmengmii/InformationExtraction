package align;

import java.util.ArrayList;
import java.util.List;

public class GridScore
{
	private String tok1;
	private String tok2;
	private StringBuilder tokMulti;
	private double score = 0.0;
	private int annotIndex1;
	private int annotIndex2;
	private GridScore next;
	private int direction;
	private int row;
	private int col;
	private List<String> tokList;

	public GridScore()
	{
		tokList = new ArrayList<String>();
		tokMulti = new StringBuilder();
	}
	
	public GridScore(int row, int col)
	{
		this();
		this.row = row;
		this.col = col;
	}
	
	public GridScore(int row, int col, String tok1, String tok2)
	{
		this(row, col);
		this.tok1 = tok1;
		this.tok2 = tok2;
	}
	
	public GridScore(int row, int col, String tok1, String tok2, int annotIndex1, int annotIndex2)
	{
		this(row, col, tok1, tok2);
		this.annotIndex1 = annotIndex1;
		this.annotIndex2 = annotIndex2;
	}
	
	public GridScore(int row, int col, String tok1, String tok2, double score, int annotIndex1, int annotIndex2)
	{
		this(row, col, tok1, tok2, annotIndex1, annotIndex2);
		this.score = score;
	}
	
	public GridScore(GridScore gridScore)
	{
		this();
		this.tok1 = gridScore.tok1;
		this.tok2 = gridScore.tok2;
		this.score = gridScore.score;
		this.annotIndex1 = gridScore.annotIndex1;
		this.annotIndex2 = gridScore.annotIndex2;
		this.next = gridScore.next;
		this.direction = gridScore.direction;
		this.row = gridScore.row;
		this.col = gridScore.col;
	}

	public String getTok1() {
		return tok1;
	}

	public void setTok1(String tok1) {
		this.tok1 = tok1;
	}

	public String getTok2() {
		return tok2;
	}

	public void setTok2(String tok2) {
		this.tok2 = tok2;
	}

	public double getScore() {
		return score;
	}

	public void setScore(double score) {
		this.score = score;
	}

	public int getAnnotIndex1() {
		return annotIndex1;
	}

	public void setAnnotIndex1(int annotIndex1) {
		this.annotIndex1 = annotIndex1;
	}
	
	public int getAnnotIndex2() {
		return annotIndex2;
	}

	public void setAnnotIndex2(int annotIndex2) {
		this.annotIndex2 = annotIndex2;
	}
	
	public GridScore getNext()
	{
		return next;
	}
	
	public void setNext(GridScore next)
	{
		this.next = next;
	}
	
	public int getDirection()
	{
		return direction;
	}
	
	public void setDirection(int direction)
	{
		this.direction = direction;
	}
	
	public int getRow()
	{
		return row;
	}
	
	public int getCol()
	{
		return col;
	}
	
	public List<String> getTokList()
	{
		return tokList;
	}
	
	public void addTok(String tok)
	{
		//System.out.println("tok: " + tok + ", tokMulti: " + tokMulti.toString());
		//this is the target
		//if (tok.indexOf("|") < 0) {
		if (tok.equals(":target")) {
			//System.out.println("here1");
			tokList = new ArrayList<String>();
			tokList.add(tok);
			tokMulti = new StringBuilder(tok);
		}
		else if (tokMulti.length() == 0 || tokMulti.indexOf(":target") < 0) {
			//System.out.println("here2");
			
			if (tokList.indexOf(tok) < 0) {
				tokList.add(tok);
				
				if (tokMulti.length() > 0)
					tokMulti.append("!");
				tokMulti.append(tok);
			}
		}
		
		//System.out.println("tokMulti: " + tokMulti.toString());
	}
	
	public String getTokMulti()
	{
		return tokMulti.toString();
	}
	
	public void setTokMulti(String tok)
	{
		tokMulti = new StringBuilder(tok);
	}
}
