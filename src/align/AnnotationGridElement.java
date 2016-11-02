package align;

import java.util.*;

import msa.Annotation;

public class AnnotationGridElement
{
	private Annotation annot;
	private String tok;
	private int startIndex;
	private int endIndex;
	private int row;

	public AnnotationGridElement()
	{
	}
	
	public AnnotationGridElement(String tok, int startIndex, int endIndex, int row)
	{
		this.tok = tok;
		this.startIndex = startIndex;
		this.endIndex = endIndex;
		this.row = row;
	}
	
	public AnnotationGridElement(Annotation annot, String tok, int startIndex, int endIndex, int row)
	{
		this(tok, startIndex, endIndex, row);
		this.annot = annot;
	}
	
	public AnnotationGridElement clone()
	{
		AnnotationGridElement elem = new AnnotationGridElement();
		elem.annot = annot;
		elem.tok = tok;
		elem.startIndex = startIndex;
		elem.endIndex = endIndex;
		elem.row = row;
		
		return elem;
	}

	public Annotation getAnnot() {
		return annot;
	}

	public void setAnnot(Annotation annot) {
		this.annot = annot;
	}

	public int getStartIndex() {
		return startIndex;
	}

	public void setStartIndex(int startIndex) {
		this.startIndex = startIndex;
	}

	public int getEndIndex() {
		return endIndex;
	}

	public void setEndIndex(int endIndex) {
		this.endIndex = endIndex;
	}
	
	public String getTok()
	{
		return tok;
	}
	
	public void setTok(String tok)
	{
		this.tok = tok;
	}

	public int getRow() {
		return row;
	}

	public void setRow(int row) {
		this.row = row;
	}
}
