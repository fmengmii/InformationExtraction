package msa;

import java.util.*;

public class AnnotationSentence
{
	private List<String> toks;
	private AnnotationSequence sequence;
	private int sentIndex;
	
	public AnnotationSentence()
	{
	}
	
	public AnnotationSentence(List<String> toks, AnnotationSequence sequence, int sentIndex)
	{
		this.toks = toks;
		this.sequence = sequence;
		this.sentIndex = sentIndex;
	}
	
	public List<String> getToks()
	{
		return toks;
	}
	
	public AnnotationSequence getSequence()
	{
		return sequence;
	}
	
	public int getSentIndex()
	{
		return sentIndex;
	}
}
