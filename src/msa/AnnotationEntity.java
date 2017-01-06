package msa;

import java.util.*;

public class AnnotationEntity
{
	private String value;
	private long docID;
	private long start;
	private long end;
	private List<long[]> indexList;
	
	public AnnotationEntity()
	{
		indexList = new ArrayList<long[]>();
	}
	
	public AnnotationEntity(String value, long docID, long start, long end)
	{
		this();
		this.value = value;
		this.docID = docID;
		this.start = start;
		this.end = end;
		long[] indexes = new long[2];
		indexes[0] = start;
		indexes[1] = end;
		indexList.add(indexes);
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public long getDocID() {
		return docID;
	}

	public void setDocID(long docID) {
		this.docID = docID;
	}

	public long getStart() {
		return start;
	}

	public void setStart(long start) {
		this.start = start;
	}

	public long getEnd() {
		return end;
	}

	public void setEnd(long end) {
		this.end = end;
	}
	
	public void addIndexes(long[] indexes)
	{
		indexList.add(indexes);
	}

	public List<long[]> getIndexList() {
		return indexList;
	}

	public void setIndexList(List<long[]> indexList) {
		this.indexList = indexList;
	}
}
