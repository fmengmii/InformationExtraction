package msa;

public class SentenceInfo
{
	private String docNamespace;
	private String docTable;
	private long docID;
	private int sentID;
	
	public SentenceInfo()
	{
	}
	
	public SentenceInfo(String docNamespace, String docTable, long docID, int sentID)
	{
		this.docNamespace = docNamespace;
		this.docTable = docTable;
		this.docID = docID;
		this.sentID = sentID;
	}

	public String getDocNamespace() {
		return docNamespace;
	}

	public void setDocNamespace(String docNamespace) {
		this.docNamespace = docNamespace;
	}

	public String getDocTable() {
		return docTable;
	}

	public void setDocTable(String docTable) {
		this.docTable = docTable;
	}

	public long getDocID() {
		return docID;
	}

	public void setDocID(long docID) {
		this.docID = docID;
	}

	public int getSentID() {
		return sentID;
	}

	public void setSentID(int sentID) {
		this.sentID = sentID;
	}
}
