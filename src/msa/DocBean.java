package msa;

public class DocBean
{
	private String docNamespace;
	private String docTable;
	private long docID;
	
	public DocBean()
	{
	}
	
	public DocBean(String docNamespace, String docTable, long docID)
	{
		this.docNamespace = docNamespace;
		this.docTable = docTable;
		this.docID = docID;
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
}
