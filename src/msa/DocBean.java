package msa;

public class DocBean
{
	private String docNamespace;
	private String docTable;
	private long docID;
	private long frameInstanceID = -1;
	private Integer status;
	
	public DocBean()
	{
	}
	
	public DocBean(String docNamespace, String docTable, long docID)
	{
		this.docNamespace = docNamespace;
		this.docTable = docTable;
		this.docID = docID;
	}
	
	public DocBean(String docNamespace, String docTable, long docID, long frameInstanceID, Integer status)
	{
		this.docNamespace = docNamespace;
		this.docTable = docTable;
		this.docID = docID;
		this.frameInstanceID = frameInstanceID;
		this.status = status;
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

	public long getFrameInstanceID() {
		return frameInstanceID;
	}

	public void setFrameInstanceID(long frameInstanceID) {
		this.frameInstanceID = frameInstanceID;
	}

	public Integer getStatus() {
		return status;
	}

	public void setStatus(int status) {
		this.status = status;
	}
}
