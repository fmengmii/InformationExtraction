package msa;

public class ProfileMatchBean
{
	private long docID;
	private long start;
	private long end;
	private String value;
	private long profileID;
	private long targetID;
	private double score;
	
	public ProfileMatchBean()
	{
	}
	
	public ProfileMatchBean(long docID, long start, long end, String value)
	{
		this.docID = docID;
		this.start = start;
		this.end = end;
		this.value = value;
	}
	
	public ProfileMatchBean(long profileID, long targetID, long docID, long start, long end, String value)
	{
		this(docID, start, end ,value);
		this.profileID = profileID;
		this.targetID = targetID;
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

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public long getProfileID() {
		return profileID;
	}

	public void setProfileID(long profileID) {
		this.profileID = profileID;
	}

	public long getTargetID() {
		return targetID;
	}

	public void setTargetID(long targetID) {
		this.targetID = targetID;
	}

	public double getScore() {
		return score;
	}

	public void setScore(double score) {
		this.score = score;
	}
	
	
}
