package cluster;

public class DocScore
{
	private int doc1;
	private int doc2;
	private double score;
	
	public DocScore()
	{
	}
	
	public DocScore(int doc1, int doc2, double score)
	{
		this.doc1 = doc1;
		this.doc2 = doc2;
		this.score = score;
	}

	public int getDoc1() {
		return doc1;
	}

	public void setDoc1(int doc1) {
		this.doc1 = doc1;
	}

	public int getDoc2() {
		return doc2;
	}

	public void setDoc2(int doc2) {
		this.doc2 = doc2;
	}

	public double getScore() {
		return score;
	}

	public void setScore(double score) {
		this.score = score;
	}
}
