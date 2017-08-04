package msa;

import nlputils.sequence.*;

import java.util.*;

public class SmithWatermanMSA 
{
	private double[][] matrix;
	private int[][] links;
	private double hiScore;
	private int hiX;
	private int hiY;
	private double gapPenalty = -0.0001;
	private double matchScore = 1.0;
	private double mismatchScore = -1.0;
	private double targetMatchScore = 100.0;
	private double gapMatchScore = 0.0;
	private double wildcardMatchScore = 0.0;
	private ScoreTokens scoreTokens;
	private List<String> alignment1;
	private List<String> alignment2;
	//private int matchIndexes1;
	//private int matchIndexes2;
	private List<Integer> matchIndexesList1;
	private List<Integer> matchIndexesList2;
	
	
	public SmithWatermanMSA()
	{
		scoreTokens = null;
	}
	
	public void setMatchScore(double matchScore)
	{
		this.matchScore = matchScore;
	}
	
	public void setMismatchScore(double mismatchScore)
	{
		this.mismatchScore = mismatchScore;
	}
	
	public void setGapPenalty(double gapPenalty)
	{
		this.gapPenalty = gapPenalty;
	}
	
	public void setWildcardMatchScore(double wildcardMatchScore)
	{
		this.wildcardMatchScore = wildcardMatchScore;
	}
	
	public void setScoreTokens(ScoreTokens scoreTokens)
	{
		this.scoreTokens = scoreTokens;
	}
	
	public List<String> getAlignment1()
	{
		return alignment1;
	}
	
	public List<String> getAlignment2()
	{
		return alignment2;
	}
	
//	public int getMatchIndexes1()
//	{
//		return matchIndexes1;
//	}
//	
//	public int getMatchIndexes2()
//	{
//		return matchIndexes2;
//	}
	
	public List<Integer> getMatchIndexesList1()
	{
		return matchIndexesList1;
	}
	
	public List<Integer> getMatchIndexesList2()
	{
		return matchIndexesList2;
	}
	
	public double align(List<String> toks1, List<String> toks2)
	{
		hiScore = -Double.MAX_VALUE;
		matrix = new double[toks1.size()+1][toks2.size()+1];
		links = new int[toks1.size()+1][toks2.size()+1];
		
		for (int i=1; i<=toks1.size(); i++) {
			String tok1 = toks1.get(i-1);
			
			for (int j=1; j<=toks2.size(); j++) {
				String tok2 = toks2.get(j-1);
			
				double score1 = matrix[i-1][j-1] + match(tok1, tok2);
				double score2 = matrix[i-1][j] + gapPenalty;
				double score3 = matrix[i][j-1] + gapPenalty;
				
				double score = 0.0;
				int direction = 0;
				
				if (score1 > 0) {
					score = score1;
					direction = 1;
				}
				
				if (score2 > score) {
					score = score2;
					direction = 2;
					
				}
				
				if (score3 > score) {
					score = score3;
					direction = 3;
				}
				
				matrix[i][j] = score;
				links[i][j] = direction;
				
				if (score > hiScore) {
					hiScore = score;
					hiX = i;
					hiY = j;
				}
			}
			
			
		}
		
		doAlignment(toks1, toks2);
		
		return hiScore;
	}
	
	private void doAlignment(List<String> toks1, List<String> toks2)
	{
		alignment1 = new ArrayList<String>();
		alignment2 = new ArrayList<String>();
		int i = hiX;
		int j = hiY;
		//matchIndexes1 = 0;
		//matchIndexes2 = 0;
		matchIndexesList1 = new ArrayList<Integer>();
		matchIndexesList2 = new ArrayList<Integer>();
		
		while (matrix[i][j] > 0) {
			int dir = links[i][j];

			if (dir == 1) {
				alignment1.add(0, toks1.get(i-1));
				alignment2.add(0, toks2.get(j-1));
				i--;
				j--;
				//matchIndexes1 = matchIndexes1 | (int) Math.pow(2.0, i);
				//matchIndexes2 = matchIndexes2 | (int) Math.pow(2.0, j);
				matchIndexesList1.add(0, i);
				matchIndexesList2.add(0, j);
			}
			else if (dir == 2) {
				alignment1.add(0, toks1.get(i-1));
				alignment2.add(0, "|||");
				i--;
			}
			else if (dir == 3) {
				alignment1.add(0, "|||");
				alignment2.add(0, toks2.get(j-1));
				j--;
			}
		}
	}
	
	private double match(String tok1, String tok2)
	{
		double score = 0.0;
		
		if (tok1 == null || tok2 == null)
			return 0.0;
		
		if (scoreTokens == null) {			
			if (tok1.startsWith("$") && tok1.substring(1).equals(tok2))
				return targetMatchScore;
			
			else if (!tok1.equals(tok2) && (tok1.equals("%%") || tok2.equals("%%")))
				score = -targetMatchScore;
			
			else if (tok1.equals("*") || tok2.equals("*"))
				return wildcardMatchScore;
			
			else if (tok1.equals(tok2)) {
				if (tok1.equals(":target") || tok1.equals("%%"))
					score = targetMatchScore;
				else
					score = matchScore;
			}
			/*
			else if (tok1.equals("-") || tok2.equals("-"))
				score = gapMatchScore;
				*/
			else {
				score = mismatchScore;
			}
		}
		else {
			score = scoreTokens.match(tok1, tok2);
		}
		
		return score;
	}
	
	private double gapPenalty(String tok1, String tok2)
	{
		if (tok1.equals("-") || tok2.equals("-"))
			return 0.0;
		
		return gapPenalty;
	}
	
	public String listToString(List<String> list)
	{
		StringBuffer strBuf = new StringBuffer();
		for (int i=0; i<list.size(); i++) {
			strBuf.append(list.get(i) + " ");
		}
		
		return strBuf.toString().trim();
	}
	
	public void printMatrix()
	{
		for (int i=0; i<matrix.length; i++) {
			for (int j=0; j<matrix[i].length; j++) {
				System.out.format("%5.2f ", matrix[i][j]);
			}
			
			System.out.println();
		}
		
		for (int i=0; i<links.length; i++) {
			for (int j=0; j<links[i].length; j++)
				System.out.print(links[i][j] + " ");
			
			System.out.println();
		}
	}
	
	public static void main(String[] args)
	{
		SmithWatermanMSA sw = new SmithWatermanMSA();
		sw.setGapPenalty(-0.1);
		//sw.setMatchScore(1.0);
		sw.setMismatchScore(-1.0);
		MultipleSequenceAlignment msa = new MultipleSequenceAlignment();
		List<String> toks1 = SequenceUtilities.getToksFromStr("her blood pressure ranged as an outpatient between 170 to / to ##");
		//List<String> toks1 = SequenceUtilities.getToksFromStr(":start :target . :end");
		
		List<String> toks2 = SequenceUtilities.getToksFromStr("as a result lopressor was titrated down from 25 mg to ## mg bid");
		//List<String> toks2 = SequenceUtilities.getToksFromStr(":start ( :number - :target ) :end");
		System.out.println(msa.toString());
		
		double score = sw.align(toks1, toks2);
		List<String> alignment1 = sw.getAlignment1();
		List<String> alignment2 = sw.getAlignment2();
		sw.printMatrix();
		System.out.println("score: " + score);
		System.out.println(sw.listToString(alignment1) + "\n" + sw.listToString(alignment2));
		System.out.println(sw.getMatchIndexesList2());
		
		/*
		double score = sw.align(msa, toks2);
		toks2 = SequenceUtilities.getToksFromStr("no acute ##");
		sw.align(msa, toks2);
		toks2 = SequenceUtilities.getToksFromStr("no large ##");
		sw.align(msa, toks2);
		toks2 = SequenceUtilities.getToksFromStr("no enhancing ##");
		sw.align(msa, toks2);
		toks2 = SequenceUtilities.getToksFromStr("no significant ##");
		sw.align(msa, toks2);
		toks2 = SequenceUtilities.getToksFromStr("no gross ##");
		sw.align(msa, toks2);
		
		toks2 = SequenceUtilities.getToksFromStr("no #orientation# ##");
		sw.align(msa, toks2);
		*/
		
		//sw.printMatrix();
		
		//List<String> toks3 = SequenceUtilities.getToksFromStr("there is no evidence for #anat# ## or #finding#");
		//sw.alignGlobal(msa, toks3);
		
		//System.out.println("score: " + score + " hiX:" + sw.hiX + " hiY:" + sw.hiY);
		//System.out.println(msa.toString());
		
	}

}
