package msa;

import nlputils.sequence.*;

import java.util.*;

import align.AnnotationSequenceGrid;

public class MultipleSequenceAlignment
{
	private List<MSARow> alignment;
	private List<Integer> multiplicity;
	private List<List<SentenceInfo>> sentInfoList;
	private List<Integer> colSizes;
	private int weight;
	private int id;
	private int total = 0;
	
	public MultipleSequenceAlignment()
	{
		alignment = new ArrayList<MSARow>();
		weight = 0;
		multiplicity = new ArrayList<Integer>();
		colSizes = new ArrayList<Integer>();
		sentInfoList = new ArrayList<List<SentenceInfo>>();
	}
	
	public int getNumRows()
	{
		return alignment.size();
	}
	
	public int getTotal()
	{
		return total;
	}
	
	public int getNumCols()
	{
		if (alignment.size() == 0)
			return 0;
		
		return alignment.get(0).size();
	}
	
	public MSARow getRow(int row)
	{
		return alignment.get(row);
	}
	
	public List<SentenceInfo> getSentInfoList(int row)
	{
		return sentInfoList.get(row);
	}
	
	public List<MSAToken> getColumn(int col)
	{
		List<MSAToken> column = new ArrayList<MSAToken>();
		for (int i=0; i<getNumRows(); i++) {
			MSAToken tok = get(i, col);
			column.add(tok);
		}
		
		return column;
	}
	
	public MSARow getRowCopy(int rowIndex)
	{
		MSARow rowCopy = alignment.get(rowIndex).copy();
		return rowCopy;
	}
	
	public MSAToken get(int row, int col)
	{
		MSARow msaRow = alignment.get(row);
		return msaRow.get(col);
	}
	
	public int getWeight()
	{
		return weight;
	}
	
	public int getID()
	{
		return id;
	}
	
	public void setID(int id)
	{
		this.id = id;
	}
	
	public int getMultiplicity(int index)
	{
		return multiplicity.get(index); 
	}
	
	public void addRow(List<String> toks, List<Integer> matchIndexesList, SentenceInfo sentInfo)
	{		
		MSARow row = new MSARow();
		List<MSAToken> baseTokens = new ArrayList<MSAToken>();
		List<MSAToken> fillerTokens = new ArrayList<MSAToken>();
		MSAToken currTok = new MSAToken();
		boolean start = false;
		int minIndex = matchIndexesList.get(0);
		int maxIndex = matchIndexesList.get(matchIndexesList.size()-1);
		
		for (int i=0; i<toks.size(); i++) {
			if (i > maxIndex)
				break;

			String tok = toks.get(i);
			
			if (matchIndexesList.contains(i)) {
				if (start) {
					fillerTokens.add(currTok);
					int colIndex = fillerTokens.size()-1;

					if (colSizes.size() <= colIndex)
						colSizes.add(currTok.size());
					
					else{
						if (currTok.size() > colSizes.get(colIndex)) 
							colSizes.set(colIndex, currTok.size());
					}
				}
				
				baseTokens.add(new MSAToken(tok));
				start = true;
				currTok = new MSAToken();
				
			}
			else if (i > minIndex)
				currTok.addToken(tok);
			
		}
		
		row.setBaseTokens(baseTokens);
		row.setFillerTokens(fillerTokens);
		
		int rowIndex = duplicateRow(row);
		
		if (rowIndex >= 0) {
			int currMult = multiplicity.get(rowIndex);
			multiplicity.set(rowIndex, ++currMult);
			List<SentenceInfo> oneSentInfoList = sentInfoList.get(rowIndex);
			oneSentInfoList.add(sentInfo);
		}
		
		else {
			alignment.add(row);
			multiplicity.add(1);
			List<SentenceInfo> oneSentInfoList = new ArrayList<SentenceInfo>();
			oneSentInfoList.add(sentInfo);
			sentInfoList.add(oneSentInfoList);
		}
		
		total++;
	}
	
	public void addRow(List<String> toks, int mult)
	{
		MSARow row = new MSARow();

		for (int i=0; i<toks.size(); i++) {
			if (i> 0)
				row.addFillerTokens(new MSAToken());
			
			String tok = toks.get(i);
			MSAToken msaTok = new MSAToken();
			msaTok.addToken(tok);
			row.addBaseToken(msaTok);
		}
		
		alignment.add(row);
		multiplicity.add(mult);
	}
	
	public void addRow(MSARowBean rowBean, int mult)
	{
		
		MSARow row = new MSARow();
		List<MSAToken> fillerTokens = row.getFillerTokens();

		for (String tokStr : rowBean.getBaseTokens()) {
			MSAToken tok = new MSAToken();
			tok.addToken(tokStr);
			row.addBaseToken(tok);
		}
		
		for (String tokStr : rowBean.getFillerTokens()) {
			MSAToken tok = new MSAToken();
			if (tokStr.length() > 0) {
				String[] parts = tokStr.split(" ");
				for (int i=0; i<parts.length; i++)
					tok.addToken(parts[i]);
			}
			row.addFillerTokens(tok);
			
			int colIndex = fillerTokens.size()-1;

			if (colSizes.size() <= colIndex)
				colSizes.add(tok.size());
			
			else{
				if (tok.size() > colSizes.get(colIndex)) 
					colSizes.set(colIndex, tok.size());
			}
		}
		
		alignment.add(row);
		multiplicity.add(mult);
	}
	
	public String toString()
	{
		StringBuilder strBld = new StringBuilder();
		strBld.append("ID: " + id + "\n");
		for (int i=0; i<alignment.size(); i++) {
			MSARow row = alignment.get(i);
			strBld.append(row.toString());
			strBld.append(" (x" + multiplicity.get(i) + ")");
			strBld.append("\n");
		}
		
		return strBld.toString();
	}
	
	public String toProfileString(boolean dashes, String dashChar)
	{
		StringBuilder strBld = new StringBuilder();
		if (alignment.size() > 0) {
			MSARow row = alignment.get(0);
			String lastBaseTok = "";
			for (int i=0; i<row.size(); i++) {
				MSAToken tok = row.get(i);
				String tokStr = tok.toString();

				if (row.isBaseToken(i)) {
					/*
					if (tokStr.startsWith("#") && tokStr.equals(lastBaseTok))
						continue;
						*/
					if (tokStr != null && tokStr.length() > 0) {
						strBld.append(tokStr + " ");
						//if (tokStr.startsWith("#"))
						lastBaseTok = tok.toString();
					}
				}
				else if (!isColumnEmpty(i) && dashes) {
					//int colSize = colSizes.get((i-1)/2);
					//for (int j=0; j<colSize; j++)
					strBld.append(dashChar + " ");
				}
			}
		}
		
		return strBld.toString().trim();
	}
	
	public String toProfileStringMultiGap(boolean dashes, String dashChar)
	{
		StringBuilder strBld = new StringBuilder();
		if (alignment.size() > 0) {
			MSARow row = alignment.get(0);
			String lastBaseTok = "";
			for (int i=0; i<row.size(); i++) {
				MSAToken tok = row.get(i);
				String tokStr = tok.toString();
				if (row.isBaseToken(i)) {
					/*
					if (tokStr.startsWith("#") && tokStr.equals(lastBaseTok))
						continue;
						*/
					if (tokStr != null && tokStr.length() > 0) {
						strBld.append(tokStr + " ");
						//if (tokStr.startsWith("#"))
						lastBaseTok = tok.toString();
					}
				}
				else if (!isColumnEmpty(i) && dashes) {
					int colSize = colSizes.get((i-1)/2);
					for (int j=0; j<colSize; j++)
						strBld.append(dashChar + " ");
				}
			}
		}
		
		return strBld.toString().trim();
	}
	
	public String toProfileString()
	{
		return toProfileString(true, "-");
	}
	
	public String toProfileString(boolean dashes)
	{
		return toProfileString(dashes, "-");
	}
	
	public String toProfileStringWildCard()
	{
		return toProfileString(true, "*");
	}
	
	public boolean isColumnEmpty(int index)
	{
		List<MSAToken> column = getColumn(index);
		for (MSAToken tok : column) {
			if (tok.size() > 0)
				return false;
		}
		
		return true;
	}
	
	public int getTotalRows()
	{
		int total = 0;
		for (int mult : multiplicity)
			total += mult;
		
		return total;
	}
	
	private int duplicateRow(MSARow row)
	{
		for (int i=0; i<alignment.size(); i++) {
			MSARow row2 = alignment.get(i);
			if (row.equals(row2))
				return i;
		}
		
		return -1;
	}
	
	public void combine(MultipleSequenceAlignment msa)
	{
		for (int i=0; i<msa.getNumRows(); i++) {
			MSARow row = msa.getRow(i);
			int rowIndex = duplicateRow(row);
			if (rowIndex >= 0) {
				int currMult = multiplicity.get(rowIndex);
				multiplicity.set(rowIndex, ++currMult);
			}
			else if (row.size() == this.alignment.get(0).size()){
				alignment.add(row);
				multiplicity.add(1);
			}
		}
	}
	
	public static void main(String[] args)
	{
		MultipleSequenceAlignment msa = new MultipleSequenceAlignment();
		List<String> toks = SequenceUtilities.getToksFromStr("there is a ##");
		List<Integer> matchIndexesList1 = new ArrayList<Integer>();
		matchIndexesList1.add(1);
		matchIndexesList1.add(2);
		matchIndexesList1.add(3);
		msa.addRow(toks, matchIndexesList1, null);
		
		toks = SequenceUtilities.getToksFromStr("is a large and big ##");
		List<Integer> matchIndexesList2 = new ArrayList<Integer>();
		matchIndexesList2.add(0);
		matchIndexesList2.add(1);
		matchIndexesList2.add(5);
		msa.addRow(toks, matchIndexesList2, null);
		
		System.out.println(msa.toString());
		System.out.println(msa.toProfileString());
	}
}
