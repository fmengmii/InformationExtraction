package msa;

import java.util.ArrayList;
import java.util.List;

public class MSARow
{
	private List<MSAToken> baseTokens;
	private List<MSAToken> fillerTokens;
	private final int SPACE = 50;
	private String rowStr = null;
	
	public MSARow()
	{
		baseTokens = new ArrayList<MSAToken>();
		fillerTokens = new ArrayList<MSAToken>();
	}
	
	public List<MSAToken> getBaseTokens()
	{
		return baseTokens;
	}
	
	public MSAToken getBaseToken(int index)
	{
		return baseTokens.get(index);
	}
	
	public List<MSAToken> getFillerTokens()
	{
		return fillerTokens;
	}
	
	public MSAToken getFillerTokens(int index)
	{
		return fillerTokens.get(index);
	}
	
	public boolean isBaseToken(int index)
	{
		return index % 2 == 0;
	}
	
	public MSAToken get(int index)
	{
		if (isBaseToken(index)) {
			return baseTokens.get(index / 2);
		}
		
		return fillerTokens.get((index-1) / 2);
	}
	
	public void setBaseTokens(List<MSAToken> baseTokens)
	{
		this.baseTokens = baseTokens;
	}
	
	public void setFillerTokens(List<MSAToken> fillerTokens)
	{
		this.fillerTokens = fillerTokens;
	}
	
	public void addBaseToken(MSAToken token)
	{
		baseTokens.add(token);
	}
	
	public void addFillerTokens(int index, MSAToken tokens)
	{
		fillerTokens.set(index, tokens);
	}
	
	public void addFillerTokens(MSAToken tokens)
	{
		fillerTokens.add(tokens);
	}
	
	public int size()
	{
		return baseTokens.size() + fillerTokens.size();
	}
	
	public int getNumTokens()
	{
		int baseCount = 0;
		for (MSAToken tok : baseTokens) {
			if (tok.toString().length() > 0)
				baseCount++;
		}
		
		int fillerCount = 0;
		for (MSAToken tok : fillerTokens) {
			if (tok.toString().length() > 0)
				fillerCount++;
		}
		
		return baseCount + fillerCount;
	}
	
	public MSARow copy()
	{
		MSARow copyRow = new MSARow();
		List<MSAToken> baseTokens = new ArrayList<MSAToken>();
		for (int i=0; i<this.baseTokens.size(); i++) {
			baseTokens.add(this.baseTokens.get(i));
		}
		
		copyRow.baseTokens = baseTokens;
		
		List<MSAToken> fillerTokens = new ArrayList<MSAToken>();
		for (int i=0; i<this.fillerTokens.size(); i++) {
			MSAToken tok = this.fillerTokens.get(i).copy();
			fillerTokens.add(tok);
		}
		
		copyRow.fillerTokens = fillerTokens;
		
		return copyRow;
	}
	
	public String toString()
	{
		if (rowStr == null) {
			StringBuilder strBld = new StringBuilder();
			for (int i=0; i<baseTokens.size(); i++) {
				MSAToken baseTok = baseTokens.get(i);
				String baseTokStr = baseTok.toString();
				strBld.append(baseTokStr);
				
				for (int j=0; j<SPACE-baseTokStr.length(); j++)
					strBld.append(" ");
				
				if (i < fillerTokens.size()) {
					MSAToken tok = fillerTokens.get(i);
					String tokStr = tok.toString();
					if (tokStr.length() > 0)
						strBld.append(tokStr + " ");
					else
						strBld.append("||| ");
					
					for (int j=0; j<SPACE-tokStr.length(); j++)
						strBld.append(" ");
				}
				
			}
			
			rowStr = strBld.toString().trim();
		}
		
		return rowStr;
	}
	
	public String toPlainString()
	{
		StringBuilder strBld = new StringBuilder();
		for (int i=0; i<baseTokens.size(); i++) {
			MSAToken baseTok = baseTokens.get(i);
			String baseTokStr = baseTok.toString();
			strBld.append(baseTokStr);
			
			strBld.append(" ");
			
			/*
			if (i < fillerTokens.size()) {
				MSAToken tok = fillerTokens.get(i);
				String tokStr = tok.toString();
				if (tokStr != null && tokStr.length() > 0)
					strBld.append(tokStr + " ");
			}
			*/
		}
		
		return strBld.toString().trim();
	}
	
	public boolean equals(MSARow row)
	{
		return row.toString().equals(this.toString());
	}
	
	public List<String> getTokensAsStrings()
	{
		List<String> strList = new ArrayList<String>();
		
		for (int i=0; i<baseTokens.size(); i++) {
			strList.add(baseTokens.get(i).toString());
			if (i < fillerTokens.size()) {
				MSAToken tok = fillerTokens.get(i);
				String tokStr = tok.toString();
				if (tokStr != null && tokStr.length() > 0)
					strList.add(tokStr);
			}
		}
		
		return strList;
	}
}
