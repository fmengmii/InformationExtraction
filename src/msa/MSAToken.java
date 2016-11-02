package msa;

import java.util.ArrayList;
import java.util.List;

public class MSAToken 
{
	private List<String> tokens;
	
	public MSAToken()
	{
		tokens = new ArrayList<String>();
	}
	
	public MSAToken(String tok)
	{
		this();
		tokens.add(tok);		
	}
	
	public List<String> getTokens()
	{
		return tokens;
	}
	
	public void addToken(String tok)
	{
		tokens.add(tok);
	}
	
	public int size()
	{
		return tokens.size();
	}
	
	public MSAToken copy()
	{
		MSAToken tok = new MSAToken();
		List<String> tokens = new ArrayList<String>();
		for (int i=0; i<this.tokens.size(); i++)
			tokens.add(this.tokens.get(i));
		
		tok.tokens = tokens;
		
		return tok;
	}
	
	public String toString()
	{
		StringBuffer strBuf = new StringBuffer();
		for (String tok : tokens) {
			strBuf.append(tok + " ");
		}
		
		return strBuf.toString().trim();
	}
}
