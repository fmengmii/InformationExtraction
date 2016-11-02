package msa;

import java.util.*;

public class MSARowBean
{
	private List<String> baseTokens;
	private List<String> fillerTokens;
	
	public MSARowBean()
	{
	}
	
	public MSARowBean(List<String> baseTokens, List<String> fillerTokens)
	{
		this.baseTokens = baseTokens;
		this.fillerTokens = fillerTokens;
	}

	public List<String> getBaseTokens() {
		return baseTokens;
	}

	public void setBaseTokens(List<String> baseTokens) {
		this.baseTokens = baseTokens;
	}

	public List<String> getFillerTokens() {
		return fillerTokens;
	}

	public void setFillerTokens(List<String> fillerTokens) {
		this.fillerTokens = fillerTokens;
	}
	
	
}
