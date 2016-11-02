package msa.db;

public class MSADBException extends Exception
{
	public MSADBException()
	{
	}
	
	public MSADBException(String msg)
	{
		super(msg);
	}
	
	public MSADBException(Exception e)
	{
		super(e);
	}
}
