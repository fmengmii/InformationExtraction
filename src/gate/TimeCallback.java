package gate;

import java.util.*;

public class TimeCallback implements GateCallback
{
	private long startTime;
	private long duration;
	private Map<String, Object> retMap;
	
	public TimeCallback(long duration)
	{
		this.duration = duration;
		startTime = System.currentTimeMillis();
		retMap = new HashMap<String, Object>();
		retMap.put("terminate", new Boolean(false));
	}
	
	public void setDuration(long duration)
	{
		this.duration = duration;
	}

	public Map<String, Object> callBack(long docID)
	{
		long currTime = System.currentTimeMillis();
		if (currTime - startTime > duration)
			retMap.put("terminate", new Boolean(true));
		
		return retMap;
	}
}
