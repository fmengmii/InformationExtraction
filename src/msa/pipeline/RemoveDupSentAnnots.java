package msa.pipeline;

import java.util.*;
import java.io.*;
import java.sql.*;

public class RemoveDupSentAnnots extends MSAModule
{
	private int projID;
	
	public RemoveDupSentAnnots()
	{
	}

	public void init(String user, String password, String config)
	{
		try {
			Properties props = new Properties();
			props.load(new FileReader(config));
			
			schema = props.getProperty("schema") + ".";
			
			String host = props.getProperty("host");
			String dbName = props.getProperty("dbName");
			String dbType = props.getProperty("dbType");
			
			initDB(user, password, host, dbName, dbType);
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}
	
	public void setProjID(int projID)
	{
		this.projID = projID;
	}
	
	public void run()
	{	
		try {
			Statement stmt = conn.createStatement();
			PreparedStatement pstmtDelete = conn.prepareStatement("delete from " + schema  + "annotation where document_id = ? and start = ? and " + rq + "end" + rq + " = ? "
				+ "and provenance = '##auto'");
			ResultSet rs = stmt.executeQuery("select document_id, start, " + rq + "end" + rq + ", value from " + schema + "annotation "
				+ "where provenance = '##auto' and document_id in "
				+ "(select a.document_id from " + schema + "frame_instance_document a, " + schema + "project_frame_instance b where a.frame_instance_id = b.frame_instance_id and "
				+ "b.project_id = " + projID + ") "
				+ "order by document_id, start");
			
			Map<Long, List<List<Long>>> docMap = new HashMap<Long, List<List<Long>>>();
			while (rs.next()) {
				long docID = rs.getLong(1);
				long start = rs.getLong(2);
				long end = rs.getLong(3);
				String value = rs.getString(4);
				
				List<List<Long>> indexes = docMap.get(docID);
				if (indexes == null) {
					indexes = getIndexes(docID);
					docMap.put(docID, indexes);
				}
				
				List<Long> startList = indexes.get(0);
				List<Long> endList = indexes.get(1);
				
				for (int i=0; i<endList.size(); i++) {
					long endIndex = endList.get(i);
					if (endIndex >= end) {
						long startIndex = startList.get(i);
						if (start >= startIndex) {
							pstmtDelete.setLong(1, docID);
							pstmtDelete.setLong(2, start);
							pstmtDelete.setLong(3, end);
							pstmtDelete.execute();
							
							System.out.println("removed: " + docID + ", " + start + ", " + end + ", " + value);
							break;
						}
					}
				}
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	private List<List<Long>> getIndexes(long docID) throws SQLException
	{
		List<List<Long>> indexes = new ArrayList<List<Long>>();
		List<Long> startList = new ArrayList<Long>();
		List<Long> endList = new ArrayList<Long>();
		indexes.add(startList);
		indexes.add(endList);
		
		Statement stmt = conn.createStatement();
		
		ResultSet rs = stmt.executeQuery("select start, " + rq + "end" + rq + " from " + schema + "annotation where annotation_type = 'SentenceDuplicate' and document_id = " + docID
			+ " order by start");
		while (rs.next()) {
			long start = rs.getLong(1);
			long end = rs.getLong(2);
			
			startList.add(start);
			endList.add(end);
		}
		
		return indexes;
		
	}
	
	public static void main(String[] args)
	{
		if (args.length != 4) {
			System.out.println("usage: user password config projID");
			System.exit(0);
		}
		
		RemoveDupSentAnnots remove = new RemoveDupSentAnnots();
		remove.init(args[0], args[1], args[2]);
		remove.setProjID(Integer.parseInt(args[3]));
		remove.run();
	}
}
