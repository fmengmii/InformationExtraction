package msa.pipeline;

import java.sql.*;
import java.util.*;
import java.io.*;

import utils.db.DBConnection;

public class SkipDocsGrayedOut extends MSAModule
{
	private List<String> grayAnnotTypeList;
	private String docQuery;
	private String schema;
	private String projName;

	public SkipDocsGrayedOut()
	{
		
	}
	
	public void init(String user, String password, String config)
	{
		try {
			Properties props = new Properties();
			props.load(new FileReader(config));
			String host = props.getProperty("host");
			String dbName = props.getProperty("dbName");
			String dbType = props.getProperty("dbType");
			String grayAnnotTypeListStr = props.getProperty("grayAnnotTypeList");
			grayAnnotTypeList = new ArrayList<String>();
			grayAnnotTypeList = gson.fromJson(grayAnnotTypeListStr, grayAnnotTypeList.getClass());
			//docQuery = props.getProperty("docQuery");
			schema = props.getProperty("schema") + ".";
			projName = props.getProperty("projName");
			
			initDB(user, password, host, dbName, dbType);
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	public void run()
	{
		try {
			StringBuilder strBlder = new StringBuilder("(");
			for (String annotType: grayAnnotTypeList) {
				if (strBlder.length() > 1)
					strBlder.append(",");
				strBlder.append("'" + annotType + "'");
			}
			
			strBlder.append(")");
			
			Statement stmt = conn.createStatement();
			int projID = -1;
			ResultSet rs = stmt.executeQuery("select project_id from " + schema + "project where name = '" + projName + "'");
			if (rs.next()) {
				projID = rs.getInt(1);
			}
			
			System.out.println("Project ID: " + projID);
			
			PreparedStatement pstmtGrayAnnots = conn.prepareStatement("select start, " + rq + "end" + rq + ", annotation_type from " + schema + "annotation where document_id = ? and annotation_type in " 
				+ strBlder.toString() + " order by start");
			PreparedStatement pstmtPreloadValues = conn.prepareStatement("select distinct start, " + rq + "end" + rq + " from " + schema + "annotation where document_id = ? and value = ?");
			PreparedStatement pstmtPreloadAnnots = conn.prepareStatement("select distinct start, " + rq + "end" + rq + " from " + schema + "annotation where document_id = ? and annotation_type = ?");
			PreparedStatement pstmtUpdateDocDisabled = conn.prepareStatement("update " + schema + "frame_instance_document set disabled = 1 where document_id = ?");
			PreparedStatement pstmtLastEnd = conn.prepareStatement("select max( " + rq + "end" + rq + " ) from " + schema + "annotation where document_id = ? and annotation_type = 'Token'");

			
			//get project preload annotations
			List<String> preloadValueList = new ArrayList<String>();
			List<String> preloadAnnotList = new ArrayList<String>();
			rs = stmt.executeQuery("select value, type from " + schema + "project_preload where project_id = " + projID);
			while (rs.next()) {
				String val = rs.getString(1);
				int type = rs.getInt(2);
				
				System.out.println("Preload values: " + val + ", " + type);
				
				if (type == 1) {
					preloadValueList.add(val);
				}
				else
					preloadAnnotList.add(val);
			}
			
			boolean removed = false;
			
			rs = stmt.executeQuery("select distinct document_id from " + schema + "frame_instance_document where frame_instance_id in "
				+ "(select distinct a.frame_instance_id from " + schema + "project_frame_instance a where a.project_id = " + projID + ") order by document_id");
			
			while (rs.next()) {
				long docID = rs.getLong(1);
				System.out.println("DocID: " + docID);
				
				long currStart = Long.MAX_VALUE;
				long currEnd = 0;
				
				long lastEnd = -1;
				pstmtLastEnd.setLong(1, docID);
				ResultSet rs2 = pstmtLastEnd.executeQuery();
				if (rs2.next()) {
					lastEnd = rs2.getLong(1);
				}
				
				pstmtPreloadValues.setLong(1, docID);
				pstmtPreloadAnnots.setLong(1, docID);
				
				List<List<Long>> preloadList = new ArrayList<List<Long>>();
				for (String val : preloadValueList) {
					pstmtPreloadValues.setString(2, val);
					
					rs2 = pstmtPreloadValues.executeQuery();
					while (rs2.next()) {
						List<Long> indexes = new ArrayList<Long>();
						indexes.add(rs.getLong(1));
						indexes.add(rs.getLong(2));
						preloadList.add(indexes);
						
						System.out.println("Adding preload value: " + val + ", " + indexes.get(0) + ", " + indexes.get(1));
					}
				}
				
				for (String val : preloadAnnotList) {
					pstmtPreloadAnnots.setString(2, val);
					
					rs2 = pstmtPreloadAnnots.executeQuery();
					while (rs2.next()) {
						List<Long> indexes = new ArrayList<Long>();
						indexes.add(rs.getLong(1));
						indexes.add(rs.getLong(2));
						preloadList.add(indexes);
						
						System.out.println("Adding preload annot: " + val + ", " + indexes.get(0) + ", " + indexes.get(1));
					}
				}
				
				pstmtGrayAnnots.setLong(1, docID);
				rs2 = pstmtGrayAnnots.executeQuery();
				while (rs2.next()) {
					long start = rs2.getLong(1);
					long end = rs2.getLong(2);
					String annotType = rs2.getString(3);
					
					System.out.println("gray indexes: " + annotType + ", " + start + ", " + end);
					
					if (start <= currEnd) {
						if (start < currStart)
							currStart = start;
						currEnd = end;
					}
					
					
					if (preloadList.size() > 0) {
						for (int i=0; i<preloadList.size(); i++) {
							List<Long> indexes = preloadList.get(i);
							if (start <= indexes.get(0) && end <= indexes.get(1) && end > indexes.get(0)) {
								indexes.set(0, end);
							}
							if (start >= indexes.get(0) && end >= indexes.get(1) && start <= indexes.get(1)) {
								indexes.set(1, start);
							}
							if (start >= indexes.get(0) && end >= indexes.get(1) && start <= indexes.get(1)) {
								indexes.set(1, start);
							}
							
							if (indexes.get(0) <= indexes.get(1)) {
								System.out.println("removing: " + indexes.get(0) + ", " + indexes.get(1));
								preloadList.remove(i);
								i--;
								removed = true;
							}
						}
					}
				}
				
				if ((removed && preloadList.size() == 0) || (currStart == 0 && currEnd == lastEnd)) {
					System.out.println("Disabled!");
					pstmtUpdateDocDisabled.setLong(1, docID);
					pstmtUpdateDocDisabled.execute();
				}
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}

	public static void main(String[] args)
	{
		if (args.length != 3) {
			System.out.println("usage: user password config");
			System.exit(0);
		}
		
		SkipDocsGrayedOut skip = new SkipDocsGrayedOut();
		skip.init(args[0], args[1], args[2]);
		skip.run();
	}
}
