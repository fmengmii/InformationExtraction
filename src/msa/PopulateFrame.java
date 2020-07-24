package msa;

import java.sql.*;
import java.util.*;

import utils.db.DBConnection;

public class PopulateFrame
{
	private Connection conn;
	private Connection conn2;
	private PreparedStatement pstmtInsert;
	private PreparedStatement pstmtDelete;
	private PreparedStatement pstmtCountDelete;
	private PreparedStatement pstmtCheckElemRepeat;
	private PreparedStatement pstmtInsertElemRepeat;
	private PreparedStatement pstmtUpdateElemRepeat;
	private PreparedStatement pstmtDeleteElemRepeat;
	private PreparedStatement pstmtCheckFrameInstanceLocked;
	private PreparedStatement pstmtLockFrameInstance;
	private PreparedStatement pstmtDeleteFrameInstanceLock;
	private String schema;
	private String rq;
	private String provenance;
	
	public PopulateFrame()
	{
	}
	
	public void init(String user, String password, String host, String dbName, String dbType, String schema, String provenance, String rq)
	{
		this.schema = schema + ".";
		this.provenance = provenance;
		this.rq = rq;
		
		try {
			conn = DBConnection.dbConnection(user, password, host, dbName, dbType);
			conn2 = DBConnection.dbConnection(user, password, host, dbName, dbType);
			pstmtInsert = conn2.prepareStatement("insert into " + this.schema + "frame_instance_data (frame_instance_id, slot_id, value, section_slot_number, element_slot_number, document_namespace, "
				+ "document_table, document_id, annotation_id, provenance, element_id, v_scroll_pos, scroll_height, scroll_width) values (?,?,?,?,?,?,?,?,?,?,?,null,null,null)");
			pstmtDelete = conn.prepareStatement("delete from " + this.schema + "frame_instance_data where frame_instance_id = ?");
			//pstmtCountDelete = conn.prepareStatement("select element_id, count(*) from " + this.schema + "frame_instance_data where frame_instance_id = ? and provenance = ? group by element_id");
			
			pstmtCheckElemRepeat = conn.prepareStatement("select repeat_num from " + this.schema + "frame_instance_element_repeat where frame_instance_id = ? and element_id = ? and section_slot_num = ?");
			pstmtInsertElemRepeat = conn.prepareStatement("insert into " + this.schema + "frame_instance_element_repeat (frame_instance_id, element_id, section_slot_num, repeat_num) "
				+ "values (?,?,?,?)");
			pstmtUpdateElemRepeat = conn.prepareStatement("update " + this.schema + "frame_instance_element_repeat set repeat_num = ? where frame_instance_id = ? and "
				+ "element_id = ?");
			pstmtDeleteElemRepeat = conn.prepareStatement("delete from " + this.schema + "frame_instance_element_repeat where frame_instance_id = ?");
			pstmtCheckFrameInstanceLocked = conn.prepareStatement("select count(*) from " + this.schema + "frame_instance_lock where frame_instance_id = ?");
			pstmtLockFrameInstance = conn.prepareStatement("insert into " + this.schema + "frame_instance_lock values (?,'##auto',CURRENT_TIMESTAMP)");
			pstmtDeleteFrameInstanceLock = conn.prepareStatement("delete from " + this.schema + "frame_instance_lock where frame_instance_id = ?");
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	public void populate(int projID)
	{
		try {
			
			Statement stmt = conn.createStatement();
			
			conn2.setAutoCommit(false);
			
			/*
			String queryStr = "delete from " + schema + "frame_instance_data where (document_id, annotation_id) in (select distinct a.document_id, a.id from " + schema + "annotation a," + schema + "document_status b where a.provenance = '" + provenance + "'"
				+ " and b.status = 0 and a.document_id = b.document_id)";
			if (DBConnection.dbType.startsWith("sqlserver")) {
				queryStr = "delete from " + schema + "frame_instance_data where exists "
					+ "(select b.* from " + schema + "annotation b where b.provenance = '" + provenance + "' and " + schema + "frame_instance_data.document_id = b.document_id and "
					+ schema + "frame_instance_data.annotation_id = b.id)";
			}
			
			stmt.execute(queryStr);
			*/
			
			String queryStr ="select a.id, a.document_namespace, a.document_table, a.document_id, a.value, a.annotation_type, a.start, a.provenance, b.frame_instance_id "
				+ "from " + schema + "annotation a, " + schema + "frame_instance_document b, " + schema + "document_status c "
				+ " where (a.provenance = '" + provenance + "' or a.provenance = 'validation-tool') and (c.status = 0 or c.status = 1 or c.status = -4) and c.document_id = a.document_id and "
				+ "b.document_id = a.document_id and "
				+ "a.document_id in "
				+ "(select d.document_id from " + schema + "frame_instance_document d, " + schema + "project_frame_instance e "
				+ "where e.project_id = " + projID + " and d.frame_instance_id = e.frame_instance_id) order by a.document_id, a.start";
			
			//queryStr ="select id, document_namespace, document_table, document_id, value, annotation_type from " + schema + "annotation where provenance = '" + provenance + "' order by document_id, start";
			
			/*
			if (DBConnection.dbType.startsWith("sqlserver")) {
				queryStr = "select a.id, a.document_namespace, a.document_table, a.document_id, a.value, a.annotation_type from " + schema + "annotation a where provenance = '" + provenance + "' and not exists "
					+ "(select b.* from " + schema + "frame_instance_data b where "
					+ "a.document_namespace = b.document_namespace and a.document_table = b.document_table and a.document_id = b.document_id and "
					+ "a.id = b.annotation_id)";
			}
			*/
			
			
			PreparedStatement pstmtDeleteAnnot = conn.prepareStatement("delete from " + schema + "annotation where provenance = 'validation-tool' and "
				+ "document_id = ? and start = ? and annotation_type = ? and id = ?");
			
			
			ResultSet rs = stmt.executeQuery("select a.frame_instance_id from " + schema + "frame_instance_status a, " + schema + "project_frame_instance b "
				+ "where (a.status = 0 or a.status = 1 or a.status = -4) and b.project_id = " + projID + " and a.frame_instance_id = b.frame_instance_id");
			
			
			conn.setAutoCommit(false);
			while (rs.next()) {
				int frameInstanceID = rs.getInt(1);
				pstmtDelete.setInt(1, frameInstanceID);
				//pstmtDelete.execute();
				pstmtDelete.addBatch();
				
				//System.out.println(pstmtDelete.toString());
				
				pstmtDeleteElemRepeat.setInt(1, frameInstanceID);
				//pstmtDeleteElemRepeat.execute();
				pstmtDeleteElemRepeat.addBatch();
			}
			
			pstmtDelete.executeBatch();
			pstmtDeleteElemRepeat.executeBatch();
			conn.commit();
			conn.setAutoCommit(true);
			
			
			rs = stmt.executeQuery("select a.document_id, a.start "
				+ "from " + schema + "annotation a, " + schema + "document_status b "
				+ "where a.provenance = 'validation-tool' and (b.status = 0 or b.status = 1 or b.status = -4) and b.document_id = a.document_id and "
				+ "a.document_id in "
				+ "(select d.document_id from " + schema + "frame_instance_document d, " + schema + "project_frame_instance e "
				+ "where e.project_id = " + projID + " and d.frame_instance_id = e.frame_instance_id) order by a.document_id, a.start");
			
			Map<String, Integer> userDefinedMap = new HashMap<String, Integer>();
			while (rs.next()) {
				long docID = rs.getLong(1);
				int start = rs.getInt(2);
				
				userDefinedMap.put(docID + "|" + start, 0);
			}
			
			
			
			
			Map<String, Integer> map = new HashMap<String, Integer>();
			Map<Integer, Integer> sectionSlotMap = new HashMap<Integer, Integer>();
			Map<String, Boolean> usedMap = new HashMap<String, Boolean>();
						
			rs = stmt.executeQuery(queryStr);
			
			
			
			//conn.setAutoCommit(false);
			
			while (rs.next()) {
				int annotID = rs.getInt(1);
				String docNamespace = rs.getString(2);
				String docTable = rs.getString(3);
				long docID = rs.getLong(4);
				String value = rs.getString(5);
				String annotType = rs.getString(6);
				int start = rs.getInt(7);
				String provenance2 = rs.getString(8);
				int frameInstanceID = rs.getInt(9);
				
				
				if (userDefinedMap.get(docID + "|" + start) != null && provenance2.equals(provenance)) {
					continue;
				}
				
				
				Integer userDefinedCount = userDefinedMap.get(docID + "|" + start);
				
				if (userDefinedCount != null && (userDefinedCount > 0 || !provenance2.equals("validation-tool"))) {
				
					//duplicate user-defined annotation
					pstmtDeleteAnnot.setLong(1, docID);
					pstmtDeleteAnnot.setInt(2, start);
					pstmtDeleteAnnot.setString(3, annotType);
					pstmtDeleteAnnot.setInt(4, annotID);
					pstmtDeleteAnnot.execute();
					continue;
				}
				else if (userDefinedCount == null && provenance2.equals("validation-tool")) {
					userDefinedMap.put(docID + "|" + start, 1);
				}
				
				if (usedMap.get(docID + "|" + start) != null)
					continue;
				else
					usedMap.put(docID + "|" + start, true);

				
				//int frameInstanceID = getFrameInstanceID(docID);
				
				//System.out.println("docID: " + docID);
				
				/*
				if (frameInstanceID != currFrameInstanceID) {
					currFrameInstanceID = frameInstanceID;
				
					//lock frame instance in one transaction
					conn.setAutoCommit(false);
					pstmtCheckFrameInstanceLocked.setInt(1, frameInstanceID);
					
					int count = 0;
					ResultSet rs2 = pstmtCheckFrameInstanceLocked.executeQuery();
					if (rs2.next()) {
						count = rs2.getInt(1);
					}
					
					//skip over because locked
					if (count > 0)
						continue;
					
					pstmtLockFrameInstance.setInt(1, frameInstanceID);
					pstmtLockFrameInstance.execute();
					
					conn.commit();
					conn.setAutoCommit(true);
				}
				*/				

				
				int[] ans = getElementSlotID(annotType);
				String key = frameInstanceID + "|" + ans[0] + "|" + ans[1];
				
				Integer sectionSlotNum = 0;
				if (ans[3] == 1) {
					sectionSlotNum = sectionSlotMap.get(ans[2]);
					if (sectionSlotNum == null)
						sectionSlotNum = -1;
					
					sectionSlotMap.put(ans[2], ++sectionSlotNum);
				}
				
				
				Integer elemSlotNum = map.get(key);
				if (elemSlotNum == null) {
					elemSlotNum = -1;
				}
				
				map.put(key, ++elemSlotNum);

				
				pstmtInsert.setInt(1, frameInstanceID);
				pstmtInsert.setInt(2, ans[1]);
				pstmtInsert.setString(3, value);
				pstmtInsert.setInt(4, sectionSlotNum);
				pstmtInsert.setInt(5, elemSlotNum);
				pstmtInsert.setString(6, docNamespace);
				pstmtInsert.setString(7, docTable);
				pstmtInsert.setLong(8, docID);
				pstmtInsert.setInt(9, annotID);
				pstmtInsert.setString(10, provenance2);
				pstmtInsert.setInt(11, ans[0]);
				//pstmtInsert.execute();
				pstmtInsert.addBatch();
				
				
				//release lock
				//pstmtDeleteFrameInstanceLock.setInt(1, frameInstanceID);
				//pstmtDeleteFrameInstanceLock.execute();
			}
			
			pstmtInsert.executeBatch();
			conn2.commit();
			
			
			for (String key : map.keySet()) {
				int repeatNum = map.get(key) + 1;
				
								
				String[] parts = key.split("\\|");
				long frameInstanceID = Long.parseLong(parts[0]);
				int elemID = Integer.parseInt(parts[2]);
				
				int repeat = 0;
				pstmtCheckElemRepeat.setLong(1, frameInstanceID);
				pstmtCheckElemRepeat.setInt(2, elemID);
				pstmtCheckElemRepeat.setInt(3, 0);
				ResultSet rs2 = pstmtCheckElemRepeat.executeQuery();
				if (rs2.next()) {
					repeat = rs2.getInt(1);
				}
				
				if (repeat > 0) {
					pstmtUpdateElemRepeat.setInt(1, repeatNum + repeat);
					pstmtUpdateElemRepeat.setLong(2, frameInstanceID);
					pstmtUpdateElemRepeat.setInt(3, elemID);
					pstmtUpdateElemRepeat.execute();
				}
				else {
					pstmtInsertElemRepeat.setLong(1, frameInstanceID);
					pstmtInsertElemRepeat.setInt(2, elemID);
					pstmtInsertElemRepeat.setInt(3, 0);
					pstmtInsertElemRepeat.setInt(4, repeatNum);
					pstmtInsertElemRepeat.execute();
				}
				
				System.out.println(key + ": " + repeatNum + repeat);
			}
			
			
			
			
			stmt.close();
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	private int getFrameInstanceID(long docID)
	{
		int frameInstanceID = -1;
		
		try {
			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery("select distinct frame_instance_id from " + schema + "frame_instance_document where document_id = " + docID);
			if (rs.next())
				frameInstanceID = rs.getInt(1);
			
			stmt.close();
		}
		catch(SQLException e)
		{
			e.printStackTrace();
		}
		
		return frameInstanceID;
	}
	
	private int[] getElementSlotID(String annotType)
	{
		int[] ans = new int[4];
		
		try {
			int slotID = -1;
			int elementID = -1;
			int sectionID = -1;
			
			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery("select slot_id from " + schema + "slot where annotation_type = '" + annotType + "'");
			if (rs.next())
				slotID = rs.getInt(1);
			
			
			rs = stmt.executeQuery("select a.element_id, c.section_id from " + schema + "element_value a, " + schema + "value b, " + schema + "element c where b.slot_id = " + slotID + " and a.value_id = b.value_id and a.element_id = c.element_id");
			if (rs.next()) {
				elementID = rs.getInt(1);
				sectionID = rs.getInt(2);
			}
			
			int repeat = 0;
			rs = stmt.executeQuery("select " + rq + "repeat" + rq + " from " + schema + "crf_section where section_id = " + sectionID);
			if (rs.next()) {
				repeat = rs.getInt(1);
			}
			
			ans[0] = elementID;
			ans[1] = slotID;
			ans[2] = sectionID;
			ans[3] = repeat;
			
			stmt.close();
		}
		catch(SQLException e)
		{
			e.printStackTrace();
		}
		
		return ans;
	}
	
	public static void main(String[] args)
	{
		if (args.length != 8) {
			System.out.println("usage: user password host dbName dbType schema provenance projID");
			System.exit(0);
		}
		
		try {
			Connection conn = DBConnection.dbConnection(args[0], args[1], args[2], args[3], args[4]);
			PopulateFrame pop = new PopulateFrame();
			pop.init(args[0], args[1], args[2], args[3], args[4], args[5], args[6], DBConnection.reservedQuote);
			pop.populate(Integer.parseInt(args[7]));
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
}
