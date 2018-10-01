package msa;

import java.sql.*;
import java.util.*;

import utils.db.DBConnection;

public class PopulateFrame
{
	private Connection conn;
	private PreparedStatement pstmtInsert;
	private PreparedStatement pstmtCheckElemRepeat;
	private PreparedStatement pstmtInsertElemRepeat;
	private PreparedStatement pstmtUpdateElemRepeat;
	private String schema;
	private String rq;
	private String provenance;
	
	public PopulateFrame()
	{
	}
	
	public void init(Connection conn, String schema, String provenance, String rq)
	{
		this.schema = schema + ".";
		this.provenance = provenance;
		this.rq = rq;
		
		try {
			this.conn = conn;
			pstmtInsert = conn.prepareStatement("insert into " + this.schema + "frame_instance_data (frame_instance_id, slot_id, value, section_slot_number, element_slot_number, document_namespace, "
				+ "document_table, document_id, annotation_id, provenance, element_id, v_scroll_pos, scroll_height, scroll_width) values (?,?,?,?,?,?,?,?,?,'" + provenance + "',?,null,null,null)");
			
			pstmtCheckElemRepeat = conn.prepareStatement("select count(*) from " + this.schema + "frame_instance_element_repeat where frame_instance_id = ? and element_id = ? and section_slot_num = ?");
			pstmtInsertElemRepeat = conn.prepareStatement("insert into " + this.schema + "frame_instance_element_repeat (frame_instance_id, element_id, section_slot_num, repeat_num) values (?,?,?,?)");
			pstmtUpdateElemRepeat = conn.prepareStatement("update " + this.schema + "frame_instance_element_repeat set repeat_num = ? where frame_instance_id = ? and "
				+ "element_id = ?");
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	public void populate()
	{
		try {
			
			Statement stmt = conn.createStatement();
			
			String queryStr = "delete from " + schema + "frame_instance_data where (document_id, annotation_id) in (select distinct a.document_id, a.id from " + schema + "annotation a," + schema + "document_status b where a.provenance = '" + provenance + "'"
				+ "and )";
			if (DBConnection.dbType.startsWith("sqlserver")) {
				queryStr = "delete from " + schema + "frame_instance_data where exists "
					+ "(select b.* from " + schema + "annotation b where b.provenance = '" + provenance + "' and " + schema + "frame_instance_data.document_id = b.document_id and "
					+ schema + "frame_instance_data.annotation_id = b.id)";
			}
			
			stmt.execute(queryStr);
			
			queryStr ="select id, document_namespace, document_table, document_id, value, annotation_type from " + schema + "annotation where provenance = '" + provenance + "' and (document_namespace, document_table, document_id, start, annotation_type) not in "
					+ "(select distinct a.document_namespace, a.document_table, a.document_id, a.start, a.annotation_type from frame_instance_data a)";
			
			//queryStr ="select id, document_namespace, document_table, document_id, value, annotation_type from " + schema + "annotation where provenance = '" + provenance + "' order by document_id, start";
			
			if (DBConnection.dbType.startsWith("sqlserver")) {
				queryStr = "select a.id, a.document_namespace, a.document_table, a.document_id, a.value, a.annotation_type from " + schema + "annotation a where provenance = '" + provenance + "' and not exists "
					+ "(select b.* from " + schema + "frame_instance_data b where "
					+ "a.document_namespace = b.document_namespace and a.document_table = b.document_table and a.document_id = b.document_id and "
					+ "a.id = b.annotation_id)";
			}
			
			ResultSet rs = stmt.executeQuery(queryStr);
			Map<String, Integer> map = new HashMap<String, Integer>();
			Map<Integer, Integer> sectionSlotMap = new HashMap<Integer, Integer>();
			
			while (rs.next()) {
				int annotID = rs.getInt(1);
				String docNamespace = rs.getString(2);
				String docTable = rs.getString(3);
				long docID = rs.getLong(4);
				String value = rs.getString(5);
				String annotType = rs.getString(6);
				
				int frameInstanceID = getFrameInstanceID(docID);
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
				pstmtInsert.setInt(10, ans[0]);
				pstmtInsert.execute();
				
			}
			
			for (String key : map.keySet()) {
				int repeatNum = map.get(key) + 1;
				
				System.out.println(key + ": " + repeatNum);
				
				String[] parts = key.split("\\|");
				long frameInstanceID = Long.parseLong(parts[0]);
				int elemID = Integer.parseInt(parts[1]);
				
				int count = 0;
				pstmtCheckElemRepeat.setLong(1, frameInstanceID);
				pstmtCheckElemRepeat.setInt(2, elemID);
				pstmtCheckElemRepeat.setInt(3, 0);
				ResultSet rs2 = pstmtCheckElemRepeat.executeQuery();
				if (rs2.next()) {
					count = rs2.getInt(1);
				}
				
				if (count > 0) {
					pstmtUpdateElemRepeat.setInt(1, repeatNum);
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
		if (args.length != 6) {
			System.out.println("usage: user password host dbName dbType provenance");
			System.exit(0);
		}
		
		try {
			Connection conn = DBConnection.dbConnection(args[0], args[1], args[2], args[3], args[4]);
			PopulateFrame pop = new PopulateFrame();
			pop.init(conn, args[3], args[5], DBConnection.reservedQuote);
			pop.populate();
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
}
