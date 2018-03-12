package msa;

import java.sql.*;
import java.util.*;

import utils.db.DBConnection;

public class PopulateFrame
{
	private Connection conn;
	private PreparedStatement pstmtInsert;
	private String schema;
	
	public PopulateFrame()
	{
	}
	
	public void init(Connection conn, String schema)
	{
		this.schema = schema + ".";
		
		try {
			this.conn = conn;
			pstmtInsert = conn.prepareStatement("insert into " + this.schema + "frame_instance_data (frame_instance_id, slot_id, value, section_slot_number, element_slot_number, document_namespace, "
				+ "document_table, document_id, annotation_id, provenance, element_id, v_scroll_pos, scroll_height, scroll_width) values (?,?,?,0,0,?,?,?,?,'msa-ie',?,null,null,null)");
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
			
			String queryStr = "delete from " + schema + "frame_instance_data where (document_id, annotation_id) in (select distinct document_id, id from " + schema + "annotation where provenance = 'msa-ie')";
			if (DBConnection.dbType.startsWith("sqlserver")) {
				queryStr = "delete from " + schema + "frame_instance_data where exists "
					+ "(select b.* from " + schema + "annotation b where b.provenance = 'msa-ie' and " + schema + "frame_instance_data.document_id = b.document_id and "
					+ schema + "frame_instance_data.annotation_id = b.id)";
			}
			
			stmt.execute(queryStr);
			
			queryStr ="select id, document_namespace, document_table, document_id, value, annotation_type from " + schema + "annotation where provenance = 'msa-ie' and (document_namespace, document_table, document_id, id) not in "
					+ "(select distinct a.document_namespace, a.document_table, a.document_id, a.annotation_id from frame_instance_data a)";
			
			if (DBConnection.dbType.startsWith("sqlserver")) {
				queryStr = "select a.id, a.document_namespace, a.document_table, a.document_id, a.value, a.annotation_type from " + schema + "annotation a where provenance = 'msa-ie' and not exists "
					+ "(select b.* from " + schema + "frame_instance_data b where "
					+ "a.document_namespace = b.document_namespace and a.document_table = b.document_table and a.document_id = b.document_id and "
					+ "a.id = b.annotation_id)";
			}
			
			ResultSet rs = stmt.executeQuery(queryStr);
			Map<String, Boolean> map = new HashMap<String, Boolean>();
			
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
				
				Boolean flag = map.get(key);
				if (flag == null) {
					map.put(key, true);
				
					pstmtInsert.setInt(1, frameInstanceID);
					pstmtInsert.setInt(2, ans[1]);
					pstmtInsert.setString(3, value);
					pstmtInsert.setString(4, docNamespace);
					pstmtInsert.setString(5, docTable);
					pstmtInsert.setLong(6, docID);
					pstmtInsert.setInt(7, annotID);
					pstmtInsert.setInt(8, ans[0]);
					pstmtInsert.execute();
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
		int[] ans = new int[2];
		
		try {
			int slotID = -1;
			int elementID = -1;
			
			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery("select slot_id from " + schema + "slot where annotation_type = '" + annotType + "'");
			if (rs.next())
				slotID = rs.getInt(1);
			
			
			rs = stmt.executeQuery("select a.element_id from " + schema + "element_value a, " + schema + "value b where b.slot_id = " + slotID + " and a.value_id = b.value_id");
			if (rs.next())
				elementID = rs.getInt(1);
			
			ans[0] = elementID;
			ans[1] = slotID;
			
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
		PopulateFrame pop = new PopulateFrame();
		//pop.init();
		pop.populate();
	}
}
