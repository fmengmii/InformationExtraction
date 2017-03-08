package msa;

import java.sql.*;

import utils.db.DBConnection;

public class PopulateFrame
{
	private Connection conn;
	private PreparedStatement pstmtInsert;
	
	public PopulateFrame()
	{
	}
	
	public void init(Connection conn)
	{
		try {
			this.conn = conn;
			pstmtInsert = conn.prepareStatement("insert into frame_instance_data (frame_instance_id, slot_id, value, section_slot_number, element_slot_number, document_namespace, "
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
			ResultSet rs = stmt.executeQuery("select id, document_namespace, document_table, document_id, value, annotation_type from annotation where provenance = 'msa-ie' and (document_namespace, document_table, document_id, id) not in "
					+ "(select distinct a.document_namespace, a.document_table, a.document_id, a.annotation_id from frame_instance_data a)");
			while (rs.next()) {
				int annotID = rs.getInt(1);
				String docNamespace = rs.getString(2);
				String docTable = rs.getString(3);
				long docID = rs.getLong(4);
				String value = rs.getString(5);
				String annotType = rs.getString(6);
				
				int frameInstanceID = getFrameInstanceID(docID);
				int[] ans = getElementSlotID(annotType);
				
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
			ResultSet rs = stmt.executeQuery("select distinct frame_instance_id from frame_instance_document where document_id = " + docID);
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
			ResultSet rs = stmt.executeQuery("select slot_id from slot where annotation_type = '" + annotType + "'");
			if (rs.next())
				slotID = rs.getInt(1);
			
			
			rs = stmt.executeQuery("select a.element_id from element_value a, value b where b.slot_id = " + slotID + " and a.value_id = b.value_id");
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
