package msa;

import java.io.*;
import java.sql.*;
import java.util.*;

import com.google.gson.Gson;

import utils.db.DBConnection;

public class EvalProfilesSimple
{
	private Connection conn;
	private Gson gson;

	public EvalProfilesSimple()
	{
		gson = new Gson();
	}
	
	public void eval(String user, String password, String config)
	{
		try {
			
			Properties props = new Properties();
			props.load(new FileReader(config));
			
			String host = props.getProperty("host");
			String dbName = props.getProperty("dbName");
			String dbType = props.getProperty("dbType");
			
			List<String> annotTypeList = new ArrayList<String>();
			annotTypeList = gson.fromJson(props.getProperty("annotTypeList"), annotTypeList.getClass());
			
			String targetType = props.getProperty("targetType");
			
			String provenance = props.getProperty("provenance");
			String ansProvenanceStr = props.getProperty("ansProvenanceList");
			List<String> ansProvenanceList = new ArrayList<String>();
			ansProvenanceList = gson.fromJson(ansProvenanceStr, ansProvenanceList.getClass());
			
			String docDBQuery = props.getProperty("docDBQuery");
			String annotQuery = props.getProperty("annotQuery");

			List<Long> docIDList = new ArrayList<Long>();
			
			conn = DBConnection.dbConnection(user, password, host, dbName, dbType);
			
			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(docDBQuery);
			while (rs.next()) {
				long docID = rs.getLong(1);
				docIDList.add(docID);
			}
			
			System.out.println("reading answers...");
			Map<String, String> ansMap = new HashMap<String, String>();
			Map<String, String> entityAnsMap = new HashMap<String, String>();
			
			for (int i=0; i<annotTypeList.size(); i++) {
				String annotType = annotTypeList.get(i);
				String ansProvenance = ansProvenanceList.get(i);
				for (long docID : docIDList)
					readAnswers(ansMap, entityAnsMap, annotType, docID, ansProvenance);
			}
			
			System.out.println("total answers: " + entityAnsMap.size());
			
			int tp = 0;
			int fp = 0;
			
			int total = 0;
			
			int etp = 0;
			int efp = 0;
			
			rs = stmt.executeQuery(annotQuery);
			
			long entityStart = -1;
			long lastEnd = -1;
			StringBuilder strBlder = new StringBuilder();
			int eTotal = entityAnsMap.size();
			long lastDocID = -1;
			
			while (rs.next()) {
				long docID = rs.getLong(1);
				long start = rs.getLong(2);
				long end = rs.getLong(3);
				String annotType = rs.getString(4);
				String value = rs.getString(5);
				
				String mapStr = ansMap.get(docID + "|" + start + "|" + end);
				if (mapStr != null) {
					Map<String, String> map = new HashMap<String, String>();
					map = gson.fromJson(mapStr, map.getClass());
					String annotType2 = map.get("annotType");
				
					if (annotType.equals(annotType2))
						tp++;
					else {
						System.out.println("FP wrong! docID:" + docID + " start:" + start + " end:" + end +" annotType:" + annotType2 + " value:" + value);
						fp++;
					}
				}
				//else if (annotType.equals(targetType)) {
				else {
					System.out.println("FP not found! docID:" + docID + " start:" + start + " end:" + end +" annotType:" + annotType + " value:" + value);
					fp++;
				}
				
				if (start > lastEnd + 1 || docID != lastDocID) {
					if (entityStart > 0) {
						String entityValue = entityAnsMap.get(lastDocID + "|" + entityStart + "|" + lastEnd);
						if (entityValue != null) {
							etp++;
							System.out.println("entity found: " + lastDocID + "|" + entityStart + "|" + lastEnd + "|" + strBlder.toString());
							entityAnsMap.remove(lastDocID + "|" + entityStart + "|" + lastEnd);
						}
						else {
							efp++;
							System.out.println("entity FP: " + lastDocID + "|" + entityStart + "|" + lastEnd + "|" + strBlder.toString());
						}
					}
					
					/*
					if (docID != lastDocID)
						strBlder = new StringBuilder();
						*/
					
					strBlder = new StringBuilder();
					entityStart = start;
				}
				

				strBlder.append(value + " ");
				total++;
				lastEnd = end;
				lastDocID = docID;
			}
			
			int fn = ansMap.size() - tp;
			
			double prec = ((double) tp) / ((double) (tp + fp));
			double recall = ((double) tp) / ((double) (tp + fn));
			
			System.out.println("tp=" + tp + " fp=" + fp + " fn=" + fn + " total=" + ansMap.size() + " total annots=" + total);
			System.out.println("prec=" + prec + " recall=" + recall);
			
			int efn = eTotal - etp;
			
			for (String key : entityAnsMap.keySet()) {
				String value = entityAnsMap.get(key);
				System.out.println("entity FN: " + key + "|" + value);
			}
			
			prec = ((double) etp) / ((double) (etp + efp));
			recall = ((double) etp) / ((double) (etp + efn));
			double efscore = 2.0 * ((prec * recall) / (prec + recall));
			System.out.println("etp=" + etp + " efp=" + efp + " efn=" + efn + " etotal=" + entityAnsMap.size());
			System.out.println("eprec=" + prec + " erecall=" + recall + " efscore=" + efscore);
			
			
			conn.close();
			
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	private void readAnswers(Map<String, String> ansMap, Map<String, String> entityAnsMap, String annotType, long docID, String provenance) throws SQLException
	{
		Statement stmt = conn.createStatement();
		//PreparedStatement pstmt = conn.prepareStatement("select start, end from annotation where document_id = ? and annotation_type = 'Token' and start >= ? and end <= ? order by start");
		
		String annotType2 = "B" + annotType.substring(1);
		ResultSet rs = stmt.executeQuery("select start, end, value from annotation "
			+ "where document_id = " + docID + " and provenance = '" + provenance + "' and (annotation_type = '" + annotType + "' or annotation_type = '" + annotType2 + "') order by start");
		
		//ResultSet rs = stmt.executeQuery("select start, end, value, annotation_type from annotation "
		//	+ "where document_id = " + docID + " and provenance = 'conll2003-token' and annotation_type != 'O' order by start");
				
		
		int entityStart = -1;
		int lastEnd = -1;
		StringBuilder strBlder = new StringBuilder();
		
		while (rs.next()) {
			//long docID = rs.getLong(1);
			int start = rs.getInt(1);
			int end = rs.getInt(2);
			String value = rs.getString(3);
			//String annotType3 = rs.getString(4);
			
			Map<String, String> map = new HashMap<String, String>();
			map.put("annotType", annotType);
			map.put("value", value);
			
			
			ansMap.put(docID + "|" + start + "|" + end, gson.toJson(map));
			

			if (start > lastEnd + 1) {
				if (entityStart > 0) {
					entityAnsMap.put(docID + "|" + entityStart + "|" + lastEnd, strBlder.toString());
					System.out.println(docID + "|" + entityStart + "|" + lastEnd + "|" + strBlder.toString());
					strBlder = new StringBuilder();
				}
				

				entityStart = start;
			}
			
			strBlder.append(value + " ");
			lastEnd = end;
		}
		
		if (entityStart > 0) {
			entityAnsMap.put(docID + "|" + entityStart + "|" + lastEnd, strBlder.toString());
			System.out.println(docID + "|" + entityStart + "|" + lastEnd + "|" + strBlder.toString());
		}
		
		stmt.close();
		//pstmt.close();
	}
	
	
	public void processOverlapping() throws SQLException
	{
		Map<String, Double> profileScoreMap = new HashMap<String, Double>();
		String[] annotTypes = {"I-PER", "I-ORG", "I-LOC"};
		String provenance = "conll2003";
		
		StringBuilder strBlder = new StringBuilder("(" + annotTypes[0]);
		
		for (int i=1; i<annotTypes.length; i++) {
			strBlder.append("or annotation_type = '" + annotTypes[i] + "'");
		}
		
		strBlder.append(")");
		
		Statement stmt = conn.createStatement();
		
		ResultSet rs = stmt.executeQuery("select document_id, start, annotation_type, features from annotation where provenance = '" + provenance + "' and " + strBlder.toString());
		while (rs.next()) {
			
		}
		
	}
	
	
	public void overlapping(String user, String password, String config) throws SQLException, FileNotFoundException, IOException, ClassNotFoundException
	{
		Properties props = new Properties();
		props.load(new FileReader(config));
		String host = props.getProperty("host");
		String dbName = props.getProperty("dbName");
		String dbType = props.getProperty("dbType");
		conn = DBConnection.dbConnection(user, password, host, dbName, dbType);
		
		
		/*
		pstmtAnnotID = conn.prepareStatement("select max(id) from annotation where document_namespace = 'ner' and document_table = 'conll2003_document' "
				+ "and document_id = ?");
		addSingleEntities("##auto-per2", "I-PER", "prob_per_entity");
		addSingleEntities("##auto-org2", "I-ORG", "prob_org_entity");
		addSingleEntities("##auto-loc2", "I-LOC", "prob_loc_entity");
		*/
		
		
		//removeLowProb("I-PER", "##auto-per2");

		
		
		String[] annotTypeList = {"I-PER", "I-ORG", "I-LOC"};
		String[] provList = {"##auto-per2", "##auto-org2", "##auto-loc2"};
		
		StringBuilder strBlder = new StringBuilder("(");
		for (int i=0; i<annotTypeList.length; i++) {
			if (i>0)
				strBlder.append(" or ");
			strBlder.append("(annotation_type = '" + annotTypeList[i] + "' and provenance = '" + provList[i] + "')");
		}
		
		strBlder.append(")");
		
		Map<String, Double> profileScoreMap = new HashMap<String, Double>();
		Map<String, String> provMap = new HashMap<String, String>();
		Map<String, Map<String, Double>> scoreMap = new HashMap<String, Map<String, Double>>();
		Map<String, Boolean> multiMap = new HashMap<String, Boolean>();
		Map<String, Double> valScoreMap = new HashMap<String, Double>();

		PreparedStatement pstmt = conn.prepareStatement("delete from annotation where document_id = ? and start = ? and provenance = ?");
		
		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery("select document_id, start, provenance, features, value from annotation where "
			+ strBlder.toString());
		
		while(rs.next()) {
			long docID = rs.getLong(1);
			long start = rs.getLong(2);
			String provenance = rs.getString(3);
			String features = rs.getString(4);
			String value = rs.getString(5).toLowerCase();
			
			String key = docID + "|" + start;
			
			Double profileScore = 0.0;
			Double targetScore = 0.0;
			
			if (features != null) {
			
				Map<String, Object> featureMap = new HashMap<String, Object>();
				featureMap = gson.fromJson(features, featureMap.getClass());
				
				
				int profileID = ((Double) featureMap.get("profileID")).intValue();
				Double targetIDDouble = (Double) featureMap.get("targetID");
				
				int targetID = -1;
				if (targetIDDouble != null)
					targetID = targetIDDouble.intValue();
				
				profileScore = profileScoreMap.get(profileID + "|" + provenance);
				double totalScore = 0.0;
				if (profileScore == null) {
					if (profileID == -1)
						profileScore = -1.0;
					else if (profileID == -2)
						profileScore = -2.0;
					else {
						profileScore = getProfileScore(profileID, provenance);
										
						profileScoreMap.put(profileID + "|" + provenance, profileScore);
					}
				}

				
				if (targetID >= 0) {
					targetScore = profileScoreMap.get(targetID + "|" + provenance);
					if (targetScore == null) {
						targetScore = getProfileScore(targetID, provenance);
						profileScoreMap.put(targetID + "|" + provenance, targetScore);
					}
				}
					
				totalScore = profileScore + targetScore;
				
				
				Map<String, Double> provScoreMap = scoreMap.get(key + "|" + value);
				if (provScoreMap == null) {
					provScoreMap = new HashMap<String, Double>();
					scoreMap.put(key + "|" + value, provScoreMap);
				}
				
				Double score = provScoreMap.get(provenance);
				if (score == null) {
					provScoreMap.put(provenance, totalScore);
				}
				
				score = valScoreMap.get(value + "|" + docID + "|" + provenance);
				if (score == null) {
					score = 0.0;
				}
				
				//System.out.println(key + "|" + value + "|" + provenance + "|" + score);
				
				if (totalScore > score)
					valScoreMap.put(value + "|" + docID + "|" + provenance, totalScore);
				
			}
		}
				
		for (String key : scoreMap.keySet()) {
			String[] parts = key.split("\\|");
			long docID = Long.parseLong(parts[0]);
			long start = Long.parseLong(parts[1]);
			String value = parts[2];
			
			Map<String, Double> provScoreMap = scoreMap.get(key);
			double max = -100.0;
			String maxProv = "";
			for (int i=0; i<provList.length; i++) {
				Double score = provScoreMap.get(provList[i]);
				if (score != null) {
					/*
					if (score == -2.0) {
						score = valScoreMap.get(value + "|" + docID + "|" + provList[i]);
						if (score == null)
							score = 0.0;
					}
					*/
					
					System.out.println("score: " + key + "|" + provList[i] + "|" + score);
					
					if (score > max) {
						max = score;
						maxProv = provList[i];
					}
				}
			}
			
			if (provScoreMap.size() > 1) {
				for (String prov : provScoreMap.keySet()) {
					if (!prov.equals(maxProv)) {
						pstmt.setLong(1, docID);
						pstmt.setLong(2, start);
						pstmt.setString(3, prov);
						pstmt.execute();
						
						System.out.println("deleting: " + key + "|" + value + "|" + prov);
					}
				}
			}
			
		}
		

		
		conn.close();
	}
	
	private double getProfileScore(int profileID, String provenance) throws SQLException
	{
		Statement stmt = conn.createStatement();
		
		
		double profileScore = 0.0;
		String profileStr = "";
		
		String suffix = "per";
		if (provenance.equals("##auto-org2"))
			suffix = "org";
		else if (provenance.equals("##auto-loc2"))
			suffix = "loc";
		
		ResultSet rs = stmt.executeQuery("select profile from profile_" + suffix + " where profile_id = " + profileID);
		if (rs.next()) {
			profileStr = rs.getString(1);
		}
		
		List<String> tokList = new ArrayList<String>();
		
		tokList = gson.fromJson(profileStr, tokList.getClass());
		
		double score = 0.0;
		for (String tok : tokList) {
			if (tok.indexOf(":i-") >= 0) {
				score += 10.0;
			}
			else if (tok.indexOf("string") >= 0 || tok.indexOf("root") >= 0) {
				score += 3.0;
			}
			else if (tok.indexOf("lookup") >= 0) {
				score += 2.0;
			}
			else if (tok.indexOf("category") >= 0) {
				score += 1.0;
			}
		}
		
		
		stmt.close();
		
		return score;
	}
	
	
	private void removeLowProb(String annotType, String provenance) throws SQLException
	{
		Statement stmt = conn.createStatement();
		PreparedStatement pstmt = conn.prepareStatement("select value from annotation where provenance = 'conll2003-token' and document_id < 1163 and value = ? and annotation_type = '" + annotType + "'");
		PreparedStatement pstmt2 = conn.prepareStatement("select value from annotation where provenance = 'conll2003-token' and document_id < 1163 and value = ? and annotation_type != '" + annotType + "'");
		PreparedStatement pstmt3 = conn.prepareStatement("delete from annotation where provenance = '" + provenance + "' and document_id = ? and start = ?");
		
		Map<String, Double> probMap = new HashMap<String, Double>();
		
		ResultSet rs = stmt.executeQuery("select document_id, start, value from annotation where provenance = '" + provenance + "'");
		while (rs.next()) {
			long docID = rs.getLong(1);
			long start = rs.getLong(2);
			String value = rs.getString(3);
			
			if (Character.isLowerCase(value.charAt(0))) {
				continue;
			}
			
			System.out.println("value: " + value);
			
			Double prob = probMap.get(value);
			if (prob == null) {
				pstmt.setString(1, value);
				ResultSet rs2 = pstmt.executeQuery();
				int count1 = 0;
				while (rs2.next()) {
					String value2 = rs2.getString(1);
					if (Character.isUpperCase(value2.charAt(0)))
						count1++;
				}
				
				int count2 = 0;
				pstmt2.setString(1, value);
				rs2 = pstmt2.executeQuery();
				while (rs2.next()) {
					String value2 = rs2.getString(1);
					if (Character.isUpperCase(value2.charAt(0)))
						count2++;
				}
				
				prob = ((double) count1) / ((double) count2);
				
				if (count1 + count2 < 5) {
					prob = -1.0;
				}
				
				probMap.put(docID + "|" + start, prob);
				
				if (prob == 0.0) {
					pstmt3.setLong(1, docID);
					pstmt3.setLong(2, start);
					
					System.out.println("deleting: " + docID + "|" + start + "|" + value);
					pstmt3.execute();
				}
			}
		}
	}
	
	
	public static void main(String[] args)
	{
		try {
			if (args.length != 4) {
				System.out.println("usage: user password config overlapping");
				System.exit(0);
			}
			
			EvalProfilesSimple eval = new EvalProfilesSimple();
			
			if (args[3].equals("y"))
				eval.overlapping(args[0], args[1], args[2]);
			else
				eval.eval(args[0], args[1], args[2]);
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
}
