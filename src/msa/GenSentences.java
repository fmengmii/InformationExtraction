package msa;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.gson.Gson;

import msa.db.MSADBException;
import msa.db.MSADBInterface;
import msa.db.MySQLDBInterface;
import nlputils.sequence.SequenceUtilities;

public class GenSentences
{
	//private BoundStatement bstmt;
	//private BoundStatement bstmt2;
	//private Session session;
	private boolean punct = false;
	private boolean verbose = false;
	private boolean requireTarget = false;
	private String tokenType = "Token";
	private String sentType = "Sentence";
	private String targetType;
	
	private Gson gson;
	private MSADBInterface db;
	
	private Map<String, List<String>> annotFeatureMap;
	private Map<String, Map<String, Object>> annotFilterMap;
	
	private List<AnnotationSentence> sentList;
	private List<AnnotationSequence> seqList;
	private List<AnnotationSequence> posSeqList;
	private List<AnnotationSequence> negSeqList;
	private Map<String, AnnotationSequence> seqMap;
		
	private String orderBy;
	
	private List<Long> docIDList;
	
	private boolean combineSents = false;
	
	
	public int contextSize = 10;
	
	private int profileType = 1;
	
	public GenSentences()
	{
		gson = new Gson();
	}
	
	public void setPunct(boolean punct)
	{
		this.punct = punct;
	}
	
	public void setVerbose(boolean verbose)
	{
		this.verbose = verbose;
	}
	
	public void setRequireTarget(boolean requireTarget)
	{
		this.requireTarget = requireTarget;
	}
	
	public List<AnnotationSentence> getSentList()
	{
		return sentList;
	}
	
	public List<AnnotationSequence> getSeqList()
	{
		return seqList;
	}
	
	public List<AnnotationSequence> getPosSeqList()
	{
		return posSeqList;
	}
	
	public List<AnnotationSequence> getNegSeqList()
	{
		return negSeqList;
	}
	
	public void setOrderBy(String orderBy)
	{
		this.orderBy = orderBy;
	}
	
	public void setDocIDList(List<Long> docIDList)
	{
		this.docIDList = docIDList;
	}
	
	public void setTokenType(String tokenType)
	{
		this.tokenType = tokenType;
	}
	
	public void setSentType(String sentType)
	{
		this.sentType = sentType;
	}
	
	public void setProfileType(int profileType)
	{
		this.profileType = profileType;
	}
	
	public Map<String, AnnotationSequence> getSeqMap()
	{
		return seqMap;
	}
	
	public MSADBInterface getDB()
	{
		return db;
	}
	
	public void setTarget(Map<String, Object> targetMap)
	{
		String annotType = (String) targetMap.get("annotType");
		//System.out.println("annotType: " + annotType);
		List<String> features = (List<String>) targetMap.get("features");
		annotFilterMap.put(annotType, targetMap);
		annotFeatureMap.put(annotType, features);
	}
	
	public void setCombineSents(boolean combineSents)
	{
		this.combineSents = combineSents;
	}
	
	public void init(MSADBInterface db, List<Map<String, Object>> annotFilterList, String targetType, String targetProvenance)
	{
		this.db = db;
		
		annotFilterMap = new HashMap<String, Map<String, Object>>();
		annotFeatureMap = new HashMap<String, List<String>>();
		
		
		for (Map<String, Object> map : annotFilterList) {
			String annotType = (String) map.get("annotType");
			//System.out.println("annotType: " + annotType);
			List<String> features = (List<String>) map.get("features");
			annotFilterMap.put(annotType, map);
			annotFeatureMap.put(annotType, features);
		}
		
		this.targetType = targetType;
		
		seqMap = new HashMap<String, AnnotationSequence>();
	}
	
	public void genSentences(String docNamespace, String docTable, String where, int limit)
	{
		try {
			StringBuilder docQueryStrBlder = new StringBuilder("select document_id from " + docNamespace + "." + docTable);
			
			if (where != null) {
				docQueryStrBlder.append(" where " + where);
			}
			
			if (orderBy != null)
				docQueryStrBlder.append(" " + orderBy);
			
			if (limit >= 0)
				docQueryStrBlder.append(" limit " + limit);
			
			docIDList = db.getDocIDList(docQueryStrBlder.toString());
			genSentenceAnnots(docNamespace, docTable);
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	public void genSentenceAnnots(String docNamespace, String docTable)
	{
		seqList = new ArrayList<AnnotationSequence>();
		sentList = new ArrayList<AnnotationSentence>();
		
		try {	
			for (long docID : docIDList) {
				//get doc name
				
				System.out.println("gensentannots: " + docID);
				
				System.out.println("\n\n");
				List<AnnotationSequence> docSeqList = db.getSentsInDoc(docNamespace, docTable, docID, targetType);
				
				if (docSeqList.size() == 0) {
					System.out.println("No sequences: " + docID);
				}
				
				int lastSentIndex = 0;

				for (AnnotationSequence seq : docSeqList) {
					
					//check to see if this sentence was cached
					AnnotationSequence seq2 = seqMap.get(docID + "|" + seq.getStart());
					if (seq2 != null) {
						//System.out.println("cached sentence!");
						seqList.add(seq2);
						continue;
					}
							
					//add to the cache
					String key = seq.getDocID() + "|" + seq.getStart();
					seqMap.put(key, seq);
					
					

					List<Annotation> annotList = db.getSentAnnots(docNamespace, docTable, docID, lastSentIndex, seq.getEnd(), punct);
					if (annotList.size() == 0)
						continue;
					
					long seqStart = seq.getStart();
					lastSentIndex = seq.getEnd();
					
					long currMaxEnd = -1;

					for (Annotation annot : annotList) {
						
						if (annot.getEnd() > currMaxEnd) {
							currMaxEnd = annot.getEnd();
						}
						
						String annotType = annot.getAnnotationType();
						//System.out.println("annotType: " + annotType);

						
						Map<String, Object> annotFilter = annotFilterMap.get(annotType);
						
						
						
						if (annotFilter == null)
							continue;
						
						String prov = (String) annotFilter.get("provenance");
						
						
						if (prov != null && annot.getProvenance() != null && !annot.getProvenance().equals(prov))
							continue;
						
						
						List<String> featureList = annotFeatureMap.get(annotType);
						
						/*
						String targetStr = ":target";
						Boolean target = (Boolean) annotFilter.get("target");
						if (featureList != null && (!requireTarget || target == null))
							target = false;
						else
							targetStr = (String) annotFilter.get("targetStr");
						
						if (featureList == null)
							target = true;
						*/
						
						boolean target = false;
						if (featureList == null)
							target = true;
						
						seq.addAnnotation(annot, featureList, target, annotType);
						
						
					}
					
					
					//set toks for current sequence
					List<String> toks = new ArrayList<String>();
					List<Annotation> tokAnnotList = seq.getAnnotList(tokenType);
					
					if (tokAnnotList == null)
						continue;
					
					for (Annotation annot : tokAnnotList) {
						toks.add(annot.getValue());
					}
					
					seq.setToks(toks);
					
					seqList.add(seq);
					
					if (verbose) {
						String annotListStr = SequenceUtilities.getStrFromToks(toks);
						System.out.println(annotListStr);
					}
				}
			}
			
			genExtractionSequences();
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	public void genExtractionSequences()
	{
		posSeqList = new ArrayList<AnnotationSequence>();
		negSeqList = new ArrayList<AnnotationSequence>();
		//seqMap = new HashMap<String, AnnotationSequence>();
		

		AnnotationSequence prevSeq = null;
		boolean prevFlag = false;
		for (int i=0; i<seqList.size(); i++) {
			//AnnotationSequence seq = new AnnotationSequence();
			
			
			AnnotationSequence currSeq = seqList.get(i).clone();
			
			String annotListStr = SequenceUtilities.getStrFromToks(currSeq.getToks());
			//System.out.println(annotListStr);

			
			
			long start = currSeq.getStart();
			long end = currSeq.getEnd();
			int[] focusCoords = new int[2];
			focusCoords[0] = (int) start;
			focusCoords[1] = (int) end;
			currSeq.setFocusCoords(focusCoords);

			
			
			long maxEnd = -1;
			Map<String, List<Annotation>> annotMap = currSeq.getAnnotMap();
			for (String annotType : annotMap.keySet()) {
				List<Annotation> annotList = annotMap.get(annotType);
				for (Annotation annot : annotList) {
					long endIndex = annot.getEnd();
					
					if (endIndex > maxEnd)
						maxEnd = endIndex;
				}
			}
			
			
			int endIndex = -1;
			AnnotationSequence endSeq = new AnnotationSequence();
			for (int j=i+1; j<seqList.size(); j++) {
				AnnotationSequence nextSeq = seqList.get(j);
				if (nextSeq.getStart() < maxEnd && nextSeq.getStart() >= start)
					endSeq.append(seqList.get(j));
				else {
					endIndex = j-1;
					break;
				}
			}
			
			if (profileType != 2)
				endSeq.removeAnnotType(":target");
			
			currSeq.append(endSeq);
			
			//seq.setFocusCoords(focusCoords);
			
			//add previous seq?
			boolean addPrev = false;

			if (combineSents) {
				if ((prevSeq != null && currSeq.getToks().size() < 10 && prevSeq.getDocID() == currSeq.getDocID()) || prevFlag) {
					//prevSeq = currSeq;
					AnnotationSequence seq2 = prevSeq.clone();
					seq2.append(currSeq);
					seq2.setEnd(currSeq.getEnd());
					prevSeq = currSeq;
					currSeq = seq2;
					
					if (currSeq.getToks().size() < 10)
						prevFlag = true;
					else
						prevFlag = false;
					
					addPrev = true;
				}
				else
					prevSeq = currSeq;
			}

			
			System.out.println("seq: " + SequenceUtilities.getStrFromToks(currSeq.getToks()));
			System.out.println("len: " + currSeq.getToks().size());
			
		
			if (requireTarget && currSeq.getAnnotList(":target") != null) {
				//System.out.println("has target!");
				posSeqList.add(currSeq);
			}
			else
				negSeqList.add(currSeq);
			
			if (addPrev) {
				if (requireTarget && prevSeq.getAnnotList(":target") != null) {
					//System.out.println("has target!");
					posSeqList.add(prevSeq);
				}
				else
					negSeqList.add(prevSeq);
			}
			
		}
		
		posSeqList = splitSents(posSeqList);
		negSeqList = splitSents(negSeqList);
		

		System.out.println("posSeqList:" + posSeqList.size() + " negSeqList: " + negSeqList.size());
	}
	
	public void addTargets(String targetType)
	{
		//this.targetType = targetType;
		
		for (AnnotationSequence seq : seqList) {
			
			//remove any existing targets
			seq.removeAnnotType(":target");
			
			Map<String, Object> annotFilter = annotFilterMap.get(targetType);
			List<Annotation> annotList = seq.getAnnotList(targetType);
			
			//System.out.println("targetType: " + targetType);
			
			if (annotList == null) {
				//System.out.println("annotlist null");
				continue;
			}
			
			System.out.println("annotList: " + annotList.size());
			
			for (Annotation annot : annotList) {
				String annotType = annot.getAnnotationType();
				
				if (annotType.equals(targetType)) {
					List<String> featureList = annotFeatureMap.get(annotType);
					
					String targetStr = ":target";
					//Boolean target = (Boolean) annotFilter.get("target");
					targetStr = (String) annotFilter.get("targetStr");
					
					seq.addAnnotation(annot, featureList, true, targetStr);
				}
			}
		}
		
		genExtractionSequences();
	}
	
	public void genTargetPhrases(String docNamespace, String docTable, String targetType, String targetProvenance) throws MSADBException
	{
		seqList = new ArrayList<AnnotationSequence>();
		
		for (long docID : docIDList) {
			List<Annotation> targetAnnotList = db.getAnnotations(docNamespace, docTable, docID, -1, -1, targetType, targetProvenance);
			for (Annotation annot : targetAnnotList) {
				long docID2 = annot.getDocID();
				long start = annot.getStart();
				long end = annot.getEnd();
				
				AnnotationSequence seq = new AnnotationSequence();
				seq.setDocID(docID);
				seq.setStart((int) start);
				seq.setEnd((int) end);
				List<Annotation> targetSubAnnotList = db.getAnnotations(docNamespace, docTable, docID2, start, end, null, null);
				for (Annotation subAnnot : targetSubAnnotList) {
					String annotType = subAnnot.getAnnotationType();
	
					List<String> features = annotFeatureMap.get(annotType);
					if (features == null)
						continue;
					
					seq.addAnnotation(subAnnot, features, false, ":target");
				}
				
				seqList.add(seq);
				
				if (verbose) {
					List<String> toks = new ArrayList<String>();
					List<Annotation> tokAnnotList = seq.getAnnotList(tokenType);
					for (Annotation annot2 : tokAnnotList) {
						toks.add(annot2.getValue());
					}
					
					System.out.println(SequenceUtilities.getStrFromToks(toks));
				}
			}
		}
	}
	
	public void removeDocID(long docID)
	{
		if (seqMap != null) {
			Set<String> keySet = new HashSet<String>(seqMap.keySet());
			for (String key : keySet) {
				String[] parts = key.split("\\|");
				long docID2 = Long.parseLong(parts[0]);
				if (docID == docID2) {
					seqMap.remove(key);
				}
			}
		}
	}
	
	
	private List<AnnotationSequence> splitSents(List<AnnotationSequence> seqList)
	{
		List<AnnotationSequence> seqList2 = new ArrayList<AnnotationSequence>();
		
		for (AnnotationSequence seq : seqList) {
			if (seq.getToks().size() > 20) {
				for (int i=0; i<seq.getToks().size(); i+=10) {
					AnnotationSequence seq2 = seq.subSequence(i, 20);
					seqList2.add(seq2);
				}
			}
		}
		
		
		return seqList2;
	}
}
