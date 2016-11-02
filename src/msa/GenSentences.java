package msa;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.google.gson.Gson;

import msa.db.MSADBException;
import msa.db.MSADBInterface;
import nlputils.sequence.SequenceUtilities;

public class GenSentences
{
	//private BoundStatement bstmt;
	//private BoundStatement bstmt2;
	//private Session session;
	private boolean punct = false;
	private boolean verbose = false;
	private boolean requireTarget = false;
	private boolean maskTarget = false;
	private String tokenType = "Token";
	
	private Gson gson;
	private MSADBInterface db;
	
	private List<Map<String, Object>> annotNGramsFilterList;
	private Map<String, List<String>> annotFeatureMap;
	private Map<String, Map<String, Object>> annotFilterMap;
	private List<String> annotTypeNameList;
	
	private List<AnnotationSentence> sentList;
	private List<AnnotationSequence> seqList;
	private List<AnnotationSequence> posSeqList;
	private List<AnnotationSequence> negSeqList;
		
	private String orderBy;
	
	private List<Long> docIDList;
	
	private int minTargetSize;
	private int maxTargetSize;
	
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
	
	public void setProfileType(int profileType)
	{
		this.profileType = profileType;
	}
	
	public void setMaskTarget(boolean maskTarget)
	{
		this.maskTarget = maskTarget;
	}
	
	public void init(MSADBInterface db, List<Map<String, Object>> annotFilterList, String targetType, String targetProvenance)
	{
		this.db = db;
		
		annotTypeNameList = new ArrayList<String>();
		annotFilterMap = new HashMap<String, Map<String, Object>>();
		annotFeatureMap = new HashMap<String, List<String>>();
		
		
		for (Map<String, Object> map : annotFilterList) {
			String annotType = (String) map.get("annotType");
			List<String> features = (List<String>) map.get("features");
			annotFilterMap.put(annotType, map);
			annotFeatureMap.put(annotType, features);
		}
		
	}
	
	public void genSentences(String docNamespace, String docTable, String where, int limit)
	{
		try {
			StringBuilder docQueryStrBlder = new StringBuilder("select document_id from " + docTable);
			
			if (where != null) {
				docQueryStrBlder.append(" where " + where);
			}
			
			if (orderBy != null)
				docQueryStrBlder.append(" " + orderBy);
			
			if (limit >= 0)
				docQueryStrBlder.append(" limit " + limit);
			
			if (db instanceof msa.db.CassandraDBInterface)
				docQueryStrBlder.append(" allow filtering");
			
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
		negSeqList = new ArrayList<AnnotationSequence>();
		
		try {	
			for (long docID : docIDList) {
				//get doc name
				String docName = docNamespace + "-" + docTable + "-" + docID;
				
				System.out.println("\n\n");
				List<AnnotationSequence> docSeqList = db.getSentsInDoc(docNamespace, docTable, docID);
				if (docSeqList.size() == 0) {
					System.out.println("No sequences: " + docID);
				}
				
				int lastSentIndex = 0;

				for (AnnotationSequence seq : docSeqList) {
					//System.out.println("before get sent annots");
					List<Annotation> annotList = db.getSentAnnots(docNamespace, docTable, docID, lastSentIndex, seq.getEnd(), punct);
					if (annotList.size() == 0)
						continue;
					
					long seqStart = seq.getStart();
					lastSentIndex = seq.getEnd();
					
					/*
					for (int i=0; i<seqQ.size(); i++) {
						long maxEnd = maxEndList.get(i);
						if (maxEnd <= seqStart) {
							seqQ.remove(i);
							maxEndList.remove(i);
							i--;
						}
						else {
							AnnotationSequence seq2 = seqQ.get(i);
							seq2.append(seq);
						}
					}
					*/
					
					
					
					boolean hasTarget = false;
					long currMaxEnd = -1;
					//System.out.println("after get sent annots");
					for (Annotation annot : annotList) {
						//System.out.println(annot.toString());
						
						if (annot.getEnd() > currMaxEnd) {
							currMaxEnd = annot.getEnd();
						}
						
						String annotType = annot.getAnnotationType();
						
						Map<String, Object> annotFilter = annotFilterMap.get(annotType);
						if (annotFilter == null)
							continue;
						
						String prov = (String) annotFilter.get("provenance");
						
						if (prov != null && !annot.getProvenance().equals(prov))
							continue;
						
						List<String> featureList = annotFeatureMap.get(annotType);
						
						String targetStr = ":target";
						Boolean target = (Boolean) annotFilter.get("target");
						if (target == null)
							target = false;
						else
							targetStr = (String) annotFilter.get("targetStr");
						
						if (featureList == null && !target)
							continue;
						
						if (!maskTarget || !target)
							seq.addAnnotation(annot, featureList, target, targetStr);
						
						if (target)
							hasTarget = true;
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
		
		for (int i=0; i<seqList.size(); i++) {
			//AnnotationSequence seq = new AnnotationSequence();
			
			AnnotationSequence currSeq = seqList.get(i).clone();
			
			
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
			
			//maxEnd += contextSize;
			
			/*
			int minStart = i;
			for (int j=i-1; j>=0; j--) {
				AnnotationSequence prevSeq = seqList.get(j);
				
				if (prevSeq.getStart() <= start && start - prevSeq.getStart() > contextSize) {
					minStart = j;
					break;
				}
			}
			*/
			
			
			/*
			for (int j=minStart; j<i; j++) {
				seq.append(seqList.get(j));
			}
			
			seq.removeAnnotType(":target");
			
			seq.append(currSeq);
			seq.setDocID(currSeq.getDocID());
			*/
			
			
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
			
			
			
		
			if (requireTarget && currSeq.getAnnotList(":target") != null) {
				//System.out.println("has target!");
				posSeqList.add(currSeq);
			}
			else
				negSeqList.add(currSeq);
			
			//System.out.println("i=" + i + ", minStart=" + minStart + ", endIndex=" + endIndex);
		}
		
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
}
