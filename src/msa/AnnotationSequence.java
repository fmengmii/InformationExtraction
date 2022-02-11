package msa;

import java.util.*;

import com.google.gson.Gson;

public class AnnotationSequence
{
	private Map<String, List<Annotation>> annotMap;
	private List<Annotation> annotList;
	private Gson gson;
	private List<Map<String, Object>> annotFilterList;
	private Map<String, List<Map<String, Object>>> annotFilterMap;
	private Map<String, Map<String, Object>> annotFilterNameMap;
	private long docID;
	private int sentID;
	private int start;
	private int end;
	private List<Integer> targetIndexList;
	private List<String> toks;
	
	private int currIndex;
	private List<List<Annotation>> annotMatrix;
	private int[] focusCoords;
	
	
	public AnnotationSequence()
	{
		gson = new Gson();
		annotMap = new HashMap<String, List<Annotation>>();
		annotFilterMap = new HashMap<String, List<Map<String, Object>>>();
		annotFilterNameMap = new HashMap<String, Map<String, Object>>();
		targetIndexList = new ArrayList<Integer>();
		toks = new ArrayList<String>();
	}
	
	/*
	public AnnotationSequence(List<Map<String, Object>> annotFilterList)
	{
		this();
		this.annotFilterList = annotFilterList;
		initAnnotFilterList();
	}
	
	public AnnotationSequence(Map<String, Object> annotFilter, List<Annotation> annotList)
	{
		this(annotFilterList);

		for (Annotation annot : annotList) {
			addAnnotation(annot, annotFilter);
		}
	}
	*/
	
	public AnnotationSequence(long docID, int sentID, int start, int end)
	{
		this();
		this.docID = docID;
		this.sentID = sentID;
		this.start = start;
		this.end = end;
	}
	
	public void setAnnotFilterList(List<Map<String, Object>> annotFilterList)
	{
		this.annotFilterList = annotFilterList;
		initAnnotFilterList();
	}
	
	public List<Annotation> getAnnotList()
	{
		return annotList;
	}
	
	public void setAnnotList(List<Annotation> annotList)
	{
		this.annotList = annotList;
	}
	
	public long getDocID()
	{
		return docID;
	}
	
	public void setDocID(long docID)
	{
		this.docID = docID;
	}
	
	public int getSentID()
	{
		return sentID;
	}
	
	public void setSentID(int sentID)
	{
		this.sentID = sentID;
	}
	
	public List<String> getToks()
	{
		return toks;
	}
	
	public void setToks(List<String> toks)
	{
		this.toks = toks;
	}
	
	public void addTok(String tok)
	{
		toks.add(tok);
	}
	
	public List<String> getToks(String annotType)
	{
		List<String> toks2 = new ArrayList<String>();
		List<Annotation> annotList = annotMap.get(annotType);
		for (Annotation annot : annotList) {
			toks2.add(annot.getLabel());
		}
		
		return toks2;
	}
	
	public List<List<String>> getTargetToks(boolean requireTarget)
	{
		
		List<List<String>> targetToksList = new ArrayList<List<String>>();
		
		if (targetIndexList.size() == 0 && !requireTarget) {
			targetToksList.add(toks);
		}
		else {
			for (int index : targetIndexList) {
				List<String> toks2 = copyToks();
				toks2.set(index, ":target");
				targetToksList.add(toks2);
			}
		}
		
		return targetToksList;
	}
	
	public int getStart()
	{
		return start;
	}
	
	public void setStart(int start)
	{
		this.start = start;
	}
	
	public int getEnd()
	{
		return end;
	}
	
	public void setEnd(int end)
	{
		this.end = end;
	}
	
	public List<Integer> getTargetIndexList()
	{
		return targetIndexList;
	}
	
	public List<Annotation> getAnnotList(String annotType)
	{
		return annotMap.get(annotType);
	}
	
	public Map<String, List<Annotation>> getAnnotMap()
	{
		return annotMap;
	}
	
	public Set<String> getAnnotTypes()
	{
		return annotMap.keySet();
	}
	
	public List<String> init()
	{
		currIndex = 0;
		genAnnotMatrix();
		
		return getCurrElements();
	}
	
	public void removeAnnotType(String annotType)
	{
		annotMap.remove(annotType);
	}
	
	public int[] getFocusCoords() {
		return focusCoords;
	}

	public void setFocusCoords(int[] focusCoords) {
		this.focusCoords = focusCoords;
	}
	
	public AnnotationSequence clone()
	{
		AnnotationSequence seq = new AnnotationSequence(this.docID, this.sentID, this.start, this.end);
		seq.annotFilterList = this.annotFilterList;
		
		for (int targetIndex : targetIndexList)
			seq.targetIndexList.add(targetIndex);
		
		for (String tok : toks)
			seq.toks.add(tok);
		
		for (String annotType : annotMap.keySet()) {
			List<Annotation> annotList = annotMap.get(annotType);
			List<Annotation> annotList2 = new ArrayList<Annotation>();
			
			seq.annotMap.put(annotType, annotList2);
			for (Annotation annot : annotList)
				annotList2.add(annot);
		}
		
		
		return seq;
	}
	
	
	public AnnotationSequence subSequence(int start, int len)
	{
		AnnotationSequence seq = new AnnotationSequence();
		seq.annotFilterList = this.annotFilterList;
		
		for (int i=start; i<start+len; i++) {
			seq.toks.add(this.toks.get(i));
		}
		
		List<Annotation> tokAnnots = this.annotMap.get("token|string");
		long begin = tokAnnots.get(0).getStart();
		long end = tokAnnots.get(tokAnnots.size()-1).getEnd();		
		
		for (String annotType : annotMap.keySet()) {
			List<Annotation> annotList = annotMap.get(annotType);
			List<Annotation> annotList2 = new ArrayList<Annotation>();
			
			seq.annotMap.put(annotType, annotList2);
			for (Annotation annot : annotList) {
				if (annot.getStart() >= begin && annot.getEnd() <= end)
					annotList2.add(annot);
			}
		}
		
		return seq;
	}
	
	
	public List<String> next(int index)
	{
		List<Annotation> annotList = annotMatrix.get(index);
		Annotation annot = annotList.get(currIndex);
		List<Annotation> toks = annotMatrix.get(0);

		for (int i=currIndex; i<toks.size(); i++) {
			Annotation annot2 = toks.get(i);
			if (annot2.getEnd() == annot.getEnd()) {
				currIndex = i;
				break;
			}
		}
		
		return getCurrElements();
	}
	
	public int maxSize()
	{
		return annotMap.get("Token").size();
	}
	
	public void addAnnotation(Annotation annot, List<String> features, boolean target, String targetStr)
	{
		//check for target
		if (target) {
			Annotation annot2 = new Annotation(-1, annot.getAnnotationType(), annot.getStart(), annot.getEnd(), 
					annot.getValue(), annot.getFeatureMap());
				//annot2.setLabel(":target");
				annot2.setLabel(targetStr);
				//List<Annotation> annotList = annotMap.get(":target");
				List<Annotation> annotList = annotMap.get(targetStr);
				if (annotList == null) {
					annotList = new ArrayList<Annotation>();
					//annotMap.put(":target", annotList);
					annotMap.put(targetStr, annotList);
				}
				
				annotList.add(annot2);
		}
		
		else {
			String annotType = annot.getAnnotationType().toLowerCase();
			
			for (String feature : features) {
				String featureName = ":" + annotType;
				
				Object featureValue = null;
				
				if (!feature.startsWith("$")) {
					featureName += "|" + feature;
				
					featureValue = annot.getFeature(feature);
					if (featureValue == null)
						continue;
				}
				
				featureName = featureName.toLowerCase();
	
				
				List<Annotation> annotList = annotMap.get(featureName);
				if (annotList == null) {
					annotList = new ArrayList<Annotation>();
					annotMap.put(featureName, annotList);
				}
				
				Annotation annot2 = new Annotation(-1, annot.getAnnotationType(), annot.getStart(), annot.getEnd(), 
					annot.getValue(), annot.getFeatureMap());
				
				String label = "";
				if (feature.equals("$literal"))
					label = ":" + annotType.toLowerCase();
				else {
					//label = annot.getAnnotationType().toLowerCase();
					label = featureName;
					if (!feature.equals("$annotTypeName"))
						label += "|" + ((String) featureValue).toLowerCase();

					
					//label = featureName + "|" + label;
				}
				
				
				
				label = label.replaceAll(" ", "-");
				annot2.setLabel(label);
				annotList.add(annot2);
			}
		}
	}
	
	public void genAnnotationList()
	{
		annotList = new ArrayList<Annotation>();
		
		for (Map<String, Object> annotFilter : annotFilterList)
		{
			String annotType = (String) annotFilter.get("annotTypeName");
			if (annotType == null)
				annotType = (String) annotFilter.get("annotType");
			List<Annotation> annotList = annotMap.get(annotType);
			if (annotList == null)
				continue;
			
			for (Annotation annot : annotList) {
				//System.out.println("insert: " + annot.getValue() + " type: " + annot.getAnnotationType() + " start: " + annot.getStart() + " end:" + annot.getEnd());
				insertAnnotation(annot);
			}
		}
		
		genToks();
	}
	
	private void initAnnotFilterList()
	{
		for (Map<String, Object> annotFilter : annotFilterList) {
			String annotType = (String) annotFilter.get("annotType");
			List<Map<String, Object>> filterList = annotFilterMap.get(annotType);
			if (filterList == null) {
				filterList = new ArrayList<Map<String, Object>>();
				annotFilterMap.put(annotType, filterList);
			}
			
			filterList.add(annotFilter);
			
			String annotTypeName = (String) annotFilter.get("annotTypeName");
			if (annotTypeName == null)
				annotTypeName = annotType;
			annotFilterNameMap.put(annotTypeName, annotFilter);
		}
	}
	
	public List<String> copyToks()
	{
		List<String> toks2 = new ArrayList<String>();
		for (String tok : toks)
			toks2.add(tok);
		
		return toks2;
	}
	
	private void genToks()
	{
		toks = new ArrayList<String>();
		targetIndexList = new ArrayList<Integer>();
		
		int index = 0;
		for (Annotation annot : annotList) {
			String annotType = annot.getAnnotationType();

			Map<String, Object> annotFilter = annotFilterNameMap.get(annotType);
			
			Boolean target = (Boolean) annotFilter.get("target");
			if (target != null && target) {
				targetIndexList.add(index);
			}
			
			//System.out.println(annotType);
			
			String label = ":" + annotType.toLowerCase();
			Object labelObj = annotFilter.get("label");
			if (labelObj instanceof Map) {
				Map<String, String> labelMap = (Map<String, String>) labelObj;
				String feature = labelMap.get("feature");
				if (feature != null)
					label = ((String) annot.getFeature(feature)).toLowerCase();
			}
			else if (labelObj instanceof String) {
				label = (String) labelObj;
				
				if (label.startsWith("$")) {
					if (label.endsWith("value")) {
						label = annot.getValue();
					}
				}
			}
			
			annot.setLabel(label);
			toks.add(label.replace(" ", "-"));
			index++;
		}
	}
	
	public void insertAnnotation(Annotation annot)
	{
		boolean inserted = false;
		boolean found = false;
		Annotation prev = null;
		for (int i=0; i<annotList.size(); i++) {
			Annotation annot2 = annotList.get(i);
			
			if (annot.getStart() == annot2.getStart() && annot.getEnd() == annot2.getEnd()) {
				found = true;
				break;
			}
			
			if (annot.getEnd() <= annot2.getEnd()) {
				
				found = true;
				
				if (annot.getEnd() <= annot2.getStart() && (prev == null || prev.getEnd() < annot2.getStart())) {
					annotList.add(i, annot);
					inserted = true;
				}
				
				break;
			}
			
			prev = annot2;
		}
		
		if (!inserted && !found) {
			annotList.add(annot);
		}
	}
	
	public void append(AnnotationSequence seq)
	{
		for (String annotType : seq.annotMap.keySet()) {
			List<Annotation> annotList2 = seq.annotMap.get(annotType);
			
			List<Annotation> annotList = this.annotMap.get(annotType);
			if (annotList == null) {
				annotList = new ArrayList<Annotation>();
				this.annotMap.put(annotType, annotList);
			}
			
			for (Annotation annot : annotList2) {
				annotList.add(annot);
			}
		}
		
		this.toks.addAll(seq.toks);
	}
	
	private boolean checkCondition(Map<String, Object> features, Map<String, Object> condMap)
	{
		String feature = (String) condMap.get("feature");
		Object featureObj = features.get(feature);
		
		List<String> containsList = (List<String>) condMap.get("contains");
		if (containsList != null) {
			List<String> featureValList = (List<String>) featureObj;
			
			for (String containsVal : containsList) {
				if (featureValList.contains(containsVal))
					return true;
			}
			
			return false;
		}
		
		String match = (String) condMap.get("match");
		if (match != null) {
			String featureVal = (String) featureObj;
			if (featureVal.equals(match))
				return true;
					
			return false;
		}
		
		List<String> matchList = (List<String>) condMap.get("matchlist");
		if (matchList != null) {
			String featureVal = (String) featureObj;
			if (matchList.contains(featureVal))
				return true;
					
			return false;
		}
		
		return true;
	}
	
	private void genAnnotMatrix()
	{
		annotMatrix = new ArrayList<List<Annotation>>();
		List<Annotation> toks = annotMap.get("Token");
		
		Map<Long, Integer> indexMap = new HashMap<Long, Integer>();
		for (int i=0; i<toks.size(); i++) {
			Annotation annot = toks.get(i);
			indexMap.put(annot.getStart(), i);
		}
		
		for (Map<String, Object> annotFilter : annotFilterList)
		{
			String annotType = (String) annotFilter.get("annotTypeName");

			if (!annotType.equals("Token")) {
				List<Annotation> annotList2 = new ArrayList<Annotation>();
				for (int i=0; i<toks.size(); i++)
					annotList2.add(null);
				
				List<Annotation> annotList = annotMap.get(annotType);
				for (Annotation annot : annotList) {
					long start = annot.getStart();
					int index = indexMap.get(start);
					Annotation annot2 = annotList2.get(index);
					if (annot2 == null)
						annotList2.set(index, annot);
				}
			}
		}
		
		annotMatrix.add(toks);
	}
	
	private List<String> getCurrElements()
	{
		List<String> elementList = new ArrayList<String>();
		for (int i=0; i<annotMatrix.size(); i++) {
			List<Annotation> annotList = annotMatrix.get(i);
			Annotation annot = annotList.get(currIndex);
			if (annot != null)
				elementList.add(annot.getValue());
			else
				elementList.add("");
		}
		
		return elementList;
	}
}
