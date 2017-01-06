package msa;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Annotation
{
	private int id;
	private long docID;
	private String docNamespace;
	private String docTable;
	private String annotType;
	private long start;
	private long end;
	private String features;
	private Map<String, Object> featureMap;
	private boolean mask = true;
	private String value;
	private String label;
	private double score;
	private String provenance;
	
	public Annotation()
	{
		featureMap = new HashMap<String, Object>();
	}
	
	public Annotation(int id, String annotType, long start, long end, String value, Map<String, Object> featureMap)
	{
		this();
		this.id = id;
		this.annotType = annotType;
		this.start = start;
		this.end = end;
		this.value = value;
		this.featureMap = featureMap;
	}
	
	public Annotation(long docID, String docNamespace, String docTable, int id, String annotType, long start, long end, String value, Map<String, Object> featureMap)
	{
		this(id, annotType, start, end, value, featureMap);
		this.docID = docID;
		this.docNamespace = docNamespace;
		this.docTable = docTable;
	}
	
	public Annotation(int id, String annotType, long start, long end, String value, Map<String, Object> featureMap, String provenance)
	{
		this(id, annotType, start, end, value, featureMap);
		this.provenance = provenance;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getAnnotationType() {
		return annotType;
	}

	public void setAnnotationType(String annotType) {
		this.annotType = annotType;
	}

	public long getStart() {
		return start;
	}

	public void setStart(long start) {
		this.start = start;
	}

	public long getEnd() {
		return end;
	}

	public void setEnd(long end) {
		this.end = end;
	}

	public String getFeatures() {
		return features;
	}

	public void setFeatures(String features) {
		this.features = features;
	}
	
	public String toString()
	{
		return "id: " + id + "\ndocNamespace: " + docNamespace + "\ndocTable: " + docTable + "\ndocID: " + docID + "\ntype: " + annotType + "\nstart: " + start + "\nend: " + end + "\nprovenance: " + provenance + "\nfeatures: " + features + "\nvalue: " + value;
	}

	public Map<String, Object> getFeatureMap() {
		return featureMap;
	}

	public void setFeatureMap(Map<String, Object> featureMap) {
		this.featureMap = featureMap;
	}
	
	public Object getFeature(String feature)
	{
		return featureMap.get(feature);
	}
	
	public void setFeature(String key, Object value)
	{
		featureMap.put(key, value);
	}

	public boolean getMask() {
		return mask;
	}

	public void setMask(boolean mask) {
		this.mask = mask;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public long getDocID() {
		return docID;
	}

	public void setDocID(long docID) {
		this.docID = docID;
	}

	public String getDocNamespace() {
		return docNamespace;
	}

	public void setDocNamespace(String docNamespace) {
		this.docNamespace = docNamespace;
	}

	public String getDocTable() {
		return docTable;
	}

	public void setDocTable(String docTable) {
		this.docTable = docTable;
	}

	public String getLabel() {
		return label;
	}

	public void setLabel(String label) {
		this.label = label;
	}

	public double getScore() {
		return score;
	}

	public void setScore(double score) {
		this.score = score;
	}

	public String getProvenance() {
		return provenance;
	}

	public void setProvenance(String provenance) {
		this.provenance = provenance;
	}

}
