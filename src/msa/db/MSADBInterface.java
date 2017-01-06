package msa.db;

import java.util.*;

import msa.Annotation;
import msa.AnnotationSequence;
import msa.AnnotationSequence2;
import msa.MultipleSequenceAlignment;
import ngram.NGram;

public interface MSADBInterface
{
	public void init(String user, String password, String host, String keyspace, String msaKeyspace) throws MSADBException;
	public List<Long> getDocIDList(String docQuery) throws MSADBException;
	public List<AnnotationSequence> getSentsInDoc(String docNamespace, String docTable, long docID) throws MSADBException;
	public List<Annotation> getSentAnnots(String docNamespace, String docTable, long docID, int start, int end, boolean punct) throws MSADBException;
	public List<Annotation> getAnnotations(String docNamespace, String docTable, long docID, long start, long end, String annotType, String provenance) throws MSADBException;
	public void writeAnnotation(Annotation annot, String docNamespace, String docTable, long docID, String provenance) throws MSADBException;
	//public Map<String, List<NGram>> readContextVectors(String docNamespace, String docTable) throws MSADBException;
	//public void writeContextVector(String docNamespace, String docTable, String group, String context) throws MSADBException;
	public void close() throws MSADBException;
	public void writeMSAToDB(MultipleSequenceAlignment msa, String docNamespace, String docTable, String group) throws MSADBException;
	public void writeProfile(int msaID, String docNamespace, String docTable, String annotType, String group, int type, String profileStr, double score) throws MSADBException;
	public void deleteProfiles(String annotType, String group) throws MSADBException;
	public double getProfileScore(String group, String profileStr, int type, String annotType) throws MSADBException;
	public void updateProfileScore(String docNamespace, String docTable, String group, String profileStr, int type, String annotType, double score) throws MSADBException;
}
