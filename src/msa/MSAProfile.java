package msa;

import java.util.*;

import align.AnnotationSequenceGrid;
import align.SmithWatermanDim_old;
import nlputils.sequence.SequenceUtilities;

public class MSAProfile
{
	private long profileID;
	private String profileStr;
	private List<String> toks;
	private String annotType;
	private String group;
	private int type;
	private double score = 0.0;
	private int truePos = 0;
	private int falsePos = 0;
	private int rows;

	
	public MSAProfile()
	{
	}
	
	public MSAProfile(long profileID, String profileStr, String annotType, String group, int type, List<String> toks)
	{
		this();
		this.profileID = profileID;
		this.profileStr = profileStr;
		this.annotType = annotType;
		this.group = group;
		this.type = type;
		this.toks = toks;
	}
	
	public MSAProfile(String profileStr, String annotType, String group, int type, List<String> toks)
	{
		this();
		this.profileStr = profileStr;
		this.annotType = annotType;
		this.group = group;
		this.type = type;
		this.toks = toks;
	}
	
	public MSAProfile(String profileStr, String annotType, String group, int type, List<String> toks, double score, int rows)
	{
		this(profileStr, annotType, group, type, toks);
		this.score = score;
		this.rows = rows;
	}
	
	public MSAProfile(String profileStr, String annotType, String group, int type, List<String> toks, double score, int truePos, int falsePos)
	{
		this(profileStr, annotType, group, type, toks, score, 0);
		this.truePos = truePos;
		this.falsePos = falsePos;
	}
	
	public MSAProfile clone()
	{
		MSAProfile profile = new MSAProfile(profileStr, annotType, group, type, toks);
		return profile;
	}

	public long getProfileID() {
		return profileID;
	}

	public void setProfileID(long profileID) {
		this.profileID = profileID;
	}

	public String getProfileStr() {
		return profileStr;
	}

	public void setProfileStr(String profileStr) {
		this.profileStr = profileStr;
	}

	public List<String> getToks() {
		return toks;
	}

	public void setProfileToks(List<String> toks) {
		this.toks = toks;
	}
	
	public int size()
	{
		return toks.size();
	}

	public String getAnnotType() {
		return annotType;
	}

	public void setAnnotType(String annotType) {
		this.annotType = annotType;
	}

	public String getGroup() {
		return group;
	}

	public void setGroup(String group) {
		this.group = group;
	}

	public int getType() {
		return type;
	}

	public void setType(int type) {
		this.type = type;
	}

	public double getScore() {
		return score;
	}

	public void setScore(double score) {
		this.score = score;
	}

	public int getTruePos() {
		return truePos;
	}

	public void setTruePos(int truePos) {
		this.truePos = truePos;
	}

	public int getFalsePos() {
		return falsePos;
	}

	public void setFalsePos(int falsePos) {
		this.falsePos = falsePos;
	}

	public int getRows() {
		return rows;
	}

	public void setRows(int rows) {
		this.rows = rows;
	}
}
