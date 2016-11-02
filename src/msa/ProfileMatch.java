package msa;

import java.util.*;

public class ProfileMatch
{
	private MSAProfile profile;
	private List<MSAProfile> targetList;
	private int gridIndex;
	private String gridStr;
	private List<int[]> matchCoords1;
	private List<int[]> matchCoords2;
	private int[] targetIndexes;
	private String targetStr;
	private AnnotationSequence sequence;

	public ProfileMatch()
	{
	}
	
	public ProfileMatch(MSAProfile profile, List<MSAProfile> targetList, int gridIndex, List<int[]> matchCoords1, List<int[]> matchCoords2, int[] targetIndexes, 
		String targetStr, String gridStr, AnnotationSequence sequence)
	{
		this.profile = profile;
		this.targetList = targetList;
		this.gridIndex = gridIndex;
		this.matchCoords1 = matchCoords1;
		this.matchCoords2 = matchCoords2;
		this.targetIndexes = targetIndexes;
		this.targetStr = targetStr;
		this.gridStr = gridStr;
		this.sequence = sequence;
	}

	public MSAProfile getProfile() {
		return profile;
	}

	public void setProfile(MSAProfile profile) {
		this.profile = profile;
	}
	
	public List<MSAProfile> getTargetList() {
		return targetList;
	}

	public void setTarget(List<MSAProfile> targetList) {
		this.targetList = targetList;
	}

	public int getGridIndex() {
		return gridIndex;
	}

	public void setGridIndex(int gridIndex) {
		this.gridIndex = gridIndex;
	}

	public List<int[]> getMatchCoords1() {
		return matchCoords1;
	}

	public void setMatchCoords1(List<int[]> matchCoords1) {
		this.matchCoords1 = matchCoords1;
	}

	public List<int[]> getMatchCoords2() {
		return matchCoords2;
	}

	public void setMatchCoords2(List<int[]> matchCoords2) {
		this.matchCoords2 = matchCoords2;
	}

	public int[] getTargetIndexes() {
		return targetIndexes;
	}

	public void setTargetIndexes(int[] targetIndexes) {
		this.targetIndexes = targetIndexes;
	}

	public String getTargetStr() {
		return targetStr;
	}

	public void setTargetStr(String targetStr) {
		this.targetStr = targetStr;
	}

	public String getGridStr() {
		return gridStr;
	}

	public void setGridStr(String gridStr) {
		this.gridStr = gridStr;
	}

	public AnnotationSequence getSequence() {
		return sequence;
	}

	public void setSequence(AnnotationSequence sequence) {
		this.sequence = sequence;
	}
}
