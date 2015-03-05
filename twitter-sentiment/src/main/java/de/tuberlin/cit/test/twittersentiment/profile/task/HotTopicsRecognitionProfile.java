package de.tuberlin.cit.test.twittersentiment.profile.task;

public class HotTopicsRecognitionProfile extends TaskProfile {
	public final int historySize;
	public final int topCount;

	public HotTopicsRecognitionProfile(int amountSubstasks, int subtasksPerInstance, int maximumAmountSubtasks, int initalAmountSubtasks, int minimumAmountSubtasks, int historySize, int topCount) {
		super(amountSubstasks, subtasksPerInstance, maximumAmountSubtasks, initalAmountSubtasks, minimumAmountSubtasks);
		this.historySize = historySize;
		this.topCount = topCount;
	}
}