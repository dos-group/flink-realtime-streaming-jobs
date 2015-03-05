package de.tuberlin.cit.test.twittersentiment.profile.task;

public class TaskProfile {
	public final int amountSubstasks;
	public final int subtasksPerInstance;
	public final int maximumAmountSubtasks;
	public final int initalAmountSubtasks;
	public final int minimumAmountSubtasks;

	public TaskProfile(int amountSubstasks, int subtasksPerInstance, int maximumAmountSubtasks, int initalAmountSubtasks, int minimumAmountSubtasks) {
		this.amountSubstasks = amountSubstasks;
		this.subtasksPerInstance = subtasksPerInstance;
		this.maximumAmountSubtasks = maximumAmountSubtasks;
		this.initalAmountSubtasks = initalAmountSubtasks;
		this.minimumAmountSubtasks = minimumAmountSubtasks;
	}
}