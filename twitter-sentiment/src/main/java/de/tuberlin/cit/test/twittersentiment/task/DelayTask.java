package de.tuberlin.cit.test.twittersentiment.task;

import org.apache.flink.api.common.functions.MapFunction;

public class DelayTask<T> implements MapFunction<T, T> {
	private long delay;

	public DelayTask(long delay) {
		this.delay = delay;
	}

	@Override
	public T map(T value) throws Exception {
		Thread.sleep(this.delay);
		return value;
	}
}
