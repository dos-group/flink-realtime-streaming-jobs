package de.tuberlin.cit.test.twittersentiment.task;

import org.apache.flink.api.common.functions.MapFunction;

public class MapToOneTask<T> implements MapFunction<T, Long> {
	@Override
	public Long map(T value) throws Exception {
		return 1L;
	}
}
