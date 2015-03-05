package de.tuberlin.cit.test.twittersentiment.task;

import de.tuberlin.cit.test.twittersentiment.record.TopicMapRecord;
import de.tuberlin.cit.test.twittersentiment.record.TweetRecord;
import org.apache.flink.streaming.api.function.co.CoFlatMapFunction;
import org.apache.flink.types.StringValue;
import org.apache.flink.util.Collector;

import java.util.HashSet;
import java.util.Set;

public class CoFilterTask implements CoFlatMapFunction<TopicMapRecord, TweetRecord, TweetRecord> {
	private Set<StringValue> topTopics = new HashSet<>();

	@Override
	public void flatMap1(TopicMapRecord topTopics, Collector<TweetRecord> out) throws Exception {
		this.topTopics = topTopics.keySet();
	}

	@Override
	public void flatMap2(TweetRecord tweet, Collector<TweetRecord> out) throws Exception {
		for (StringValue hashtag : tweet.getHashtags()) {
			if (this.topTopics.contains(hashtag)) {
				out.collect(tweet);
			}
		}
	}
}
