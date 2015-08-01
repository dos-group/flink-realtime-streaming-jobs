package de.tuberlin.cit.test.twittersentiment.task;

import de.tuberlin.cit.test.twittersentiment.record.HashtagCountRecord;
import de.tuberlin.cit.test.twittersentiment.record.TweetRecord;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.types.StringValue;
import org.apache.flink.util.Collector;

public class HashtagExtractorTask implements FlatMapFunction<TweetRecord, HashtagCountRecord> {
	@Override
	public void flatMap(TweetRecord tweet, Collector<HashtagCountRecord> out) throws Exception {
		for (StringValue hashtag : tweet.getHashtags()) {
			out.collect(new HashtagCountRecord(hashtag, 1));
		}
	}
}
