package de.tuberlin.cit.test.twittersentiment.task;

import de.tuberlin.cit.test.twittersentiment.record.HashtagCountRecord;
import de.tuberlin.cit.test.twittersentiment.record.TopicMapRecord;
import org.apache.flink.streaming.api.functions.WindowMapFunction;
import org.apache.flink.types.IntValue;
import org.apache.flink.util.Collector;

import java.util.Iterator;
import java.util.TreeSet;

public class TopHashTagsMapper implements WindowMapFunction<HashtagCountRecord, TopicMapRecord> {
	private int topCount = 10;

	public TopHashTagsMapper(int topCount) {
		this.topCount = topCount;
	}

	@Override
	public void mapWindow(Iterable<HashtagCountRecord> values, Collector<TopicMapRecord> out) throws Exception {
		TreeSet<HashtagCountRecord> counts = new TreeSet<>();
		TopicMapRecord topTopics = new TopicMapRecord();

		for (HashtagCountRecord hashtag : values)
			counts.add(hashtag);

		Iterator<HashtagCountRecord> iter = counts.iterator();
		for (int i = 0; i < this.topCount && iter.hasNext(); i++) {
			HashtagCountRecord hashtag = iter.next();
			topTopics.put(hashtag.tag, new IntValue(hashtag.count));
		}

		out.collect(topTopics);
	}
}
