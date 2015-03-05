package de.tuberlin.cit.test.twittersentiment.task;

import de.tuberlin.cit.test.twittersentiment.record.TopicMapRecord;
import de.tuberlin.cit.test.twittersentiment.util.Utils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.MapValue;
import org.apache.flink.types.StringValue;
import org.apache.flink.util.Collector;

import java.util.*;

public class HotTopicsMergerTask implements FlatMapFunction<TopicMapRecord, TopicMapRecord> {
	public static final String TIMEOUT = "topicsmerger.timeout";
	public static final int DEFAULT_TIMEOUT = 200;
	private Date lastSent = new Date();
	private int timeout;
	private List<TopicMapRecord> topicLists = new ArrayList<>();

    public HotTopicsMergerTask(int timeout) {
        this.timeout = timeout;
    }

    public HotTopicsMergerTask() {
        this(DEFAULT_TIMEOUT);
    }

    @Override
    public void flatMap(TopicMapRecord record, Collector<TopicMapRecord> out) throws Exception {
		topicLists.add(record);

		Date now = new Date();
		if (now.getTime() - lastSent.getTime() > timeout) {
			TopicMapRecord averageTopicMap = getAverageTopicList();
			out.collect(averageTopicMap);
			lastSent = now;
			printRanking(averageTopicMap);
			topicLists.clear();
		}
	}

	private TopicMapRecord getAverageTopicList() {
		Set<StringValue> keys = new HashSet<StringValue>();
		TopicMapRecord averageTopicList = new TopicMapRecord();

		for (TopicMapRecord topicMap : topicLists) {
			keys.addAll(topicMap.keySet());
		}

		for (StringValue key : keys) {
			int sum = 0;
			for (TopicMapRecord topicList : topicLists) {
				IntValue count = topicList.get(key);
				if (count != null) {
					sum += count.getValue();
				}
			}
			averageTopicList.put(key, new IntValue(sum / topicLists.size()));
		}

		int topCount = (int) (topicLists.get(0).size() * 0.6);
		Map<StringValue, IntValue> sortedMap = Utils.sortMapByEntry(averageTopicList,
				new Comparator<MapValue.Entry<StringValue, IntValue>>() {
					public int compare(MapValue.Entry<StringValue, IntValue> o1, MapValue.Entry<StringValue, IntValue> o2) {
						return -(o1.getValue()).compareTo(o2.getValue());
					}
				});

        return (TopicMapRecord) Utils.slice(averageTopicList, new TopicMapRecord(), topCount);
	}

	private void printRanking(TopicMapRecord sortedHashtagCount) {
		System.out.println();
		System.out.println("Average Hashtag Ranking (from " + topicLists.size() +  " topic lists)");
		for (MapValue.Entry<StringValue, IntValue> stringIntegerEntry : sortedHashtagCount.entrySet()) {
			System.out.printf("%s (%d)\n", stringIntegerEntry.getKey(), stringIntegerEntry.getValue().getValue());
		}
	}
}
