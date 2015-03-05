package de.tuberlin.cit.test.twittersentiment.task;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.tuberlin.cit.test.twittersentiment.record.TopicMapRecord;
import de.tuberlin.cit.test.twittersentiment.record.TweetRecord;
import de.tuberlin.cit.test.twittersentiment.util.Utils;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.StringValue;
import org.apache.flink.util.Collector;

import java.util.*;

public class HotTopicsRecognitionTask extends RichFlatMapFunction<TweetRecord, TopicMapRecord> {
	public static final String HISTORY_SIZE = "hottopicsrecognition.historysize";
	public static final int DEFAULT_HISTORY_SIZE = 50000;
	public static final String TOP_COUNT = "hottopicsrecognition.topcount";
	public static final int DEFAULT_TOP_COUNT = 25;
	public static final String TIMEOUT = "hottopicsrecognition.timeout";
	public static final int DEFAULT_TIMEOUT = 100;

	private Date lastSent = new Date();
	private int timeout;
	private Queue<List<StringValue>> hashtagHistory;
	private Map<StringValue, Integer> hashtagCount;
	private int topCount;
	private int historySize;
	private int historyWarmupCount = 0;
    private ObjectMapper objectMapper;

    public HotTopicsRecognitionTask(int topCount, int historySize, int timeout) {
        this.topCount = topCount;
        this.historySize = historySize;
        this.timeout = timeout;
    }

	public HotTopicsRecognitionTask(int topCount, int historySize) {
		this(topCount, historySize, DEFAULT_TIMEOUT);
	}

	public HotTopicsRecognitionTask() {
        this(DEFAULT_TOP_COUNT, DEFAULT_HISTORY_SIZE, DEFAULT_TIMEOUT);
    }

    @Override
	public void open(Configuration parameters) {
        objectMapper = new ObjectMapper();
    	hashtagHistory = new ArrayDeque<>(historySize);
		hashtagCount  = new HashMap<>(topCount);

		// fill dummy history
		List<StringValue> dummy = new ArrayList<>();
		for (int i = 0; i < historySize; i++) {
			hashtagHistory.offer(dummy);
		}
	}

    @Override
    public void flatMap(TweetRecord tweet, Collector<TopicMapRecord> out) throws Exception {
		List<StringValue> hashtags = tweet.getHashtags();

		if (hashtags.size() != 0) {
			// forget history
			List<StringValue> oldHashtags = hashtagHistory.poll();
			for (StringValue hashtag : oldHashtags) {
				Integer count = hashtagCount.get(hashtag);
				if (count == null) {
					continue;
				}
				if (count <= 1) {
					hashtagCount.remove(hashtag);
				} else {
					hashtagCount.put(hashtag, count - 1);
				}
			}

			// update hashtag count
			for (StringValue hashtag : hashtags) {
				Integer count = hashtagCount.get(hashtag);
				if (count == null) {
					count = 0;
				}
				hashtagCount.put(hashtag, count + 1);
			}
			hashtagHistory.offer(hashtags);
			if (historyWarmupCount < historySize) {
				++historyWarmupCount;
			}
		}


		// send one topic list every interval
		Date now = new Date();
		if (now.getTime() - lastSent.getTime() > timeout && historyWarmupCount == historySize) {
			// construct topic list
			Map<StringValue, Integer> sortedHashtagCount = Utils.sortMapByEntry(hashtagCount,
					new Comparator<Map.Entry<StringValue, Integer>>() {
						public int compare(Map.Entry<StringValue, Integer> o1, Map.Entry<StringValue, Integer> o2) {
							return -(o1.getValue()).compareTo(o2.getValue());
						}
					});

            TopicMapRecord topicMap = new TopicMapRecord();
            Iterator<Map.Entry<StringValue, Integer>> iterator = sortedHashtagCount.entrySet().iterator();
            for (int i = 0; i < topCount && iterator.hasNext(); i++) {
                Map.Entry<StringValue, Integer> entry = iterator.next();
                topicMap.put(entry.getKey(), new IntValue(entry.getValue()));
            }
			out.collect(topicMap);

			lastSent = now;
		}
	}
}
