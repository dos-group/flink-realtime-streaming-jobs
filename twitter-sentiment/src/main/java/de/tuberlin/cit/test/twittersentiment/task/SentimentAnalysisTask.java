package de.tuberlin.cit.test.twittersentiment.task;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.tuberlin.cit.test.twittersentiment.record.TweetRecord;
import de.tuberlin.cit.test.twittersentiment.util.SentimentClassifier;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.StringValue;

public class SentimentAnalysisTask extends RichMapFunction<TweetRecord, Tuple3<Long, String, StringValue>> {
	private SentimentClassifier sentimentClassifier;
	private ObjectMapper objectMapper;

	@Override
	public void open(Configuration parameters) throws Exception {
		this.sentimentClassifier = new SentimentClassifier();
		this.objectMapper = new ObjectMapper();
	}

	@Override
	public Tuple3 map(TweetRecord tweet) throws Exception {
		String sentiment = sentimentClassifier.classify(tweet.getText().getValue());
		return new Tuple3<>(tweet.getId(), sentiment, tweet.getText());
	}
}
