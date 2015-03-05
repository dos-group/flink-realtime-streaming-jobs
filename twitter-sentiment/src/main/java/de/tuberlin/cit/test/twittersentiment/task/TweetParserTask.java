package de.tuberlin.cit.test.twittersentiment.task;

import de.tuberlin.cit.test.twittersentiment.record.TweetRecord;
import org.apache.flink.streaming.connectors.json.JSONParseFlatMap;
import org.apache.flink.util.Collector;
import org.apache.sling.commons.json.JSONArray;

public class TweetParserTask extends JSONParseFlatMap<String, TweetRecord> {

    @Override
    public void flatMap(String value, Collector<TweetRecord> out) throws Exception {
        TweetRecord tweet = new TweetRecord(
            getLong(value, "id"), getString(value, "text"), getString(value, "created_at")
        );

        JSONArray tags = (JSONArray) get(value, "entities.hashtags");
        for (int i = 0; i < tags.length(); i++) {
            tweet.addHashtag(tags.getJSONObject(i).getString("text"));
        }

        out.collect(tweet);
    }
}
