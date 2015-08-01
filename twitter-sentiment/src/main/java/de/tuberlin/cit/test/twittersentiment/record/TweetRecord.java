package de.tuberlin.cit.test.twittersentiment.record;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.types.StringValue;

import java.util.ArrayList;
import java.util.List;

public class TweetRecord {
	private long id;
	private StringValue text;
	private List<StringValue> hashtags;
	private StringValue createdAt;

	public TweetRecord(long id, StringValue text, List<StringValue> hashtags, StringValue createdAt) {
		this.id = id;
		this.text = text;
		this.hashtags = hashtags;
		this.createdAt = createdAt;
	}

	public TweetRecord(long id, StringValue text, StringValue createdAt) {
		this(id, text, new ArrayList<StringValue>(), createdAt);
	}

	public TweetRecord(long id, String text, String createdAt) {
		this(id, new StringValue(text), new StringValue(createdAt));
	}

	public TweetRecord(JsonNode json) {
		this(json.get("id").longValue(), json.get("text").asText(), json.get("created_at").asText());
		for (JsonNode hashtag : json.get("entities").get("hashtags")) {
			addHashtag(hashtag.get("text").asText());
		}
	}

	public long getId() {
		return id;
	}

	public StringValue getText() {
		return text;
	}

	public List<StringValue> getHashtags() {
		return hashtags;
	}

	public void addHashtag(String tag) {
		this.hashtags.add(new StringValue(tag.toLowerCase()));
	}

	public StringValue getCreatedAt() {
		return createdAt;
	}

	@Override
	public String toString() {
		return "Tweet: id=" + id + ", text=" + text + ", tags=" + hashtags;
	}
}
