package de.tuberlin.cit.test.twittersentiment.record;

import org.apache.flink.types.StringValue;

import java.io.Serializable;

public class HashtagCountRecord implements Serializable, Comparable<HashtagCountRecord> {
	private static final long serialVersionUID = 1L;
	public StringValue tag;
	public Integer count;

	public HashtagCountRecord() {
	}

	public HashtagCountRecord(StringValue tag, Integer count) {
		this.tag = tag;
		this.count = count;
	}

	public HashtagCountRecord(String tag, Integer count) {
		this(new StringValue(tag), count);
	}

	@Override
	public String toString() {
		return "(" + tag + ": " + count + ")";
	}

	@Override
	public boolean equals(Object other) {
		return (other instanceof HashtagCountRecord)
				&& this.tag.equals(((HashtagCountRecord) other).tag)
				&& this.count.equals(((HashtagCountRecord) other).count);
	}

	@Override
	public int compareTo(HashtagCountRecord o) {
		return this.count.compareTo(o.count);
	}

	@Override
	protected Object clone() throws CloneNotSupportedException {
		return new HashtagCountRecord(this.tag, this.count);
	}
}
