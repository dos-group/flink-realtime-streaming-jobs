package de.tuberlin.cit.test.twittersentiment.util;

import de.tuberlin.cit.test.twittersentiment.record.TweetRecord;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class UnlimitedTweetSource extends TweetSource {

	private static final Logger LOG = LoggerFactory.getLogger(UnlimitedTweetSource.class);
	private static final long LOG_STATS = 10 * 1000; // log stats every 10s

	/**
	 * Wait ...ms for input or finish (return null as tweet).
	 */
	public static final String INPUT_TIMEOUT_KEY = "unlimitedTweetSource.inputTimeout";
	public static final long DEFAULT_INPUT_TIMEOUT = 2 * 60 * 1000; // 2min
	private final long inputTimeout;

	private int tweetsEmitted;

	private long lastStatisticLogged;

	public UnlimitedTweetSource(int port, long inputTimeout) {
		super(port);
		this.inputTimeout = inputTimeout;
	}

	public UnlimitedTweetSource() {
		this(DEFAULT_TCP_SERVER_PORT, DEFAULT_INPUT_TIMEOUT);
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		this.tweetsEmitted = 0;
		this.lastStatisticLogged = System.currentTimeMillis();
//        this.inputTimeout = parameters.getLong(INPUT_TIMEOUT_KEY, DEFAULT_INPUT_TIMEOUT);
	}

	@Override
	public TweetRecord getTweet() throws InterruptedException, IOException {

		long now = System.currentTimeMillis();

		if (now - this.lastStatisticLogged >= LOG_STATS) {
			logAndResetStats();
		}

		TweetRecord tweet = queue.poll(this.inputTimeout, TimeUnit.MILLISECONDS);

		if (tweet == null || System.currentTimeMillis() - now >= this.inputTimeout) {
			LOG.info(String.format("Tweet source finished after waiting %.1fs.",
					(System.currentTimeMillis() - now) / 1000.0));
			return null;
		}

		this.tweetsEmitted++;

		return tweet;
	}

	private void logAndResetStats() {
		double phase = (System.currentTimeMillis() - this.lastStatisticLogged) / 1000.0;

		LOG.info(String.format("Emitted %.1f tweets/sec for %.1f sec (%d tweets total).",
				this.tweetsEmitted / phase, phase, this.tweetsEmitted));

		this.tweetsEmitted = 0;
		this.lastStatisticLogged = System.currentTimeMillis();
	}

	@Override
	public void close() {
		logAndResetStats();
	}
}