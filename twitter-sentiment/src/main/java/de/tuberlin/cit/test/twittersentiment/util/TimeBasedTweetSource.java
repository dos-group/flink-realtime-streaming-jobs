package de.tuberlin.cit.test.twittersentiment.util;

import de.tuberlin.cit.test.twittersentiment.record.TweetRecord;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

public class TimeBasedTweetSource extends TweetSource {

	private static final Logger LOG = LoggerFactory.getLogger(TimeBasedTweetSource.class);
	private static final long LOG_STATS = 10 * 1000; // log statistics every 10s

	/**
	 * Multiply tweet creation time (in seconds) by given factor.
	 */
	public static final String FACTOR_KEY = "timeBasedTweetSource.factor";
	//	public static final double DEFAULT_FACTOR = 0.001; // play 1000 original tweet seconds in 1s
	public static final double DEFAULT_FACTOR = 1.0d / 60; // play 1000 original tweet seconds in 1s
	private final double factor;

	/**
	 * Drop tweets if we miss the timeline.
	 */
	public static final String DROP_TWEETS_KEY = "timeBasedTweetSource.dropTweets";
	public static final boolean DEFAULT_DROP_TWEETS = false;
	private final boolean dropTweets;

	/**
	 * Drop tweet if we missed expected send time by given timeout in ms.
	 */
	public static final String DROP_TIMEOUT_KEY = "timeBasedTweetSource.dropTimeout";
	public static final long DEFAULT_DROP_TIMEOUT = 2000; // 2s
	private final long dropTimeout;

	/**
	 * Wait ...ms for input or finish (return null as tweet).
	 */
	public static final String INPUT_TIMEOUT_KEY = "timeBasedTweetSource.inputTimeout";
	public static final long DEFAULT_INPUT_TIMEOUT = 2 * 60 * 1000; // 2min
	private final long inputTimeout;

	private final SimpleDateFormat dateFormat = new SimpleDateFormat("EEE MMM dd HH:mm:ss ZZZZ yyyy", Locale.ENGLISH);

	private long tweetsEmitted;

	private long tweetsDropped;

	private long lastStatisticLogged;

	private long tweetOrigin;

	private long jobOrigin;

	public TimeBasedTweetSource(int port, double factor, boolean dropTweets, long dropTimeout, long inputTimeout) {
		super(port);
		this.factor = factor;
		this.dropTweets = dropTweets;
		this.dropTimeout = dropTimeout;
		this.inputTimeout = inputTimeout;
	}

	public TimeBasedTweetSource(int port, double factor) {
		this(port, factor, DEFAULT_DROP_TWEETS, DEFAULT_DROP_TIMEOUT, DEFAULT_INPUT_TIMEOUT);
	}

	public TimeBasedTweetSource(double factor) {
		this(DEFAULT_TCP_SERVER_PORT, factor);
	}

	public TimeBasedTweetSource() {
		this(DEFAULT_FACTOR);
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);

		this.tweetsEmitted = 0;
		this.tweetsDropped = 0;
		this.lastStatisticLogged = System.currentTimeMillis();

		this.tweetOrigin = -1;
		this.jobOrigin = -1;

//        this.factor = parameters.getDouble(FACTOR_KEY, DEFAULT_FACTOR);
//        this.dropTweets = parameters.getBoolean(DROP_TWEETS_KEY, DEFAULT_DROP_TWEETS);
//        this.dropTimeout = parameters.getLong(DROP_TIMEOUT_KEY, DEFAULT_DROP_TIMEOUT);
//        this.inputTimeout = parameters.getLong(INPUT_TIMEOUT_KEY, DEFAULT_INPUT_TIMEOUT);

		LOG.info("Time based tweet source with factor " + this.factor + " launched.");
	}

	@Override
	public TweetRecord getTweet()
			throws InterruptedException, IOException {

		while (true) {
			long now = System.currentTimeMillis();
			long creationTimestamp;

			if (System.currentTimeMillis() - this.lastStatisticLogged >= LOG_STATS) {
				logAndResetStats();
			}

			TweetRecord tweet = queue.poll(this.inputTimeout, TimeUnit.MILLISECONDS);

			if (tweet == null || System.currentTimeMillis() - now >= this.inputTimeout) {
				LOG.info(String.format("Tweet source finished after waiting %.1fs.",
						(System.currentTimeMillis() - now) / 1000.0));
				return null;
			}

			try {
				creationTimestamp = this.dateFormat.parse(tweet.getCreatedAt().getValue()).getTime();
			} catch (NumberFormatException | ParseException e) {
				LOG.error("Can't parse tweet timestamp: " + tweet.getCreatedAt().getValue(), e);
				this.tweetsDropped++;
				continue;
			}

			if (this.tweetOrigin == -1) {
				this.tweetOrigin = creationTimestamp;
				this.jobOrigin = now;
			}

			long nowOriginOffset = now - this.jobOrigin;
			long tweetOriginOffset = creationTimestamp - this.tweetOrigin;
			long sleepFromOrigin = (long) (tweetOriginOffset * this.factor);
			long sleepFromNow = sleepFromOrigin - nowOriginOffset;


			if (this.dropTweets && sleepFromNow < 0 && -1 * sleepFromNow > this.dropTimeout) {
				this.tweetsDropped++;
				continue;
			} else {
				this.tweetsEmitted++;
			}

			if (sleepFromNow > 0) {
				Thread.sleep(sleepFromNow);
			}

			return tweet;
		}
	}

	private void logAndResetStats() {
		double phase = (System.currentTimeMillis() - this.lastStatisticLogged) / 1000.0;

		LOG.info(String.format("Emitted %.1f tweets/sec for %.1f sec (total=%d, emitted=%d, dropped=%d).",
				this.tweetsEmitted / phase,
				phase,
				this.tweetsEmitted + this.tweetsDropped,
				this.tweetsEmitted,
				this.tweetsDropped));

		this.tweetsEmitted = 0;
		this.tweetsDropped = 0;
		this.lastStatisticLogged = System.currentTimeMillis();
	}

	@Override
	public void close() throws Exception {
		super.close();
		logAndResetStats();
	}
}