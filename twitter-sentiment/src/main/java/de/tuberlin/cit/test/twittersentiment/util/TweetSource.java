package de.tuberlin.cit.test.twittersentiment.util;

import de.tuberlin.cit.test.twittersentiment.record.TweetRecord;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public abstract class TweetSource extends RichSourceFunction<TweetRecord> {
	public static final String TCP_SERVER_PORT = "tweetsourcetask.tcpserverport";
	public static final int DEFAULT_TCP_SERVER_PORT = 9001;
	private static final int TWEET_QUEUE_SIZE = 10000;

	private static final Logger LOG = LoggerFactory.getLogger(TweetSource.class);

	protected int port;
	protected BlockingQueue<TweetRecord> queue;
	private Thread socketWorkerThread;

	private volatile boolean isRunning;

	public TweetSource(int port) {
		this.port = port;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);

		LOG.info("Got open signal! Starting socket worker on port {}.", port);
		this.queue = new ArrayBlockingQueue<>(TWEET_QUEUE_SIZE);
		this.socketWorkerThread = new Thread(new SocketWorker(queue, port), "SocketWorker");
		this.socketWorkerThread.start();
	}

	public abstract TweetRecord getTweet() throws InterruptedException, IOException;

	@Override
	public void run(Object checkpointLock, Collector<TweetRecord> collector) throws Exception {
		TweetRecord tweet;

		isRunning = true;

		while (isRunning && (tweet = getTweet()) != null) {
			collector.collect(tweet);
		}
	}

	@Override
	public void close() throws Exception {
		super.close();

		LOG.info("Got close signal! Interrupting socket worker.");
		this.queue = null;
		this.socketWorkerThread.interrupt();
		this.socketWorkerThread = null;
	}

	@Override
	public void cancel() {
		isRunning = false;
	}
}
