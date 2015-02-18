package de.tuberlin.cit.test.queuebehavior.util;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LatencyLog {
	public static final int LOG_INTERVAL_MILLIS = 10000;
	
	private static final Logger LOG = LoggerFactory.getLogger(LatencyLog.class);
	
	private ExecutorService backgroundWorker = Executors.newSingleThreadExecutor();
	
	private Writer out;

	private long aggregatedLatencies;

	private int counter;

	private long minLatency;

	private long maxLatency;

	private long nextLogIntervalBegin;
	
	public LatencyLog(String file) throws IOException {
		out = new FileWriter(file);
		out.write("timestamp;avgLatency;minLatency;maxLatency\n");
		out.flush();
		
		nextLogIntervalBegin = alignToInterval(System.currentTimeMillis() + LOG_INTERVAL_MILLIS,
				LOG_INTERVAL_MILLIS);

		aggregatedLatencies = 0;
		counter = 0;
		minLatency = Long.MAX_VALUE;
		maxLatency = Long.MIN_VALUE;
	}
	
	public void log(long timestamp) {
		long now = System.currentTimeMillis();

		long latency = Math.max(0, now - timestamp);
		
		aggregatedLatencies += latency;
		counter++;
		minLatency = Math.min(minLatency, latency);
		maxLatency = Math.max(maxLatency, latency);

		if (now >= nextLogIntervalBegin) {
			final String logLine = String.format("%d;%.1f;%d;%d\n",
					nextLogIntervalBegin / 1000,
					((double) aggregatedLatencies) / counter,
					minLatency, maxLatency);

			backgroundWorker.submit(createLogWorker(logLine, out));
			aggregatedLatencies = 0;
			counter = 0;
			minLatency = Long.MAX_VALUE;
			maxLatency = Long.MIN_VALUE;

			while (nextLogIntervalBegin <= now) {
				nextLogIntervalBegin += LOG_INTERVAL_MILLIS;
			}
		}
	}
	
	public void close() {
		if (out != null) {
			backgroundWorker.submit(createCloseLogWorker(out));
			backgroundWorker.shutdown();
			out = null;
		}
	}
	
	private static long alignToInterval(long timestampInMillis, long interval) {
		long remainder = timestampInMillis % interval;

		return timestampInMillis - remainder;
	}
	
	private static Runnable createLogWorker(final String logLine, final Writer out) {
		return new Runnable() {
			@Override
			public void run() {
				try {
					out.write(logLine);
					out.flush();
				} catch (IOException e) {
					LOG.error("Error when writing to receiver latency log", e);
				}
			}
		};
	}

	private static Runnable createCloseLogWorker(final Writer out) {
		return new Runnable() {
			@Override
			public void run() {
				try {
					out.close();
				} catch (IOException e) {
					LOG.error("Error when closing receiver latency log", e);
				}
			}
		};

	}
}
