package de.tuberlin.cit.test.twittersentiment;

import de.tuberlin.cit.test.twittersentiment.profile.TwitterSentimentJobProfile;
import de.tuberlin.cit.test.twittersentiment.record.TopicMapRecord;
import de.tuberlin.cit.test.twittersentiment.record.TweetRecord;
import de.tuberlin.cit.test.twittersentiment.task.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedDataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.helper.Time;
import org.apache.flink.streaming.api.windowing.helper.WindowingHelper;
import org.apache.flink.types.StringValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

/**
 * @author Bjoern Lohrmann
 * @author Sascha Wolke
 */
public class TwitterSentimentJobTest {
	private final static Logger log = LoggerFactory.getLogger(TwitterSentimentJobTest.class);

	public static void main(String[] args) throws Exception {

		if (args.length != 3) {
			printUsage();
			System.exit(1);
			return;
		}

		String jmHost = args[0];
		int jmPort = -1;
		if (jmHost.contains(":")) {
			jmHost = args[0].split(":")[0];
			jmPort = Integer.parseInt(args[0].split(":")[1]);
		}

		String outputPath = args[1];

		TwitterSentimentJobProfile profile = TwitterSentimentJobProfile.PROFILES.get(args[2]);
		if (profile == null) {
			System.err.printf("Unknown profile: %s\n", args[2]);
			printUsage();
			System.exit(1);
			return;
		}

		int topCount = profile.paraProfile.hotTopicsRecognition.topCount;
		int historySize = profile.paraProfile.hotTopicsRecognition.historySize;
		int topTopicsWindow = HotTopicsRecognitionTask.DEFAULT_TIMEOUT;
		int topTopicsTimeout = HotTopicsMergerTask.DEFAULT_TIMEOUT;

        // set up the execution environment
		final StreamExecutionEnvironment env = setupEnv(jmHost, jmPort);
		env.setBufferTimeout(10);

        // get input data
        DataStream<TweetRecord> tweetStream = env.readTextFile("test-data/tweets-en-2014-07-25-with_hashtag-500.json")
				.flatMap(new TweetParserTask())
				.map(new DelayTask<TweetRecord>(10)); // delay tweets by 10ms per tweet

		// dumpStatistics(tweetStream, "Input: %d tweets per second.", Time.of(1, TimeUnit.SECONDS)).printToErr();

        /*** Flink Window Version ***/
		// calculate top 10
		WindowedDataStream<TopicMapRecord> topTweets = tweetStream
				.flatMap(new HashtagExtractorTask()) // tweet -> [(hashtag, 1), (hashtag, 1), ...]
				.window(Time.of(topTopicsWindow, TimeUnit.MILLISECONDS))
				.every(Time.of(topTopicsTimeout, TimeUnit.MILLISECONDS))
				.groupBy("tag")
				.sum("count")
				.mapWindow(new TopHashTagsMapper(topCount));
		// topTweets.flatten().printToErr();

		DataStream<Tuple3<Long, String, StringValue>> analyzedTweets = topTweets.flatten()
				.broadcast()
				.connect(tweetStream)
				.flatMap(new CoFilterTask())
				.map(new SentimentAnalysisTask());
		analyzedTweets.writeAsText(outputPath).setParallelism(1);

		/*** Manual window version ***/
		/*
		DataStream<Tuple3<Long, String, StringValue>> analyzedTweets = tweetStream
                .flatMap(new HotTopicsRecognitionTask(topCount, historySize, topTopicsWindow))
                .flatMap(new HotTopicsMergerTask(topTopicsTimeout)).setParallelism(1)
				.connect(tweetStream)
				.flatMap(new CoFilterTask())
				.map(new SentimentAnalysisTask());
		*/

		// dumpStatistics(analyzedTweets, "Output: %d tweets per second.", Time.of(1, TimeUnit.SECONDS)).printToErr();

        env.execute("TwitterSentimentJob");
    }

	private static StreamExecutionEnvironment setupEnv(String jmHost, int jmPort) throws Exception {
		if (jmHost.equalsIgnoreCase("local")) {
			log.info("Running in local mode.");
			return StreamExecutionEnvironment.getExecutionEnvironment();

		} else {
			log.info("Running in remote mode. Creating jar...");

			// Create jar file for job deployment
			Process p = Runtime.getRuntime().exec("mvn clean package");
			if (p.waitFor() != 0) {
				System.out.println("Failed to build twitter-sentiment-git.jar");
				System.exit(1);
			}

			log.info("Sending job to jobmanager at {}:{}", jmHost, jmPort);
			return StreamExecutionEnvironment.createRemoteEnvironment(
					jmHost, jmPort, "target/twitter-sentiment-git.jar");
		}
	}

	private static void printUsage() {
		System.err.println("Parameters: <jobmanager-host>:<port> <output-file> <profile-name>");
		System.err.println("Run local test cluster with ,,local'' as jobmanager host and empty port.");
		System.err.printf("Available profiles: %s\n",
				Arrays.toString(TwitterSentimentJobProfile.PROFILES.keySet().toArray()));
	}

	private static <T> DataStream<String> dumpStatistics(DataStream<T> stream, final String outputText, WindowingHelper windowPolicy) {
		return stream.map(new MapToOneTask<T>())
				.window(windowPolicy).sum().flatten()
				.map(new MapFunction<Long, String>() {
					@Override
					public String map(Long value) throws Exception {
						return String.format(outputText, value);
					}
				});
	}
}
