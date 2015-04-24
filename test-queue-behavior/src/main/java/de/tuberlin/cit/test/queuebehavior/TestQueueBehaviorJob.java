package de.tuberlin.cit.test.queuebehavior;

import de.tuberlin.cit.test.queuebehavior.record.NumberRecord;
import de.tuberlin.cit.test.queuebehavior.task.LatencyLoggerSink;
import de.tuberlin.cit.test.queuebehavior.task.NumberSource;
import de.tuberlin.cit.test.queuebehavior.task.PrimeNumberTestTask;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

/**
 * This Flink job is intended for non-interactive cluster experiments with
 * Flink and designed to observe the effects of queueing.
 *
 * This Flink job generates large numbers (which is a fairly cheap operation)
 * at an increasing rate and tests them for primeness (which is compute
 * intensive).
 *
 * @author Bjoern Lohrmann
 * @author Sascha Wolke
 */
public class TestQueueBehaviorJob {
	private final static Logger log = LoggerFactory.getLogger(TestQueueBehaviorJob.class);

	public static void main(final String[] args) throws Exception {

		if (args.length != 3) {
			printUsage();
			System.exit(1);
			return;
		}

		String jmHost = args[0].split(":")[0];
		int jmPort = -1;
        if (jmHost.contains(":"))
            jmPort = Integer.parseInt(args[0].split(":")[1]);

		TestQueueBehaviorJobProfile profile = TestQueueBehaviorJobProfile.PROFILES.get(args[1]);
		if (profile == null) {
			System.err.printf("Unknown profile: %s\n", args[1]);
			printUsage();
			System.exit(1);
			return;
		}

		String latencyLogfile = args[2];

		final StreamExecutionEnvironment env;

		if (jmHost.equalsIgnoreCase("local")) {
			log.info("Running in local mode.");
			env = StreamExecutionEnvironment.getExecutionEnvironment();

		} else {
			log.info("Running in remote mode. Creating jar...");

			// Create jar file for job deployment
			Process p = Runtime.getRuntime().exec("mvn clean package");
			if (p.waitFor() != 0) {
				System.out.println("Failed to build test-queue-behavior.jar");
				System.exit(1);
			}

			env = StreamExecutionEnvironment.createRemoteEnvironment(
					jmHost, jmPort, "target/test-queue-behavior-git.jar");
		}


		// optional: env.setQosStatisticsReportInterval(12345);
		env.getStreamGraph().setChaining(false);

		// TODO: Use specific cluster/jobManager endpoint
		// TODO: taskDopPerInstance? elasticSettings?
		env.addSource(new NumberSource(profile.name)).setParallelism(profile.paraProfile.outerTaskDop)
				.map(new PrimeNumberTestTask()).setParallelism(profile.paraProfile.innerTaskDop)
				.beginLatencyConstraint(20)
				.flatMap(new DummyFlatMapper()).setParallelism(profile.paraProfile.innerTaskDop)
				.finishLatencyConstraint()
				.addSink(new LatencyLoggerSink(latencyLogfile)).setParallelism(profile.paraProfile.outerTaskDop);

		env.execute("Test Queue Behavior job");
	}

	private static void printUsage() {
		System.err.println("Parameters: <jobmanager-host>:<port> <profile-name> <latency-logfile>");
		System.err.println("Run local test cluster with ,,local'' as jobmanager host and empty port.");
		System.err.printf("Available profiles: %s\n",
				Arrays.toString(TestQueueBehaviorJobProfile.PROFILES.keySet().toArray()));
	}

	public static class DummyFlatMapper implements FlatMapFunction<NumberRecord, NumberRecord> {
		@Override
		public void flatMap(NumberRecord value, Collector<NumberRecord> out) throws Exception {
			out.collect(value);
		}
	}
}
