package de.tuberlin.cit.test.twittersentiment.profile;

import de.tuberlin.cit.test.twittersentiment.profile.task.HotTopicsRecognitionProfile;
import de.tuberlin.cit.test.twittersentiment.profile.task.TaskProfile;

import java.io.Serializable;
import java.util.HashMap;

public class TwitterSentimentJobProfile implements Serializable {

	public static class ParallelismProfile {

		public final String name;

		public final HotTopicsRecognitionProfile hotTopicsRecognition;
		public final TaskProfile filter;
		public final TaskProfile sentiment;

		public final static ParallelismProfile WALLY8_PARA_PROFILE = new ParallelismProfile(
				"wally8_para",
				new HotTopicsRecognitionProfile(10, 4, 10, 2, 1, 1000, 50),
				new TaskProfile(10, 4, 10, 1, 1),
				new TaskProfile(10, 4, 10, 1, 1));

		public final static ParallelismProfile WALLY50_PARA_PROFILE = new ParallelismProfile(
				"wally50_para",
				new HotTopicsRecognitionProfile(100, 4, 65, 5, 1, 1000, 50),
				new TaskProfile(100, 4, 65, 2, 1),
				new TaskProfile(100, 4, 65, 2, 1));


		public final static ParallelismProfile LOCAL_DUALCORE_PARA_PROFILE = new ParallelismProfile(
				"local_dualcore_para",
				new HotTopicsRecognitionProfile(1, 4, 1, 1, 1, 500, 40),
				new TaskProfile(1, 4, 1, 1, 1),
				new TaskProfile(1, 4, 1, 1, 1));

		public ParallelismProfile(String name,
				HotTopicsRecognitionProfile hotTopicsRecognition,
				TaskProfile filter,
				TaskProfile sentiment) {

			if (PROFILES.containsKey(name)) {
				throw new IllegalArgumentException("Profile name " + name
						+ " already reserved");
			}

			this.name = name;
			this.hotTopicsRecognition = hotTopicsRecognition;
			this.filter = filter;
			this.sentiment = sentiment;
		}
	}


	public static class LoadGenerationProfile implements Serializable {

		public final String name;

		public final int warmupPhaseDurationMillis;
		public final int incrementPhaseDurationMillis;
		public final int plateauPhaseDurationMillis;
		public final int decrementPhaseDurationMillis;
		public final int finalPhaseDurationMillis;

		public final int minEmitsPerSecond;
		public final int maxEmitsPerSecond;

		public final int incrementPhaseSteps;
		public final int decrementPhaseSteps;

		public static final LoadGenerationProfile WALLY_LOAD_PROFILE = new LoadGenerationProfile("wally_load",
				120 * 1000,
				420 * 1000,
				60 * 1000,
				420 * 1000,
				30 * 1000,
				500,
				12000,
				7,
				7);

		public static final LoadGenerationProfile LOCAL_LOAD_PROFILE = new LoadGenerationProfile("local_load",
				30 * 1000,
				60 * 1000,
				30 * 1000,
				60 * 1000,
				30 * 1000,
				2000,
				20000,
				6,
				6);

		public LoadGenerationProfile(String name,
				int warmupPhaseDurationMillis,
				int incrementPhaseDurationMillis,
				int plateauPhaseDurationMillis,
				int decrementPhaseDurationMillis,
				int finalPhaseDurationMillis,
				int minEmitsPerSecond,
				int maxEmitsPerSecond,
				int incrementPhaseSteps,
				int decrementPhaseSteps) {

			this.name = name;
			this.warmupPhaseDurationMillis = warmupPhaseDurationMillis;
			this.incrementPhaseDurationMillis = incrementPhaseDurationMillis;
			this.plateauPhaseDurationMillis = plateauPhaseDurationMillis;
			this.decrementPhaseDurationMillis = decrementPhaseDurationMillis;
			this.finalPhaseDurationMillis = finalPhaseDurationMillis;
			this.minEmitsPerSecond = minEmitsPerSecond;
			this.maxEmitsPerSecond = maxEmitsPerSecond;
			this.incrementPhaseSteps = incrementPhaseSteps;
			this.decrementPhaseSteps = decrementPhaseSteps;
		}


		public long getTotalDuration() {
			return warmupPhaseDurationMillis + incrementPhaseDurationMillis
					+ plateauPhaseDurationMillis + decrementPhaseDurationMillis
					+ finalPhaseDurationMillis;
		}
	}

	public final String name;

	public final ParallelismProfile paraProfile;

	public final LoadGenerationProfile loadGenProfile;

	public final static HashMap<String, TwitterSentimentJobProfile> PROFILES = new HashMap<>();


	public final static TwitterSentimentJobProfile WALLY8 = new TwitterSentimentJobProfile(
			"wally8", ParallelismProfile.WALLY8_PARA_PROFILE, LoadGenerationProfile.WALLY_LOAD_PROFILE);

	public final static TwitterSentimentJobProfile WALLY50 = new TwitterSentimentJobProfile(
			"wally50", ParallelismProfile.WALLY50_PARA_PROFILE, LoadGenerationProfile.WALLY_LOAD_PROFILE);

	public final static TwitterSentimentJobProfile LOCAL_DUALCORE = new TwitterSentimentJobProfile(
			"local_dualcore",
			ParallelismProfile.LOCAL_DUALCORE_PARA_PROFILE,
			LoadGenerationProfile.LOCAL_LOAD_PROFILE);

	public TwitterSentimentJobProfile(String name, ParallelismProfile paraProfile, LoadGenerationProfile loadGenProfile) {
		this.name = name;
		this.paraProfile = paraProfile;
		this.loadGenProfile = loadGenProfile;

		PROFILES.put(name, this);
	}
}