package de.tuberlin.cit.test.queuebehavior;

import java.util.HashMap;

public class TestQueueBehaviorJobProfile {
	
	public static class ParallelismProfile {
		
		public final String name;
		
		/**
		 * Degree of parallelism of inner tasks
		 */
		public final int innerTaskDop;

		/**
		 * Number of inner tasks per instance.
		 */
		public final int innerTaskDopPerInstance;

		/**
		 * Degree of parallelism for outer tasks (source, sink).
		 */
		public final int outerTaskDop;

		/**
		 * Number of outer tasks per instance.
		 */
		public final int outerTaskDopPerInstance;
		
		public final static ParallelismProfile WALLY200_PARA_PROFILE = new ParallelismProfile(
				"wally200_para", 800, 4, 200, 1);

		public final static ParallelismProfile WALLY199_PARA_PROFILE = new ParallelismProfile(
				"wally199_para", 796, 4, 199, 1);
		
		public final static ParallelismProfile WALLY50_PARA_PROFILE = new ParallelismProfile(
				"wally50_para", 200, 4, 50, 1);
		
		public final static ParallelismProfile WALLY49_PARA_PROFILE = new ParallelismProfile(
				"wally49_para", 196, 4, 49, 1);

		public final static ParallelismProfile LOCAL_DUALCORE_PARA_PROFILE = new ParallelismProfile(
				"local_dualcore_para", 2, 2, 1, 1);
		
		public ParallelismProfile(String name, 
				int innerTaskDop,
				int innerTaskDopPerInstance, 
				int outerTaskDop,
				int outerTaskDopPerInstance) {

			this.name = name;
			this.innerTaskDop = innerTaskDop;
			this.innerTaskDopPerInstance = innerTaskDopPerInstance;
			this.outerTaskDop = outerTaskDop;
			this.outerTaskDopPerInstance = outerTaskDopPerInstance;

			if (PROFILES.containsKey(name)) {
				throw new IllegalArgumentException("Profile name " + name
						+ " already reserved");
			}
			
		}
	}
	
	public static class LoadGenerationProfile {
		
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
				120 * 1000,
				10000,
				80000,
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
	
	public final static HashMap<String, TestQueueBehaviorJobProfile> PROFILES = new HashMap<>();
	
	public final static TestQueueBehaviorJobProfile WALLY200 = new TestQueueBehaviorJobProfile(
			"wally200", ParallelismProfile.WALLY200_PARA_PROFILE, LoadGenerationProfile.WALLY_LOAD_PROFILE);

	public final static TestQueueBehaviorJobProfile WALLY199 = new TestQueueBehaviorJobProfile(
			"wally199", ParallelismProfile.WALLY199_PARA_PROFILE, LoadGenerationProfile.WALLY_LOAD_PROFILE);
	
	public final static TestQueueBehaviorJobProfile WALLY50  = new TestQueueBehaviorJobProfile(
			"wally50", ParallelismProfile.WALLY50_PARA_PROFILE, LoadGenerationProfile.WALLY_LOAD_PROFILE);
	
	public final static TestQueueBehaviorJobProfile WALLY49 = new TestQueueBehaviorJobProfile(
			"wally49", ParallelismProfile.WALLY49_PARA_PROFILE, LoadGenerationProfile.WALLY_LOAD_PROFILE);

	public final static TestQueueBehaviorJobProfile LOCAL_DUALCORE = new TestQueueBehaviorJobProfile(
			"local_dualcore", 
			ParallelismProfile.LOCAL_DUALCORE_PARA_PROFILE, 
			LoadGenerationProfile.LOCAL_LOAD_PROFILE);

	public TestQueueBehaviorJobProfile(String name, ParallelismProfile paraProfile, LoadGenerationProfile loadGenProfile) {
		this.name = name;
		this.paraProfile = paraProfile;
		this.loadGenProfile = loadGenProfile;
		
		PROFILES.put(name, this);
	}
}
