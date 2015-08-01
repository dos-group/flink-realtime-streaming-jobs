package de.tuberlin.cit.test.twittersentiment.util;

import de.tuberlin.cit.test.twittersentiment.profile.TwitterSentimentJobProfile;
import de.tuberlin.cit.test.twittersentiment.record.TweetRecord;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class LoadPhaseTweetSource extends TweetSource {
	public static final String PROFILE = "tweetsourcetask.profile";

	private static final Logger LOG = LoggerFactory.getLogger(LoadPhaseTweetSource.class);

	private TwitterSentimentJobProfile.LoadGenerationProfile profile;

	private enum LoadGenPhase {
		INITIAL_SLEEP, WARMUP, INCREMENT, PLATEAU, DECREMENT, COOLDOWN, DONE
	}

	private LoadGenPhase currPhase;

	private int currPhaseTotalSteps;

	private int currPhaseStep;

	private long currPhaseStepBeginTime;

	private long currPhaseStepEndTime;

	private long currPhaseStepDuration;

	private int currPhaseStepTotalEmits;

	private int currPhaseStepEmits;

	private int sleepTime = 0;

	public LoadPhaseTweetSource(int port, String profileName) {
		super(port);
		this.profile = TwitterSentimentJobProfile.PROFILES.get(profileName).loadGenProfile;
	}

	public LoadPhaseTweetSource(String profileName) {
		this(DEFAULT_TCP_SERVER_PORT, profileName);
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		this.currPhase = LoadGenPhase.INITIAL_SLEEP;
		this.currPhaseStep = 0;
		this.currPhaseStepBeginTime = -1;
		this.currPhaseStepEndTime = -1;

//        String profileName = taskConfiguration.getString(PROFILE, null);
//        this.profile = TwitterSentimentJobProfile.PROFILES.get(profileName).loadGenProfile;
	}

	@Override
	public TweetRecord getTweet()
			throws InterruptedException, IOException {

		long now = System.currentTimeMillis();

		TweetRecord tweet = null;

		if (now < currPhaseStepEndTime) {
			tweet = nextTweetBlocking(now);
		} else {
			if (currPhase == LoadGenPhase.INITIAL_SLEEP) {
				// need to set this because configurePhaseStep() depends on it
				currPhaseStepEndTime = now + 5000;
				Thread.sleep(5000);
				// now in warmup phase
				transitionToNextPhase();

				now = System.currentTimeMillis();
				tweet = nextTweetBlocking(now);
			} else if (currPhase == LoadGenPhase.DONE) {
				Thread.sleep(100);
			} else {
				logStepStats(now);
				transitionToNextPhase();
				tweet = nextTweetBlocking(now);
			}
		}

		return tweet;
	}

	private void transitionToNextPhase() throws InterruptedException {
		switch (currPhase) {
			case INITIAL_SLEEP:
				initWarmupPhase(System.currentTimeMillis());
				break;
			case WARMUP:
				initIncrementPhase();
				break;
			case INCREMENT:
				currPhaseStep++;
				if (currPhaseStep < currPhaseTotalSteps) {
					initCurrIncrementStep();
				} else {
					initPlateauPhase();
				}
				break;
			case PLATEAU:
				initDecrementPhase();
				break;
			case DECREMENT:
				currPhaseStep++;
				if (currPhaseStep < currPhaseTotalSteps) {
					initCurrDecrementStep();
				} else {
					initCooldownPhase();
				}
				break;
			case COOLDOWN:
				initDonePhase();
				break;
			case DONE:
				break;
			default:
				throw new RuntimeException("This should never happen");
		}
	}

	private void initDonePhase() {
		currPhase = LoadGenPhase.DONE;
		currPhaseTotalSteps = -1;
		currPhaseStep = -1;

		currPhaseStepBeginTime = System.currentTimeMillis();
		currPhaseStepEndTime = -1;
	}

	private void initCooldownPhase() {
		currPhase = LoadGenPhase.COOLDOWN;
		currPhaseTotalSteps = 1;
		currPhaseStep = 0;
		configurePhaseStep(profile.finalPhaseDurationMillis,
				profile.minEmitsPerSecond);
	}

	private void initDecrementPhase() {
		currPhase = LoadGenPhase.DECREMENT;
		currPhaseTotalSteps = profile.decrementPhaseSteps;
		currPhaseStep = 0;
		initCurrDecrementStep();
	}

	private void initCurrDecrementStep() {
		long stepDuration = profile.decrementPhaseDurationMillis
				/ currPhaseTotalSteps;

		int stepEmitsPerSecond = profile.maxEmitsPerSecond
				- (int) Math.round(currPhaseStep
				* (profile.maxEmitsPerSecond - profile.minEmitsPerSecond)
				/ ((double) currPhaseTotalSteps));

		configurePhaseStep(stepDuration, stepEmitsPerSecond);
	}

	private void initPlateauPhase() {
		currPhase = LoadGenPhase.PLATEAU;
		currPhaseTotalSteps = 1;
		currPhaseStep = 0;
		configurePhaseStep(profile.plateauPhaseDurationMillis,
				profile.maxEmitsPerSecond);

	}

	private void configurePhaseStep(long stepDurationMillies,
			int emitsPerSecond) {

		currPhaseStepBeginTime = currPhaseStepEndTime;
		currPhaseStepEndTime = currPhaseStepBeginTime + stepDurationMillies;

		currPhaseStepDuration = stepDurationMillies;
		currPhaseStepTotalEmits = (int) Math.round(emitsPerSecond
				* (stepDurationMillies / 1000.0));
		currPhaseStepEmits = 0;
		sleepTime = 0;

		LOG.info(String.format("%s (step %d): Emitting %d recs/sec for %.1f sec",
				currPhase.toString(),
				currPhaseStep + 1,
				emitsPerSecond, stepDurationMillies / 1000.0));

	}

	private void initWarmupPhase(long now) {
		currPhase = LoadGenPhase.WARMUP;
		currPhaseTotalSteps = 1;
		currPhaseStep = 0;
		configurePhaseStep(profile.warmupPhaseDurationMillis,
				profile.minEmitsPerSecond);
	}

	private void initIncrementPhase() {
		currPhase = LoadGenPhase.INCREMENT;
		currPhaseTotalSteps = profile.incrementPhaseSteps;
		currPhaseStep = 0;
		initCurrIncrementStep();
	}

	private void initCurrIncrementStep() {
		long stepDuration = profile.incrementPhaseDurationMillis
				/ currPhaseTotalSteps;

		int stepEmitsPerSecond = profile.minEmitsPerSecond
				+ (int) Math.round((currPhaseStep + 1)
				* (profile.maxEmitsPerSecond - profile.minEmitsPerSecond)
				/ ((double) currPhaseTotalSteps));

		configurePhaseStep(stepDuration, stepEmitsPerSecond);
	}

	private TweetRecord nextTweetBlocking(long now) throws InterruptedException {

		if (currPhaseStepEmits % 3 == 0) {
			adjustSleepTime(now);
		}
		currPhaseStepEmits++;

		if (sleepTime > 0) {
			Thread.sleep(sleepTime);
		}

		return queue.take();
	}

	private void adjustSleepTime(long now) {
		int expectedEmitted = (int) (currPhaseStepTotalEmits * ((now - currPhaseStepBeginTime) / ((double) currPhaseStepDuration)));
		if (currPhaseStepEmits > expectedEmitted) {
			sleepTime++;
		} else if (currPhaseStepEmits < expectedEmitted && sleepTime > 0) {
			sleepTime--;
		}
	}

	private void logStepStats(long now) {
		double secsPassed = (now - currPhaseStepBeginTime) / 1000.0;
		int attemptedEmitsPerSecond = (int) (currPhaseStepTotalEmits / ((currPhaseStepEndTime - currPhaseStepBeginTime) / 1000.0));
		double actualEmitsPerSecond = currPhaseStepEmits / secsPassed;

		LOG.info(String
				.format("%s (step %d): Emitted %.1f recs/sec for %.1f sec (%d records total)",
						currPhase.toString(),
						currPhaseStep + 1,
						actualEmitsPerSecond,
						secsPassed,
						currPhaseStepEmits));

		LOG.info(String.format("qb: %d;%d;%d", currPhaseStepBeginTime / 1000,
				attemptedEmitsPerSecond, (int) actualEmitsPerSecond));
	}

	@Override
	public void close() throws Exception {
		super.close();
		logStepStats(System.currentTimeMillis());
	}
}