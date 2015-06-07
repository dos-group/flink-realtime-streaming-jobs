package de.tuberlin.cit.test.queuebehavior.task;

import de.tuberlin.cit.test.queuebehavior.TestQueueBehaviorJobProfile;
import de.tuberlin.cit.test.queuebehavior.TestQueueBehaviorJobProfile.LoadGenerationProfile;
import de.tuberlin.cit.test.queuebehavior.record.NumberRecord;
import de.tuberlin.cit.test.queuebehavior.util.BlockingRandomNumberSource;
import de.tuberlin.cit.test.queuebehavior.util.BlockingRandomNumberSource.TimestampedNumber;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO: Use OpportunisticRoundRobinChannelSelector
public class NumberSource extends RichSourceFunction<NumberRecord> {

    private Logger log = LoggerFactory.getLogger(NumberSource.class);

	public static final String PROFILE_PROPERTY_DEFAULT = TestQueueBehaviorJobProfile.LOCAL_DUALCORE.name;

    private final String profileName;

    private volatile boolean isRunning = false;

    public NumberSource(String profileName) {
        this.profileName = profileName;
    }

    public NumberSource() {
        this(PROFILE_PROPERTY_DEFAULT);
    }

    // TODO:
//		this.out = new RecordWriter<NumberRecord>(this, NumberRecord.class,
//				new OpportunisticRoundRobinChannelSelector<NumberRecord>(
//						getCurrentNumberOfSubtasks(), getIndexInSubtaskGroup()));


    @Override
    public void run(Object checkpointLock, Collector<NumberRecord> out) throws Exception {
        LoadGenerationProfile profile = TestQueueBehaviorJobProfile.PROFILES
                .get(this.profileName)
                .loadGenProfile;

        BlockingRandomNumberSource rndSource = new BlockingRandomNumberSource(profile);
        TimestampedNumber numHolder = new TimestampedNumber();
        this.isRunning = true;

        TimestampedNumber toEmit;
        while (isRunning && (toEmit = rndSource.createRandomNumberBlocking(numHolder)) != null) {
            NumberRecord record = new NumberRecord();
            record.setNumber(toEmit.number);
            record.setTimestamp(toEmit.timestamp);
            out.collect(record);
        }
    }

	@Override
	public void cancel() {
        this.isRunning = false;
    }
}
