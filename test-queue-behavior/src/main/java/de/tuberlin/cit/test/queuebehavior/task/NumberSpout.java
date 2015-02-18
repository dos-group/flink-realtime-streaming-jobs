package de.tuberlin.cit.test.queuebehavior.task;

import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import de.tuberlin.cit.test.queuebehavior.TestQueueBehaviorJobProfile;
import de.tuberlin.cit.test.queuebehavior.TestQueueBehaviorJobProfile.LoadGenerationProfile;
import de.tuberlin.cit.test.queuebehavior.util.BlockingRandomNumberSource;
import de.tuberlin.cit.test.queuebehavior.util.BlockingRandomNumberSource.TimestampedNumber;

public class NumberSpout extends BaseRichSpout {
	
	private static final long serialVersionUID = 1L;

	public static final String PROFILE_PROPERTY_KEY = "numberspout.profile";
	
    private SpoutOutputCollector collector;
    
	private String profileName;

	private transient BlockingRandomNumberSource numberSource;

	private transient TimestampedNumber numHolder;

    public NumberSpout() {
        this("local_dualcore");
    }

    public NumberSpout(String profileName) {
    	this.profileName = profileName;
    }
        
	@SuppressWarnings("rawtypes")
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		
		this.collector = collector;
		LoadGenerationProfile profile = TestQueueBehaviorJobProfile.PROFILES.get(profileName).loadGenProfile;
		this.numberSource = new BlockingRandomNumberSource(profile);
		this.numHolder = new TimestampedNumber();
	}
    
    public void close() {
    }
        
	public void nextTuple() {
		try {
			TimestampedNumber toEmit = numberSource
					.createRandomNumberBlocking(numHolder);
			if (toEmit != null) {
				collector.emit(new Values(toEmit.timestamp, toEmit.number
						.toByteArray()));
			}
		} catch (InterruptedException e) {
		}
	}
    
    public void ack(Object msgId) {
    }

    public void fail(Object msgId) {
    }
    
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("timestamp", "numberBytes"));
    }
}
