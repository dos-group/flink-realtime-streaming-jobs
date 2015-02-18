package de.tuberlin.cit.test.queuebehavior.task;

import java.io.IOException;
import java.util.Map;

import de.tuberlin.cit.test.queuebehavior.util.LatencyLog;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class LatencyLoggerBolt extends BaseRichBolt {

	private static final long serialVersionUID = 1L;
	
	private String logFile;

	private transient LatencyLog latLogger;
	
	public LatencyLoggerBolt() {
		this(LatencyLoggerSink.LATENCY_LOG_DEFAULT);
	}
	
	public LatencyLoggerBolt(String logFile) {
		this.logFile = logFile;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		try {
			this.latLogger = new LatencyLog(logFile);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void execute(Tuple testedNumberTuple) {
		long timestamp = testedNumberTuple.getLongByField("timestamp");
		latLogger.log(timestamp);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}
	
    @Override
    public void cleanup() {
    	latLogger.close();
    }    
}
