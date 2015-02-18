package de.tuberlin.cit.test.queuebehavior.task;

import java.math.BigInteger;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class PrimeNumberTestBolt extends BaseRichBolt {
	private static final long serialVersionUID = 1L;

	private transient OutputCollector collector;

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map arg0, TopologyContext topoContext,
			OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple numberTuple) {
		byte[] numberBytes = numberTuple.getBinaryByField("numberBytes");
		long timestamp = numberTuple.getLongByField("timestamp");

		BigInteger number = new BigInteger(numberBytes);
		boolean isPrime = number.isProbablePrime(100);
		collector.emit(new Values(timestamp, numberBytes, isPrime));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("timestamp", "numberBytes", "isPrime"));
	}
}
