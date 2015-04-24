package de.tuberlin.cit.test.queuebehavior.task;

import de.tuberlin.cit.test.queuebehavior.record.NumberRecord;
import de.tuberlin.cit.test.queuebehavior.util.LatencyLog;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

public class LatencyLoggerSink extends RichSinkFunction<NumberRecord> {

	public final static String LATENCY_LOG_DEFAULT = "/tmp/qos_statistics_receiver";

    private final String logFile;

    private LatencyLog latLogger;

    public LatencyLoggerSink(String logFile) {
        this.logFile = logFile;
    }

    public LatencyLoggerSink() {
        this(LATENCY_LOG_DEFAULT);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.latLogger = new LatencyLog(logFile);
    }

    @Override
    public void invoke(NumberRecord record) {
        latLogger.log(record.getTimestamp());
    }

    @Override
    public void close() throws Exception {
        super.close();

        if (this.latLogger != null)
            this.latLogger.close();
    }
}
