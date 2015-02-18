package de.tuberlin.cit.test.queuebehavior.task;

import de.tuberlin.cit.test.queuebehavior.record.NumberRecord;
import de.tuberlin.cit.test.queuebehavior.record.NumberRecord.Primeness;
import org.apache.flink.api.common.functions.MapFunction;

public class PrimeNumberTestTask implements MapFunction<NumberRecord, NumberRecord> {

    @Override
    public NumberRecord map(NumberRecord num) throws Exception {
        if (num.getNumber().isProbablePrime(100)) {
            num.setPrimeness(Primeness.PRIME);
        } else {
            num.setPrimeness(Primeness.NOT_PRIME);
        }
        return num;
    }
}
