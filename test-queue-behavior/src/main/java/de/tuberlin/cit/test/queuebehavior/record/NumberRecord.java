package de.tuberlin.cit.test.queuebehavior.record;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.types.Value;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigInteger;

/**
 * Do to the fact that BigInteger contains values without getter/setter, we have to implement the Value interface here.
 */
public class NumberRecord implements Value {

	public enum Primeness {
		PRIME, NOT_PRIME, UNKNOWN
	}

	private BigInteger number;
	
	private Primeness primeness;
	
	private long timestamp;

	public NumberRecord() {
		this.primeness = Primeness.UNKNOWN;
	}

	public BigInteger getNumber() {
		return number;
	}

	public void setNumber(BigInteger number) {
		this.number = number;
	}

	public Primeness getPrimeness() {
		return primeness;
	}

	public void setPrimeness(Primeness primeness) {
		this.primeness = primeness;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

    @Override
    public void write(DataOutputView out) throws IOException {
		out.writeLong(timestamp);
		byte[] numByte = this.number.toByteArray();
		out.writeInt(numByte.length);
		out.write(numByte);
		out.writeUTF(this.primeness.toString());
    }

    @Override
    public void read(DataInputView in) throws IOException {
        timestamp = in.readLong();
		byte[] numByte = new byte[in.readInt()];
		in.readFully(numByte);
		this.number = new BigInteger(numByte);
		this.primeness = Primeness.valueOf(in.readUTF());
    }
}
