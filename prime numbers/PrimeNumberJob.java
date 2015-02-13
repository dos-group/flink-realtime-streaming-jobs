package org.apache.flink;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.function.source.RichSourceFunction;
import org.apache.flink.streaming.api.windowing.helper.Count;
import org.apache.flink.util.Collector;

import java.math.BigInteger;
import java.util.Random;

/**
 * Job that generates random BigInteger values, adds 50 of them up and
 * outputs all the primes.
 *
 */
public class PrimeNumberJob {

	// **********************************************************************
	// PROGRAMM
	// **********************************************************************

	public static void main(String[] args) throws Exception {

		if (!parseParameters(args)) {
			return;
		}

		// set up the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment
				.getExecutionEnvironment();
//				createLocalEnvironment().setBufferTimeout(1);

		// get input data
		DataStream<BigInteger> dataStream = env.addSource(new RandomNumberStream())
				.distribute()
				.window(Count.of(50))
				.groupBy(new MyKey())
				.reduce(new SumReducer())
				.filter(new PrimeFilter())
				; // TODO: make Prime check

		if (fileOutput) {
			dataStream.writeAsText(outputPath, 1);
		} else {
			dataStream.print();
		}

//		print results on local machine
//		outStream.print();
//		dataStream.writeAsText("file:///path/to/file.txt);

		// execute program
		env.execute("Flink Java API Skeleton");
	}

	// **********************************************************************
	// USER FUNCTIONS
	// **********************************************************************

	private final static long SLEEP_TIME = 1;

	public static final class RandomNumberStream extends RichSourceFunction<BigInteger> {
		private static final long serialVersionUID = 1L;

		private transient Random random;

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
			random = new Random();
		}

		@Override
		public void invoke(Collector<BigInteger> out) throws Exception {
			while (true) {
				// new random number
				BigInteger number = BigInteger.valueOf(random.nextInt()).abs();
				out.collect(number);
				Thread.sleep(SLEEP_TIME);
			}
		}
	}

	public static final class SumReducer implements ReduceFunction<BigInteger> {

		@Override
		public BigInteger reduce(BigInteger val1, BigInteger val2) throws Exception {
//			System.out.print("Reduce, ");
//			System.out.println("val1(" + val1 + ") + val2(" + val2 + ")");
			return val1.add(val2);
		}
	}

	public static final class PrimeFilter implements FilterFunction<BigInteger> {
		@Override
		public boolean filter(BigInteger value) throws Exception {
			return value.isProbablePrime(10); // probability that value is prime is 0.9990234375
		}
	}

	private static final class MyKey implements KeySelector<BigInteger, Integer> {
		private static final long serialVersionUID = 1L;

		@Override
		public Integer getKey(BigInteger value) throws Exception {
			return 0;
		}

	}

	// *************************************************************************
	// UTIL METHODS
	// *************************************************************************

	private static boolean fileOutput = false;
	private static String outputPath;

	private static boolean parseParameters(String[] args) {

		if (args.length > 0) {
			// parse input arguments
			fileOutput = true;
			if (args.length == 1) {
				outputPath = args[0];
			} else {
				System.err.println("Usage: PrimeNumberJob <result path>");
				return false;
			}
		} else {
			System.out.println("Executing PrimeNumberJob with randomly generated data.");
			System.out.println("  Provide parameter to write to file.");
			System.out.println("  Usage: PrimeNumberJob <result path>");
		}
		return true;
	}
}
