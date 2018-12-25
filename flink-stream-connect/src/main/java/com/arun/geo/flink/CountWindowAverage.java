package com.arun.geo.flink;

import org.apache.commons.math3.fitting.leastsquares.LeastSquaresProblem.Evaluation;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

public class CountWindowAverage {

	public static void main(String[] args) {

		Configuration config = new Configuration();
		config.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);
		// set up streaming execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.fromElements(Tuple2.of(1L, 3L), Tuple2.of(1L, 5L), Tuple2.of(1L, 7L), Tuple2.of(1L, 4L), Tuple2.of(1L, 2L))
        .keyBy(0)
        .flatMap(new CountWindowAverageFun())
        .print();
		
		/*DataStream<Tuple2<Long, Long>> stream1=env.fromElements(Tuple2.of(1L, 3L), Tuple2.of(1L, 5L), Tuple2.of(1L, 7L), Tuple2.of(1L, 4L), Tuple2.of(4L, 2L));
		DataStream<Tuple2<Long, Long>> stream12=env.fromElements(Tuple2.of(1L, 4L), Tuple2.of(1L, 6L), Tuple2.of(1L, 8L), Tuple2.of(1L, 5L), Tuple2.of(1L, 10L));
		stream1.keyBy(strm1 ->strm1.f0).connect(stream12.keyBy(strm2 ->strm2.f0)).flatMap(new SumCount()).print();
	*/	try {
			env.execute("Ride Count");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static class SumCount extends RichCoFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>,Tuple2<Long, Long>>{

		/**
		 * 
		 */
		private transient ValueState<Tuple2<Long, Long>> sum;

		private static final long serialVersionUID = 5452580804159117144L;

		@Override
		public void flatMap1(Tuple2<Long, Long> arg0, Collector<Tuple2<Long, Long>> arg1) throws Exception {
			Tuple2<Long, Long> data=sum.value();
			System.out.println(data.f1);
			arg1.collect(Tuple2.of(arg0.f0, data.f1+arg0.f1));
		}

		@Override
		public void flatMap2(Tuple2<Long, Long> arg0, Collector<Tuple2<Long, Long>> arg1) throws Exception {
			sum.update(arg0);
			System.out.println("Updated 2 "+sum.value());
		}
		 @Override
		    public void open(Configuration config) {
			 	System.out.println("open");
			 	ValueStateDescriptor<Tuple2<Long, Long>> descriptor =
		                new ValueStateDescriptor<>(
		                        "average", // the state name
		                        TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {}), // type information
		                        Tuple2.of(0L, 0L));
			 	System.out.println(config.keySet());
		        // default value of the state, if nothing was set
		        sum = getRuntimeContext().getState(descriptor);
		    }
		
	}
	
	
	public static class CountWindowAverageFun extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> {

	    /**
		 * 
		 */
		private static final long serialVersionUID = 3793994830111953616L;
		/**
	     * The ValueState handle. The first field is the count, the second field a running sum.
	     */
	    private transient ValueState<Tuple2<Long, Long>> sum;

	    @Override
	    public void flatMap(Tuple2<Long, Long> input, Collector<Tuple2<Long, Long>> out) throws Exception {

	        // access the state value
	        Tuple2<Long, Long> currentSum = sum.value();

	        // update the count
	        currentSum.f0 += 1;

	        // add the second field of the input value
	        currentSum.f1 += input.f1;

	        // update the state
	        sum.update(currentSum);

	        // if the count reaches 2, emit the average and clear the state
	        if (currentSum.f0 >= 2) {
	            out.collect(new Tuple2<>(input.f0, currentSum.f1 / currentSum.f0));
	            sum.clear();
	        }
	    }

	    @Override
	    public void open(Configuration config) {
	    	System.out.println(config.keySet());
	        ValueStateDescriptor<Tuple2<Long, Long>> descriptor =
	                new ValueStateDescriptor<>(
	                        "average", // the state name
	                        TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {}), // type information
	                        Tuple2.of(0L, 0L)); // default value of the state, if nothing was set
	        sum = getRuntimeContext().getState(descriptor);
	    }
	}
}
 
