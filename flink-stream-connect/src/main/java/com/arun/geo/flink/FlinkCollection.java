package com.arun.geo.flink;

import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;



public class FlinkCollection {

	public static void main(String[] args) {

		Configuration config = new Configuration();
		config.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);
		// set up streaming execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<Tuple2<String, Integer>> myInts = env.fromElements(new Tuple2<String, Integer>("Noida", 10),
				new Tuple2<String, Integer>("Delhi", 12), new Tuple2<String, Integer>("Kanpur", 14),
				new Tuple2<String, Integer>("Agra", 18), new Tuple2<String, Integer>("Gorkhapur", 11),
				new Tuple2<String, Integer>("Nagpur", 20),new Tuple2<String, Integer>("Noida", 10),
				new Tuple2<String, Integer>("Delhi", 12), new Tuple2<String, Integer>("Kanpur", 14),
				new Tuple2<String, Integer>("Agra", 18), new Tuple2<String, Integer>("Gorkhapur", 11),
				new Tuple2<String, Integer>("Nagpur", 20));
	   DataStream<Tuple2<String, Integer>> cityPinCode = env.fromElements(new Tuple2<String, Integer>("Noida", 201301),
				new Tuple2<String, Integer>("Delhi", 200091), new Tuple2<String, Integer>("Kanpur", 20141),
				new Tuple2<String, Integer>("Agra", 2019), new Tuple2<String, Integer>("Gorkhapur", 20111),
				new Tuple2<String, Integer>("Nagpur", 2028),new Tuple2<String, Integer>("Noida", 201302),
				new Tuple2<String, Integer>("Delhi", 200091), new Tuple2<String, Integer>("Kanpur", 20142),
				new Tuple2<String, Integer>("Agra", 2018), new Tuple2<String, Integer>("Gorkhapur", 20112),
				new Tuple2<String, Integer>("Nagpur", 2029));
		
	   
		/*myInts.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
						private static final long serialVersionUID = 1L;
			
						@Override
						public String getKey(Tuple2<String, Integer> value) throws Exception {
							return value.f0;
						}
					   }
				).connect(cityPinCode.
			keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
						private static final long serialVersionUID = 1L;
			
						@Override
						public String getKey(Tuple2<String, Integer> value) throws Exception {
							return value.f0;
						}
					   })
		      ).flatMap(new EnrichmentFunction()).print();*/
		myInts.keyBy(myint ->myint.f0).connect(cityPinCode.keyBy(city ->city.f0)).flatMap(new EnrichmentFunction()).print();

		try {
			env.execute("Ride Count");
		} catch (Exception e) {

			e.printStackTrace();
		}
	}

	public static class EnrichmentFunction extends
			RichCoFlatMapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<String, Tuple2<Integer, Integer>>> {
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		

		
		 private transient ValueState<Tuple2<String, Integer>> sum;
		
		@Override
		public void open(Configuration config) throws Exception {
			
			ValueStateDescriptor<Tuple2<String, Integer>> descriptor =
	                new ValueStateDescriptor<>(
	                        "average", // the state name
	                        TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {}), // type information
	                        Tuple2.of("", 0)); // default value of the state, if nothing was set
			//descriptor.enableTimeToLive(ttlConfig);
	        sum = getRuntimeContext().getState(descriptor);
		}

		@Override
		public void flatMap1(Tuple2<String, Integer> arg0,
				Collector<Tuple2<String, Tuple2<Integer, Integer>>> collector) throws Exception {
				collector.collect(new Tuple2<>(arg0.f0, new Tuple2<>(arg0.f1,sum.value().f1)));
				System.out.println("1-"+sum.value().f1);
		}

		@Override
		public void flatMap2(Tuple2<String, Integer> arg0,
				Collector<Tuple2<String, Tuple2<Integer, Integer>>> collector) throws Exception {
			Tuple2<String, Integer> current=sum.value();
			current.f1=arg0.f1;
			sum.update(current);
			System.out.println("2-"+sum.value().f1);
		}


	}
	public static class generateData{
		
		public DataStream<Tuple2<String, Integer>> getData(StreamExecutionEnvironment env){
			//env.
			return null;
		}
	}
	}
