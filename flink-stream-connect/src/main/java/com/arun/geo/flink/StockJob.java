package com.arun.geo.flink;

import java.util.ArrayList;
import java.util.Arrays;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class StockJob {

	private static final ArrayList<String> SYMBOLS = new ArrayList<String>(
			Arrays.asList("SPX", "FTSE", "DJI", "DJT", "BUX", "DAX", "GOOG"));

	public static void main(String[] args) {
		Configuration config = new Configuration();
		config.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStream<StockPrice> SPX_stream = env.addSource(new StockSource("SPX", 10));
		
		DataStream<StockPrice> DJI_stream = env.addSource(new StockSource("DJI", 20));
		
		DataStream<DealPrice> DJI_stream1 = env.addSource(new DealSource("DJI", 30));
		
		
		DataStream<Tuple2<StockPrice, StockPrice>> enrichedRides =DJI_stream.keyBy("symbol").connect(DJI_stream1.keyBy("symbol")).flatMap(new EnrichmentFunction());
		enrichedRides.print();
		
		
		/*SPX_stream.assignTimestampsAndWatermarks(new TimeLagWatermarkGenerator());
		// SPX_stream.map(strm ->strm.toString()).print();
		SPX_stream.keyBy("symbol").filter(new StockFilter()).windowAll(TumblingEventTimeWindows.of(Time.seconds(2))).max("price").print();
		//SPX_stream.keyBy("symbol").filter(new StockFilter()).print();
		 */	
		
		try {
			env.execute("Ride Count");
		} catch (Exception e) {

			e.printStackTrace();
		}
	}

	public static class EnrichmentFunction extends RichCoFlatMapFunction<StockPrice, DealPrice, Tuple2<StockPrice, StockPrice>> {
		// keyed, managed state
		private ValueState<StockPrice> stockState;
		private ValueState<DealPrice> dealState;

		@Override
		public void open(Configuration config) {
			stockState = getRuntimeContext().getState(new ValueStateDescriptor<>("saved ride", StockPrice.class));
			dealState = getRuntimeContext().getState(new ValueStateDescriptor<>("saved fare", DealPrice.class));
		}

		@Override
		public void flatMap1(StockPrice stock, Collector<Tuple2<StockPrice, StockPrice>> out) throws Exception {
			System.out.println("map1");
			DealPrice deal = dealState.value();
			out.collect(new Tuple2(stock, deal));
			/*if (deal != null) {
				dealState.clear();
				
				out.collect(new Tuple2(stock, deal));
			} else {
				stockState.update(stock);
			}*/
		}

		@Override
		public void flatMap2(DealPrice deal, Collector<Tuple2<StockPrice, StockPrice>> out) throws Exception {
			System.out.println("map2");
			dealState.update(deal);
			/*StockPrice stock = stockState.value();
			if (stock != null) {
				stockState.clear();
				
				out.collect(new Tuple2(stock, deal));
			} else {
				dealState.update(deal);
			}*/
		}
	}
	
	public static class TimeLagWatermarkGenerator implements AssignerWithPeriodicWatermarks<StockPrice> {

		private final long maxTimeLag = 5000; // 5 seconds

		@Override
		public long extractTimestamp(StockPrice element, long previousElementTimestamp) {
			return element.getCreationTime();
		}

		@Override
		public Watermark getCurrentWatermark() {
			// return the watermark as current time minus the maximum time lag
			return new Watermark(System.currentTimeMillis() - maxTimeLag);
		}
	}
	public static class StockFilter implements FilterFunction<StockPrice> {
		@Override
		public boolean filter(StockPrice stockPrice) throws Exception {
			if (stockPrice.price > 0) {
				return true;
			} else {
				return false;
			}

		}
	}

	public class MyProcessWindowFunction extends AllWindowedStream<StockPrice, TimeWindow> {

		/*
		 * @Override public void process(String key, Context context,
		 * Iterable<Tuple2<String, Long>> input, Collector<String> out) { long
		 * count = 0; for (Tuple2<String, Long> in: input) { count++; }
		 * out.collect("Window: " + context.window() + "count: " + count); }
		 */

		public MyProcessWindowFunction(DataStream<StockPrice> input,
				WindowAssigner<? super StockPrice, TimeWindow> windowAssigner) {
			super(input, windowAssigner);
			// TODO Auto-generated constructor stub
		}

		/*@Override
		public void process(String key, ProcessWindowFunction<StockPrice, String, String, TimeWindow>.Context context,
				Iterable<StockPrice> input, Collector<String> out) throws Exception {
			long count = 0; 
			for (StockPrice in: input) { 
				count++; 
				}
			 out.collect("Window: " + context.window() + "count: " + count); 
		}*/

	}
}
