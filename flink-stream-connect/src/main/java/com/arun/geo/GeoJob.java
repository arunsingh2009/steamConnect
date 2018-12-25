package com.arun.geo;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

import com.arun.geo.pojo.Fence;
import com.arun.geo.pojo.Telemetry;
import com.arun.geo.source.FenceSource;
import com.arun.geo.source.TelemetrySource;
import com.google.gson.JsonObject;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;

public class GeoJob {

	public static void main(String[] args) {

		Configuration config = new Configuration();
		config.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStream<Telemetry> geostream1 = env.addSource(new TelemetrySource());
		DataStream<Telemetry> geostream2 = env.addSource(new TelemetrySource());

		DataStream<Fence> fenceStream = env.addSource(new FenceSource());

		// geostream1.keyBy(tem ->
		// tem.getHeader().getAssetId()).connect(geostream2.keyBy(tem ->
		// tem.getHeader().getAssetId())).flatMap(new
		// EnrichmentFunction()).print();
		// geostream.print();

		geostream1.keyBy(geo -> geo.getHeader().getAssetId()).connect(fenceStream.keyBy(geo -> geo.getAssetId()))
				.flatMap(new GeoFenceFunction()).print();
		try {
			env.execute("Geo Stream");
		} catch (Exception e) {

			e.printStackTrace();
		}
	}

	public static class EnrichmentFunction
			extends RichCoFlatMapFunction<Telemetry, Telemetry, Tuple2<Telemetry, Telemetry>> {
		// keyed, managed state
		private ValueState<Telemetry> stockState;
		private ValueState<Telemetry> dealState;

		@Override
		public void open(Configuration config) {
			stockState = getRuntimeContext().getState(new ValueStateDescriptor<>("saved ride", Telemetry.class));
			dealState = getRuntimeContext().getState(new ValueStateDescriptor<>("saved fare", Telemetry.class));
		}

		@Override
		public void flatMap1(Telemetry stock, Collector<Tuple2<Telemetry, Telemetry>> out) throws Exception {
			System.out.println("map1");
			Telemetry deal = dealState.value();
			out.collect(new Tuple2(stock, deal));
			/*
			 * if (deal != null) { dealState.clear();
			 * 
			 * out.collect(new Tuple2(stock, deal)); } else {
			 * stockState.update(stock); }
			 */
		}

		@Override
		public void flatMap2(Telemetry deal, Collector<Tuple2<Telemetry, Telemetry>> out) throws Exception {
			System.out.println("map2");
			dealState.update(deal);
			/*
			 * StockPrice stock = stockState.value(); if (stock != null) {
			 * stockState.clear();
			 * 
			 * out.collect(new Tuple2(stock, deal)); } else {
			 * dealState.update(deal); }
			 */
		}
	}

	public static class GeoFenceFunction extends RichCoFlatMapFunction<Telemetry, Fence, JsonObject> {
		// keyed, managed state
		private ValueState<Telemetry> telemetryState;
		private ValueState<Fence> fenceState;
		public static final double ERadius = (Math.PI / 180) * 6378137;
		@Override
		public void open(Configuration config) {
			telemetryState = getRuntimeContext()
					.getState(new ValueStateDescriptor<>("saved telemetry", Telemetry.class));
			fenceState = getRuntimeContext().getState(new ValueStateDescriptor<>("saved fence", Fence.class));
		}

		@Override
		public void flatMap1(Telemetry telemtry, Collector<JsonObject> out) throws Exception {
			Fence fence = fenceState.value();
			if (fence != null) {
				JsonObject json= new JsonObject();
				Geometry fenceGeometry = getGeometry(fence.getLatitude(), fence.getLogitude());
				Geometry telemetryGeometry = getGeometry(telemtry.getLocation().getLatitude(),
						telemtry.getLocation().getLogitude());
				double distan=telemetryGeometry.distance(fenceGeometry)*ERadius;
				json.addProperty("assetId", fence.getAssetId());
				json.addProperty("distance",distan);
				json.addProperty("assetName", telemtry.getHeader().getAssetName());
				double radiuos=Double.parseDouble(fence.getRadius());
				if (distan<radiuos) {
					json.addProperty("InsideFence",true);
				} else {
					json.addProperty("InsideFence",false);
				}
				json.addProperty("messageTime", telemtry.getHeader().getMessageTime());
				json.addProperty("location", telemtry.getLocation().getLatitude() +","+telemtry.getLocation().getLogitude());
				out.collect(json);
			}else{
				System.out.println("No Fence Data Found ->"+telemtry.getHeader().getAssetId());
			}
		}

		@Override
		public void flatMap2(Fence deal, Collector<JsonObject> out) throws Exception {
			fenceState.update(deal);
		}
		private static Geometry getGeometry(String latitude, String longitude) {
			Coordinate coordinates = new Coordinate(Double.parseDouble(latitude), Double.parseDouble(longitude));
			return new GeometryFactory().createPoint(coordinates);
		}
	}

	

}
