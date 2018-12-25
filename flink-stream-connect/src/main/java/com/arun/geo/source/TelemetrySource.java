package com.arun.geo.source;

import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import com.arun.geo.pojo.Header;
import com.arun.geo.pojo.Location;
import com.arun.geo.pojo.Telemetry;

public class TelemetrySource implements SourceFunction<Telemetry> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public void cancel() {

	}

	@Override
	public void run(SourceContext<Telemetry> context) throws Exception {
		while (true) {
			Telemetry telemetry = createTelemetry();
			context.collectWithTimestamp(createTelemetry(), Long.parseLong(telemetry.getHeader().getMessageTime()));
			Thread.sleep(1000);
		}

	}

	private Telemetry createTelemetry() {
		Telemetry telemetry = new Telemetry();
		telemetry.setHeader(createHeader());
		telemetry.setLocation(createLocation());
		return telemetry;

	}

	private Header createHeader() {
		String[] ids = { "23", "24", "25", "26", "27" };
		int x = ThreadLocalRandom.current().nextInt(5);
		UUID uuid = UUID.randomUUID();
		Header header = new Header();
		header.setAssetName("Asset" + ids[x]);
		header.setAssetId("GEO" + ids[x]);
		header.setMessageId(uuid.toString());
		header.setMessageTime(String.valueOf(System.currentTimeMillis()));
		return header;

	}

	private Location createLocation() {
		Location loc = new Location();
		Double latitude = ThreadLocalRandom.current().nextDouble(35.04428, 35.046036);
		Double longitude = ThreadLocalRandom.current().nextDouble(-80.664582, -80.662586);
		loc.setLatitude(latitude.toString());
		loc.setLogitude(longitude.toString());
		return loc;

	}

}
