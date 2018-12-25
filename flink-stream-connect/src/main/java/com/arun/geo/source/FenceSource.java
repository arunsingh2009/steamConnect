package com.arun.geo.source;

import java.util.concurrent.ThreadLocalRandom;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import com.arun.geo.pojo.Fence;

public class FenceSource implements SourceFunction<Fence> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public void cancel() {

	}

	@Override
	public void run(SourceContext<Fence> context) throws Exception {
		while (true) {
			Fence fence = createFence();
			context.collectWithTimestamp(fence, Long.parseLong(fence.getMessageTime()));
			Thread.sleep(3000);
		}

	}

	private Fence createFence(){
		String[] ids = { "23", "24", "25", "26", "27" };
		int x = ThreadLocalRandom.current().nextInt(5);
		Fence fence = new Fence();
		Double latitude = ThreadLocalRandom.current().nextDouble(35.04428, 35.046036);
		Double longitude = ThreadLocalRandom.current().nextDouble(-80.664582, -80.662586);
		fence.setLatitude(latitude.toString());
		fence.setLogitude(longitude.toString());
		fence.setRadius("100");
		fence.setType("Circle");
		fence.setAssetId("GEO" + ids[x]);
		fence.setMessageTime(String.valueOf(System.currentTimeMillis()));
		return fence;
	}

}
