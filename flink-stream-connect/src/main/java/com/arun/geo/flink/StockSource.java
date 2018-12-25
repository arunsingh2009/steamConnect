package com.arun.geo.flink;

import java.util.Random;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class StockSource implements SourceFunction<StockPrice> {

    /**
	 * 
	 */
	private static final long serialVersionUID = 2152574127867801924L;
	private Double price;
    private String symbol;
    private Integer sigma;
    private Double DEFAULT_PRICE=0.0;

    public StockSource(String symbol, Integer sigma) {
        this.symbol = symbol;
        this.sigma = sigma;
    }

	@Override
	public void cancel() {
	}

	@Override
	public void run(org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext<StockPrice> sourceContext)
			throws Exception {
		 	price = DEFAULT_PRICE;
	        Random random = new Random();
	        while (true) {
	            price = price + random.nextGaussian() * sigma;
	            StockPrice stock=new StockPrice(symbol, price);
	           //System.out.println("###############Stock######################-"+stock.toString());
	            sourceContext.collect(stock);
	            Thread.sleep(random.nextInt(200));
	        }
		
	}
}
