package com.arun.geo.flink;

import java.io.Serializable;

public class StockPrice implements Serializable {
	
	public String symbol;
	public Double price;
	public long creationTime;

	public StockPrice() {
	}

	public StockPrice(String symbol, Double price) {
		this.symbol = symbol;
		this.price = price;
		this.creationTime=System.currentTimeMillis();
	}

	@Override
	public String toString() {
		return "StockPrice{" + "symbol='" + symbol + '\'' + ", price=" + price + '}';
	}

	public long getCreationTime() {
		return creationTime;
	}

	public void setCreationTime(long creationTime) {
		this.creationTime = creationTime;
	}
	
}
