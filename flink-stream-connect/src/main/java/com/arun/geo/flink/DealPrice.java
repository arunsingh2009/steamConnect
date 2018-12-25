package com.arun.geo.flink;

import java.io.Serializable;

public class DealPrice implements Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	public String symbol;
	public Double price;
	public long creationTime;

	public DealPrice() {
	}

	public DealPrice(String symbol, Double price) {
		this.symbol = symbol;
		this.price = price;
		this.creationTime=System.currentTimeMillis();
	}

	@Override
	public String toString() {
		return "DealPrice{" + "symbol='" + symbol + '\'' + ", price=" + price + '}';
	}

	public long getCreationTime() {
		return creationTime;
	}

	public void setCreationTime(long creationTime) {
		this.creationTime = creationTime;
	}
}
