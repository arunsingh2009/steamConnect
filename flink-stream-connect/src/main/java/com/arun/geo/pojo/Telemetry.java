package com.arun.geo.pojo;

import java.io.Serializable;

public class Telemetry implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private Location location;

	private Header header;

	public Location getLocation() {
		return location;
	}

	public void setLocation(Location location) {
		this.location = location;
	}

	public Header getHeader() {
		return header;
	}

	public void setHeader(Header header) {
		this.header = header;
	}

	@Override
	public String toString() {
		return "ClassPojo [header = " + header + ",location = " + location + "]";
	}
}
