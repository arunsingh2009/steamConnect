package com.arun.geo.pojo;

import java.io.Serializable;

public class Fence implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private String assetId;

	private String radius;

	private String logitude;

	private String latitude;

	private String type;
	
	private String messageTime;

	public String getAssetId() {
		return assetId;
	}

	public void setAssetId(String assetId) {
		this.assetId = assetId;
	}

	public String getRadius() {
		return radius;
	}

	public void setRadius(String radius) {
		this.radius = radius;
	}

	public String getLogitude() {
		return logitude;
	}

	public void setLogitude(String logitude) {
		this.logitude = logitude;
	}

	public String getLatitude() {
		return latitude;
	}

	public void setLatitude(String latitude) {
		this.latitude = latitude;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getMessageTime() {
		return messageTime;
	}

	public void setMessageTime(String messageTime) {
		this.messageTime = messageTime;
	}

	@Override
	public String toString() {
		return "Fence [assetId=" + assetId + ", radius=" + radius + ", logitude=" + logitude + ", latitude=" + latitude
				+ ", type=" + type + ", messageTime=" + messageTime + "]";
	}
	
	
}
