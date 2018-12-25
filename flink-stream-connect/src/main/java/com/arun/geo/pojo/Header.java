package com.arun.geo.pojo;

import java.io.Serializable;

public class Header implements Serializable {

	/**
	* 
	*/
	private static final long serialVersionUID = 1L;

	private String assetId;

	private String messageTime;

	private String messageId;
	
	private String assetName;

	public String getAssetId() {
		return assetId;
	}

	public void setAssetId(String assetId) {
		this.assetId = assetId;
	}

	public String getMessageTime() {
		return messageTime;
	}

	public void setMessageTime(String messageTime) {
		this.messageTime = messageTime;
	}

	public String getMessageId() {
		return messageId;
	}

	public void setMessageId(String messageId) {
		this.messageId = messageId;
	}
	public String getAssetName() {
		return assetName;
	}

	public void setAssetName(String assetName) {
		this.assetName = assetName;
	}

	@Override
	public String toString() {
		return "Header [assetId=" + assetId + ", messageTime=" + messageTime + ", messageId=" + messageId
				+ ", assetName=" + assetName + "]";
	}
	
	
}
