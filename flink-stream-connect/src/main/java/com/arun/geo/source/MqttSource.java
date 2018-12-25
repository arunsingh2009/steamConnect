package com.arun.geo.source;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;

public class MqttSource {

	public static void main(String[] args) {
		try {
		MqttClient client = new MqttClient("tcp://localhost:1883", MqttClient.generateClientId());
		/*client.connect();
		MqttMessage message = new MqttMessage();
		message.setPayload("Hello world from Java".getBytes());
		client.publish("iot_data", message);
		client.disconnect();
		
	
			client = new MqttClient("tcp://localhost:1883", MqttClient.generateClientId());
	*/		client.setCallback( new SimpleMqttCallBack() );
			client.connect();
		} catch (MqttException e) {
			e.printStackTrace();
		}
		

	}

}
