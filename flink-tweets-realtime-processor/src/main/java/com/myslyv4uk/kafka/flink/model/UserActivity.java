package com.myslyv4uk.kafka.flink.model;

import lombok.Data;

@Data
public class UserActivity {
	
	private int id;
	private String user;
	private String action;
	private long timestamp;
	
	public UserActivity(String userActivity) {
		final String[] attributes = userActivity.replace("\"","").split(",");
		this.id = Integer.parseInt(attributes[0]);
		this.user = attributes[1];
		this.action = attributes[2];
		this.timestamp = Long.parseLong(attributes[3]);
	}
}
