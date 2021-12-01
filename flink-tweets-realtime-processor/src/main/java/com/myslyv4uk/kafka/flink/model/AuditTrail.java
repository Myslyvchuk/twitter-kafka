package com.myslyv4uk.kafka.flink.model;

import lombok.Data;

@Data
public class AuditTrail {
	
	private int id;
	private String user;
	private String entity;
	private String operation;
	private long timestamp;
	private int duration;
	private int count;
	
	public AuditTrail(String auditStr) {
		//Split the string
		final String[] attributes = auditStr.replace("\"","").split(",");
		//Assign values
		this.id = Integer.parseInt(attributes[0]);
		this.user = attributes[1];
		this.entity = attributes[2];
		this.operation = attributes[3];
		this.timestamp = Long.parseLong(attributes[4]);
		this.duration = Integer.parseInt(attributes[5]);
		this.count = Integer.parseInt(attributes[6]);
	}
	
}
