package com.myslyv4uk.kafka.tweet.model;

import lombok.Data;

import java.util.List;

@Data
public class MetaData {
	
		private String text;
		private List<Integer> indices;
}
