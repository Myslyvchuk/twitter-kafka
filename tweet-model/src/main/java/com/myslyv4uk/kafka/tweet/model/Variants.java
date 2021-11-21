package com.myslyv4uk.kafka.tweet.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class Variants {
	
	private int bitrate;
	@JsonProperty("content_type")
	private String contentType;
	private String url;
}
