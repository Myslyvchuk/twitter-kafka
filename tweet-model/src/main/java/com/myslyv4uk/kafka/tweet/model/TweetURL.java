package com.myslyv4uk.kafka.tweet.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.List;

@Data
public class TweetURL {
	
	private String url;
	@JsonProperty("expanded_url")
	private String expandedUrl;
	@JsonProperty("display_url")
	private String displayUrl;
	private List<Integer> indices;
}
