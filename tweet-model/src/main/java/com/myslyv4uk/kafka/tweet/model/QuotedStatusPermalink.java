package com.myslyv4uk.kafka.tweet.model;

import lombok.Data;

@Data
public class QuotedStatusPermalink {
	
	private String url;
	private String expanded;
	private String display;
}
