package com.myslyv4uk.kafka.tweet.model;

import lombok.Data;

@Data
public class Tweet {
	
	private String createdAt;
	private long id;
	private String idStr;
	private String text;
	private String source;
	private boolean truncated;
	private Integer inReplyToStatusId;
	private String inReplyToStatusIdStr;
	private Integer inReplyToUserId;
	private String inReplyToUserStr;
	private String inReplyToScreenName;
	//private User user;
	private String geo;
	private String coordinates;
	private String place;
	private String contributors;
	private boolean isQuoteStatus;
	
	
	
	
}
