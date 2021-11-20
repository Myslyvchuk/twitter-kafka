package com.myslyv4uk.kafka.tweet.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.List;

@Data
public class Entity {
	
	private List<MetaData> hashtags;
	private List<TweetURL> urls;
	@JsonProperty("user_mentions")
	private List<UserMentions> userMentions;
	private List<MetaData> symbols;
	private List<Media> media;
}
