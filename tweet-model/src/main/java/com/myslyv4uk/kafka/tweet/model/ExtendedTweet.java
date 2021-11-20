package com.myslyv4uk.kafka.tweet.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.List;

@Data
public class ExtendedTweet {
	
	@JsonProperty("full_text")
	private String fullText;
	@JsonProperty("display_text_range")
	private List<Integer> displayTextRange;
	private Entity entities;
	@JsonProperty("extended_entities")
	private ExtendedEntity extendedEntities;
}
