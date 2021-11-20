package com.myslyv4uk.kafka.tweet.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.List;

@Data
public class UserMentions {
	
	@JsonProperty("screen_name")
	private String screenName;
	private String name;
	private Long id;
	@JsonProperty("id_str")
	private String idStr;
	private List<Integer> indices;
}
