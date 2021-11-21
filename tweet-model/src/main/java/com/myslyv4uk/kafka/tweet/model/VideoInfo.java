package com.myslyv4uk.kafka.tweet.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.List;

@Data
public class VideoInfo {
	
	@JsonProperty("aspect_ratio")
	private List<Integer> aspectRatio;
	@JsonProperty("duration_millis")
	private long durationMillis;
	private List<Variants> variants;
}
