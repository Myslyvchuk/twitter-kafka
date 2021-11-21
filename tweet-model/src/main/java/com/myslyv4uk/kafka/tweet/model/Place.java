package com.myslyv4uk.kafka.tweet.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class Place {
	
	private String id;
	private String url;
	@JsonProperty("place_type")
	private String placeType;
	private String name;
	@JsonProperty("full_name")
	private String fullName;
	@JsonProperty("country_code")
	private String countryCode;
	private String country;
	@JsonProperty("bounding_box")
	private BoundingBox boundingBox;
	private Attributes attributes;
}
