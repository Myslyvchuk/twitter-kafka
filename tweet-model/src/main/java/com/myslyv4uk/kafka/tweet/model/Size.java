package com.myslyv4uk.kafka.tweet.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Size {
	
	@JsonProperty("sizes")
	private Map<String, Measures> sizes;
	
//	@JsonAnyGetter
//	public Map<String, Measures> getData() {
//		return sizes1;
//	}
}
