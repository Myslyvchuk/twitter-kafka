package com.myslyv4uk.kafka.tweet.model;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Size {
	
	private Map<String, Measures> sizes;
	
	@JsonAnyGetter
	public Map<String, Measures> getSizes() {
		return sizes;
	}
}
