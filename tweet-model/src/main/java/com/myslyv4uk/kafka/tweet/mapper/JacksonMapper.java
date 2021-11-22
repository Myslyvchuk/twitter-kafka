package com.myslyv4uk.kafka.tweet.mapper;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JacksonMapper {
	private static volatile ObjectMapper instance;
	
	public static ObjectMapper getInstance() {
		ObjectMapper localInstance = instance;
		if (localInstance == null) {
			synchronized (JacksonMapper.class) {
				localInstance = instance;
				if (localInstance == null) {
					localInstance = new ObjectMapper();
					localInstance.setSerializationInclusion(JsonInclude.Include.NON_NULL);
					instance = localInstance;
				}
			}
		}
		return localInstance;
	}
}
