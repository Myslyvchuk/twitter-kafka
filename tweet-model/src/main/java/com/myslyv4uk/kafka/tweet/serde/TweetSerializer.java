package com.myslyv4uk.kafka.tweet.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.myslyv4uk.kafka.tweet.model.Tweet;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

@Slf4j
public class TweetSerializer implements Serializer<Tweet> {
	
	private final ObjectMapper objectMapper = new ObjectMapper();
	@Override
	public byte[] serialize(String topic, Tweet data) {
		try {
			if (data == null){
				log.info("Null received at serializing");
				return null;
			}
			log.info("Serializing...");
			return objectMapper.writeValueAsBytes(data);
		} catch (Exception e) {
			throw new SerializationException("Error when serializing MessageDto to byte[]");
		}
	}
}
