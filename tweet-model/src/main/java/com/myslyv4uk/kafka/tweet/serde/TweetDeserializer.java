package com.myslyv4uk.kafka.tweet.serde;

import com.myslyv4uk.kafka.tweet.mapper.JacksonMapper;
import com.myslyv4uk.kafka.tweet.model.Tweet;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

@Slf4j
public class TweetDeserializer implements Deserializer<Tweet> {
		
		@Override
		public void configure(Map<String, ?> configs, boolean isKey) {
		}
		
		@Override
		public Tweet deserialize(String topic, byte[] data) {
			try {
				if (data == null){
					log.info("Null received at deserializing");
					return null;
				}
				log.info("Deserializing...");
				return JacksonMapper.getInstance().readValue(new String(data, "UTF-8"), Tweet.class);
			} catch (Exception e) {
				throw new SerializationException("Error when deserializing byte[] to MessageDto");
			}
		}
}
