package com.myslyv4uk.kafka.tweet.serde;

import com.myslyv4uk.kafka.tweet.mapper.JacksonMapper;
import com.myslyv4uk.kafka.tweet.model.Tweet;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

public class FlinkTweetDeserializationSchema implements DeserializationSchema<Tweet> {
	@Override
	public Tweet deserialize(byte[] bytes) throws IOException {
		return JacksonMapper.getInstance().readValue(bytes, Tweet.class);
	}
	
	@Override
	public boolean isEndOfStream(Tweet tweet) {
		return false;
	}
	
	@Override
	public TypeInformation<Tweet> getProducedType() {
		return TypeInformation.of(Tweet.class);
	}
}
