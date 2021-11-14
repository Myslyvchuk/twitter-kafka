package com.myslyv4uk.kafka.streams;

import com.google.gson.JsonParser;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class StreamFilterTweets {
	
	public static void main(String[] args) {
		Properties properties = new Properties();
		properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams");
		properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
		properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
		
		//create topology
		StreamsBuilder streamsBuilder = new StreamsBuilder();
		
		//input topic
		final KStream<String, String> input_topic = streamsBuilder.stream("twitter");
		final KStream<String, String> filteredStream = input_topic.filter(
						(k,jsonTweets) ->
							extractIdFromTweet(jsonTweets) > 10000
		);
		filteredStream.to("important_tweets");
		
		//build topology
		KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), properties);
		
		kafkaStreams.start();
		
	}
	
	private static int extractIdFromTweet(String value) {
		try {
			return new JsonParser().parse(value)
							.getAsJsonObject()
							.get("user")
							.getAsJsonObject()
							.get("followers_count")
							.getAsInt();
		} catch (NullPointerException e) {
			return 0;
		}
	}
}
