package com.myslyv4uk.kafka.twittert;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.myslyv4uk.kafka.tweet.mapper.JacksonMapper;
import com.myslyv4uk.kafka.tweet.model.Tweet;
import com.myslyv4uk.kafka.tweet.model.TwitterCredentials;
import com.myslyv4uk.kafka.tweet.serde.TweetSerializer;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

@Slf4j
public class TwitterProducer {
	
	public static final String BOOTSTRAP_SERVER = "localhost:9092";
	public static final String TOPIC = "twitter-raw";
	private final List<String> termsToFetchFromTwitter = List.of("bitcoin");
	private final ObjectMapper mapper;
	private static final Random RANDOM_MESSAGE_DELAY = new Random();
	
	public TwitterProducer(ObjectMapper mapper) {
		this.mapper = mapper;
	}
	
	public static void main(String[] args) {
		//twitter client
		new TwitterProducer(JacksonMapper.getInstance()).run();
	}
	
	private void run() {
		log.info("Set up!");
		BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(1000);
		Client client = createTwitterClient(msgQueue);
		client.connect();
		
		KafkaProducer<String, Tweet> producer = createProducer();
		
		while (!client.isDone()) {
			Tweet tweet = null;
			String key = null;
			try {
				// random delay polling next message in range 4 seconds
				Thread.sleep(RANDOM_MESSAGE_DELAY.nextInt(4000));
				String msg = msgQueue.poll(1, TimeUnit.SECONDS);
				log.info("{}",msg);
				tweet = mapper.readValue(msg, Tweet.class);
				key = tweet.getUser().getLocation() + "_" + String.join("_", termsToFetchFromTwitter);
				log.info("{}", tweet.getId());
			} catch (InterruptedException e) {
				e.printStackTrace();
				client.stop();
			} catch (JsonMappingException e) {
				e.printStackTrace();
			} catch (JsonProcessingException e) {
				e.printStackTrace();
			}
			if (tweet != null) {
				//log.info("{}", tweet);
				producer.send(new ProducerRecord<>(TOPIC, key, tweet), (recordMetadata, e) -> {
					if (e != null) {
						log.error("Something bad happened", e);
					}
				});
			}
		}
		log.info("End of app!");
	}
	
	private KafkaProducer<String, Tweet> createProducer() {
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, TweetSerializer.class.getName());
		
		return new KafkaProducer<>(properties);
	}
	
	private Client createTwitterClient(BlockingQueue<String> msgQueue) {
		/** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
		Hosts twitterKafkaHosts = new HttpHosts(Constants.STREAM_HOST);
		StatusesFilterEndpoint twitterKafkaEndpoint = new StatusesFilterEndpoint();
		// Optional: set up some followings and track terms
		
		twitterKafkaEndpoint.trackTerms(termsToFetchFromTwitter);
		
		Authentication twitterKafkaAuth = new OAuth1(
						TwitterCredentials.CONSUMER_KEY,
						TwitterCredentials.CONSUMER_SECRET,
						TwitterCredentials.TOKEN,
						TwitterCredentials.SECRET);
		
		ClientBuilder builder = new ClientBuilder()
						.name("Kafka-Twitter-Client-01")                      // optional: mainly for the logs
						.hosts(twitterKafkaHosts)
						.authentication(twitterKafkaAuth)
						.endpoint(twitterKafkaEndpoint)
						.processor(new StringDelimitedProcessor(msgQueue));  // optional: use this if you want to process client events
		
		return builder.build();
	}
}
