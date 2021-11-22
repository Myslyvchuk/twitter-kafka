package com.myslyv4uk.kafka.flink;

import com.myslyv4uk.kafka.tweet.model.Tweet;
import com.myslyv4uk.kafka.tweet.serde.TweetDeserializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

@Slf4j
public class TweetConsumer {

	public static void main(String[] args) throws InterruptedException {
		KafkaConsumer<String, Tweet> consumer = createConsumer("twitter-raw","tweet-raw-group");
		while (true) {
			final ConsumerRecords<String, Tweet> records = consumer.poll(Duration.ofMillis(100));
			for (ConsumerRecord<String, Tweet> record : records) {
				Thread.sleep(3000);
				log.info("=====================================================================================================");
				log.info("Key: {} Value: {}", record.key(), record.value().getText());
				log.info("Partition: {} Offset: {}", record.partition(), record.offset());
				log.info("=====================================================================================================");
			}
		}
	
	}
	
	private static KafkaConsumer<String, Tweet> createConsumer(String topic, String groupId) {
		
		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, TweetDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		//properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); //disable autocommit
		//properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");
		
		KafkaConsumer<String, Tweet> consumer = new KafkaConsumer<>(properties);
		
		consumer.subscribe(List.of(topic));
		
		return consumer;
	}
	
}
