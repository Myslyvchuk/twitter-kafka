package com.myslyv4uk.kafka.consumer;

import com.myslyv4uk.kafka.tweet.model.Tweet;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

@Slf4j
public class TweetConsumer {
	
	//private static final Random RANDOM_MESSAGE_DELAY = new Random();

	public static void main(String[] args) throws InterruptedException {
		KafkaConsumer<String, Tweet> consumer = createConsumer("twitter-raw","tweet-raw-group");
		while (true) {
			final ConsumerRecords<String, Tweet> records = consumer.poll(Duration.ofMillis(100));
			for (ConsumerRecord<String, Tweet> record : records) {
				//Thread.sleep(RANDOM_MESSAGE_DELAY.nextInt(4000));
				log.info("=====================================================================================================");
				log.info("Key: {} Value: {}", record.key(), record.value().getText());
				log.info("Partition: {} Offset: {}", record.partition(), record.offset());
				log.info("=====================================================================================================");
			}
		}
	}
	
	private static KafkaConsumer<String, Tweet> createConsumer(String topic, String groupId) {
		
		Properties properties = KafkaUtil.getKafkaConsumerProperties(groupId);
		KafkaConsumer<String, Tweet> consumer = new KafkaConsumer<>(properties);
		consumer.subscribe(List.of(topic));
		return consumer;
	}
	
}
