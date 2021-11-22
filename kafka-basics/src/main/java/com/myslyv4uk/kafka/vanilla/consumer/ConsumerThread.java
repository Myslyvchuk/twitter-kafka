package com.myslyv4uk.kafka.vanilla.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

@Slf4j
public class ConsumerThread {
	
	public static void main(String[] args) {
		new ConsumerThread().run();
	}
	
	public void run() {
		CountDownLatch latch = new CountDownLatch(1);
		ConsumerConcurrency myConsumer = new ConsumerConcurrency(latch, "vanilla");
		myConsumer.start();
		
		shutDownHook(latch, myConsumer);
	}
	
	private void shutDownHook(CountDownLatch latch, ConsumerConcurrency myConsumer) {
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			log.info("Shutdown hook");
			myConsumer.shutdown();
			try {
				latch.await();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			log.info("Application has exited");
		}));
		
		try {
			latch.await();
		} catch (InterruptedException e) {
			log.error("Is interrupted {}", e.getMessage());
		} finally {
			log.info("Is closing");
		}
	}
	
	static class ConsumerConcurrency extends Thread {
		
		private final CountDownLatch latch;
		private final KafkaConsumer<String, String> consumer;
		
		public ConsumerConcurrency(CountDownLatch latch, String topic) {
			this.latch = latch;
			consumer = creteConsumer(topic);
		}
		
		@Override
		public void run() {
			try {
				while (true) {
					final ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
					for (ConsumerRecord<String, String> record : records) {
						log.info("Key: {} Value: {}", record.key(), record.value());
						log.info("Partition: {} Offset: {}", record.partition(), record.offset());
					}
				}
			} catch (WakeupException e) {
				log.info("Received shutdown signal");
			} finally {
				consumer.close();
				latch.countDown();
			}
		}
		
		public void shutdown() {
			consumer.wakeup();
		}
	}
	
	private static KafkaConsumer<String, String> creteConsumer(String topic) {
		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "vanilla-idea-group-thread");
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
		consumer.subscribe(List.of(topic));
		return consumer;
	}
}
