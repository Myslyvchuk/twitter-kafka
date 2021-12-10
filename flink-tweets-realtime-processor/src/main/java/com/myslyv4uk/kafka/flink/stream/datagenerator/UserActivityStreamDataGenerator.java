package com.myslyv4uk.kafka.flink.stream.datagenerator;

import lombok.SneakyThrows;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.List;
import java.util.Properties;
import java.util.Random;

/****************************************************************************
 * This Generator generates a series of data files in the raw_data folder
 * It is an audit trail data source.
 * This can be used for streaming consumption of data by Flink
 ****************************************************************************/

public class UserActivityStreamDataGenerator implements Runnable {
	private static final Random RANDOM = new Random();
	private static final String ANSI_PURPLE = "\u001B[35m";
	private static final String ANSI_RESET = "\u001B[0m";
	
	public static void main(String[] args) {
		UserActivityStreamDataGenerator fsdg = new UserActivityStreamDataGenerator();
		fsdg.run();
	}
	
	@SneakyThrows
	public void run() {
		//Setup Kafka Client
		try (Producer<String, String> dataProducer = new KafkaProducer<>(getKafkaProducerProperties())) {
			//Define list of users
			final List<String> appUser = List.of("Bob", "Alice", "John", "Bohdan", "Alex", "Iren", "Ivan", "Jack", "Harry");
			//Define list of application operations
			final List<String> userAction = List.of("login", "logout", "viewVideo", "viewLink", "viewReview");
			
			//Generate 100 sample user activity
			for (int i = 0; i < 100; i++) {
				//Capture current timestamp
				String currentTime = String.valueOf(System.currentTimeMillis());
				//Generate a random user
				String user = appUser.get(RANDOM.nextInt(appUser.size()));
				//Generate a random operation
				String action = userAction.get(RANDOM.nextInt(userAction.size()));
				//Create a CSV Text array
				String[] csvText = {String.valueOf(i), user, action, currentTime};
				
				ProducerRecord<String, String> data = new ProducerRecord<>(
								"flink-kafka-user-activity", currentTime, String.join(",", csvText));
				dataProducer.send(data).get();
				
				System.out.println(ANSI_PURPLE +
								"Kafka Stream Generator : Sending Event : " + String.join(",", csvText)
								+ ANSI_RESET);
				
				//Sleep for a random time ( 1 - 3 secs) before the next record.
				Thread.sleep(RANDOM.nextInt(2000));
			}
		}
	}
	
	private Properties getKafkaProducerProperties() {
		Properties kafkaProps = new Properties();
		kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		return kafkaProps;
	}
	
}
