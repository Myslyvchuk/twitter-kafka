package com.myslyv4uk.kafka.elasticsearch;

import com.google.gson.JsonParser;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Properties;

@Slf4j
public class ElasticSearchConsumer {
	
	public static void main(String[] args) throws IOException {
		RestHighLevelClient client = createClient();
		
		KafkaConsumer<String, String> esConsumer = createConsumer();
		while (true) {
			final ConsumerRecords<String, String> records = esConsumer.poll(Duration.ofMillis(100));
			log.info("Received {} records.", records.count());
			
			for (ConsumerRecord<String, String> record : records) {
				// log.info("Key: {} Value: {}", record.key(), record.value());
				//log.info("Partition: {} Offset: {}", record.partition(), record.offset());
				//possible solution with id (Kafka generic id)
				//String id =  record.topic() + "_" + record.partition() + "_" + record.offset();
				String id = extractIdFromTweet(record.value());
				
				//insert into ES
				IndexRequest indexRequest = new IndexRequest(
								//index which was created by PUT localhost:9200/tweets
								"twitter",
								"tweets",
								id // to make our consumer idempotent
				).source(record.value(), XContentType.JSON);
				IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
				log.info(indexResponse.getId());
				// /twitter/tweets/{id}gIWx0XwBWCBIynXU2gaD
				
				try {
					Thread.sleep(10);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			log.info("Committing the offsets ...");
			esConsumer.commitSync();
			log.info("Offsets have been committed ...");
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
		//client.close();
	}
	
	public static RestHighLevelClient createClient() {
//        for BONSAI
//        String hostName = "url-without-protocol-and-port";
//        String userName = "username";
//        String password = "pass";
		String hostName = "localhost";
//        //don't use if you run local ES only for Bonsai exmaple or any other cloud
//        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
//        credentialsProvider.setCredentials(AuthScope.ANY,
//                new UsernamePasswordCredentials(userName, password));
//        for BONSAI port should be 443 and https
		RestClientBuilder builder = RestClient.builder(new HttpHost(hostName, 9200, "http")
//        for BONSAI
//                )
//                .setHttpClientConfigCallback(httpAsyncClientBuilder ->
//                        httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider)
		);
		return new RestHighLevelClient(builder);
	}
	
	private static String extractIdFromTweet(String value) {
		return new JsonParser().parse(value)
						.getAsJsonObject()
						.get("id_str")
						.getAsString();
	}
	
	public static KafkaConsumer<String, String> createConsumer() {
		final String groupId = "es-group";
		
		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); //disable autocommit
		properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10");
		
		
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
		
		consumer.subscribe(List.of("twitter"));
		
		return consumer;
	}
}
