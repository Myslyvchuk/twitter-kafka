package com.myslyv4uk.kafka.vanilla.producer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

@Slf4j
public class ProducerWithCallback {

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {
            for (int i = 0; i < 10; i++) {
                producer.send(new ProducerRecord<>("vanilla", "Hello Franz Kafka" + i), (recordMetadata, e) -> {
                    if (e == null) {
                        log.info("Received new metadata  recordMetadata \n" +
                                "Topic:" + recordMetadata.topic() + "\n " +
                                "Partition:" + recordMetadata.partition() + "\n " +
                                "Offset:" + recordMetadata.offset() + "\n " +
                                "Timestamp:" + recordMetadata.timestamp());
                    } else {
                        log.error("Error during producing {}", e.getMessage());
                    }
                });
            }
        }
    }

}
