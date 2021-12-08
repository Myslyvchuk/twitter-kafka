package com.myslyv4uk.kafka.flink.stream;

import com.myslyv4uk.kafka.consumer.KafkaUtil;
import com.myslyv4uk.kafka.flink.model.AuditTrail;
import com.myslyv4uk.kafka.flink.stream.datagenerator.FileStreamGenerator;
import com.myslyv4uk.kafka.flink.stream.datagenerator.KafkaDataGenerator;
import com.myslyv4uk.kafka.flink.util.FlinkUtil;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

/*
A Flink Program that reads a files stream, computes a Map and Reduce operation,
and writes to a file output
 */

public class KafkaFileWindowJoins {
	
	public static void main(String[] args) throws Exception {
		
		/****************************************************************************
		 *                  Read CSV File Stream into a DataStream.
		 ****************************************************************************/
		//Define the text input format based on the directory
		TextInputFormat auditFormat = new TextInputFormat(new Path(FlinkUtil.RAW_DATA_DIR));
		
		//Create a Datastream based on the directory
		DataStream<String> fileTrailStr = FlinkUtil.STREAM_ENV.readFile(auditFormat,
						FlinkUtil.RAW_DATA_DIR,    //Director to monitor
						FileProcessingMode.PROCESS_CONTINUOUSLY,
						1000); //monitor interval
		
		//Convert each record to an Object
		DataStream<AuditTrail> fileTrailObj = fileTrailStr
						.map((MapFunction<String, AuditTrail>) auditStr -> {
							System.out.println("--- Received File Record : " + auditStr);
							return new AuditTrail(auditStr);
						});
		
		/****************************************************************************
		 *                  Read Kafka Topic Stream into a DataStream.
		 ****************************************************************************/
		//Setup a Kafka Consumer on Flink
		FlinkKafkaConsumer<String> kafkaConsumer =
						new FlinkKafkaConsumer<>
										("flink.kafka.streaming.source", //topic
														new SimpleStringSchema(), //Schema for data
														KafkaUtil.getKafkaConsumerProperties("flink.learn.realtime")); //connection properties
		
		//Setup to receive only new messages
		kafkaConsumer.setStartFromLatest();
		
		//Create the data stream
		DataStream<String> kafkaTrailStr = FlinkUtil.STREAM_ENV.addSource(kafkaConsumer);
		
		//Convert each record to an Object
		DataStream<AuditTrail> kafkaTrail = kafkaTrailStr
						.map((MapFunction<String, AuditTrail>) auditStr -> {
							System.out.println("--- Received Kafka Record : " + auditStr);
							return new AuditTrail(auditStr);
						});
		
		/****************************************************************************
		 *                  Join both streams based on the same window
		 ****************************************************************************/
		
		DataStream<Tuple2<String, Integer>> joinCounts = fileTrailObj.join(kafkaTrail) //Join the two streams
						.where(new KeySelector<AuditTrail, String>() { //WHERE used to select JOIN column from first Stream
							@Override
							public String getKey(AuditTrail auditTrail) {
								return auditTrail.getUser();
							}
						})
						.equalTo(new KeySelector<AuditTrail, String>() {//EQUALTO used to select JOIN column from second stream
							@Override
							public String getKey(AuditTrail auditTrail) {
								return auditTrail.getUser();
							}
						})
						.window(TumblingProcessingTimeWindows.of(Time.seconds(5))) //Create a Tumbling window of 5 seconds
						//Apply JOIN function. Will be called for each matched
						//combination of records.
						.apply(new JoinFunction<AuditTrail, AuditTrail, Tuple2<String, Integer>>() {
							@Override
							public Tuple2<String, Integer> join(AuditTrail fileTrail, AuditTrail kafkaTrail) {
								return new Tuple2<>(fileTrail.getUser(), 1);
							}
						});
		//Print the counts
		joinCounts.print();
		
		/****************************************************************************
		 *                  Setup data source and execute the Flink pipeline
		 ****************************************************************************/
		//Start the File Stream generator on a separate thread
		FlinkUtil.printHeader("Starting File Data Generator...");
		Thread genThread = new Thread(new FileStreamGenerator());
		genThread.start();
		
		//Start the Kafka Stream generator on a separate thread
		FlinkUtil.printHeader("Starting Kafka Data Generator...");
		Thread kafkaThread = new Thread(new KafkaDataGenerator());
		kafkaThread.start();
		
		// execute the streaming pipeline
		FlinkUtil.STREAM_ENV.execute("Flink Streaming Window Joins Example");
		
	}
	
}
