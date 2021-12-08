package com.myslyv4uk.kafka.flink.stream;

import com.myslyv4uk.kafka.consumer.KafkaUtil;
import com.myslyv4uk.kafka.flink.model.AuditTrail;
import com.myslyv4uk.kafka.flink.stream.datagenerator.KafkaDataGenerator;
import com.myslyv4uk.kafka.flink.util.FlinkUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.text.SimpleDateFormat;
import java.util.Date;

/*
A Flink Program that reads a files stream, computes a Map and Reduce operation,
and writes to a file output
 */

public class WindowingOperations {
	
	public static void main(String[] args) throws Exception {
		/****************************************************************************
		 *                  Read Kafka Topic Stream into a DataStream.
		 ****************************************************************************/
		//Setup a Kafka Consumer on Flink
		FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>("flink.kafka.streaming.source", //topic
						new SimpleStringSchema(), //Schema for data
						KafkaUtil.getKafkaConsumerProperties("flink.learn.realtime")); //connection properties
		
		//Setup to receive only new messages
		kafkaConsumer.setStartFromLatest();
		
		//Create the data stream
		DataStream<String> auditTrailStr = FlinkUtil.STREAM_ENV.addSource(kafkaConsumer);
		
		//Convert each record to an Object
		DataStream<AuditTrail> auditTrail = auditTrailStr.map((MapFunction<String, AuditTrail>) auditStr -> {
								System.out.println("--- Received Record : " + auditStr);
								return new AuditTrail(auditStr);
							});
		
		/****************************************************************************
		 *                  Use Sliding Windows.
		 ****************************************************************************/
		//Compute the count of events, minimum timestamp and maximum timestamp
		//for a sliding window interval of 10 seconds, sliding by 5 seconds
		DataStream<Tuple4<String, Integer, Long, Long>> slidingSummary = auditTrail
						.map(i -> new Tuple4<>(String.valueOf(System.currentTimeMillis()), //Current Time
                    1,      //Count each Record
                    i.getTimestamp(),   //Minimum Timestamp
                    i.getTimestamp()))  //Maximum Timestamp
						.returns(Types.TUPLE(Types.STRING, Types.INT, Types.LONG, Types.LONG))
						.windowAll(SlidingProcessingTimeWindows.of(
										Time.seconds(10), //Window Size
										Time.seconds(5)))  //Slide by 5
						.reduce((x, y) -> new Tuple4<>(
										x.f0,
										x.f1 + y.f1,
										Math.min(x.f2, y.f2),
										Math.max(x.f3, y.f3)));
		
		//Pretty Print the tuples
		slidingSummary.map((MapFunction<Tuple4<String, Integer, Long, Long>, Object>) slidingSummary1 -> {
			SimpleDateFormat format = new SimpleDateFormat("HH:mm:ss");
			String minTime = format.format(new Date(slidingSummary1.f2));
			String maxTime = format.format(new Date(slidingSummary1.f3));
			System.out.println("Sliding Summary : "
							+ (new Date())
							+ " Start Time : " + minTime
							+ " End Time : " + maxTime
							+ " Count : " + slidingSummary1.f1);
			return null;
		});
		
		/****************************************************************************
		 *                  Use Session Windows.
		 ****************************************************************************/
		//Execute the same example as before using Session windows
		//Partition by User and use a window gap of 5 seconds.
		DataStream<Tuple4<String, Integer, Long, Long>> sessionSummary = auditTrail
						.map(i -> new Tuple4<>
										(i.getUser(), //Get user
														1,      //Count each Record
														i.getTimestamp(),   //Minimum Timestamp
														i.getTimestamp()))  //Maximum Timestamp
						.returns(Types.TUPLE(Types.STRING,
										Types.INT,
										Types.LONG,
										Types.LONG))
						.keyBy((KeySelector<Tuple4<String, Integer, Long, Long>, Object>) trail -> trail.f0) //Key by user
						.window(ProcessingTimeSessionWindows.withGap(Time.seconds(5)))
						.reduce((x, y) -> new Tuple4<>(
										x.f0,
										x.f1 + y.f1,
										Math.min(x.f2, y.f2),
										Math.max(x.f3, y.f3)));
		//Pretty print
		sessionSummary.map(new MapFunction<Tuple4<String, Integer, Long, Long>, Object>() {
			@Override
			public Object map(Tuple4<String, Integer, Long, Long> sessionSummary) {
				SimpleDateFormat format = new SimpleDateFormat("HH:mm:ss");
				String minTime = format.format(new Date(sessionSummary.f2));
				String maxTime = format.format(new Date(sessionSummary.f3));
				System.out.println("Session Summary : "
								+ (new Date()).toString()
								+ " User : " + sessionSummary.f0
								+ " Start Time : " + minTime
								+ " End Time : " + maxTime
								+ " Count : " + sessionSummary.f1);
				return null;
			}
		});
		
		/****************************************************************************
		 *                  Setup data source and execute the Flink pipeline
		 ****************************************************************************/
		//Start the Kafka Stream generator on a separate thread
		FlinkUtil.printHeader("Starting Kafka Data Generator...");
		Thread kafkaThread = new Thread(new KafkaDataGenerator());
		kafkaThread.start();
		
		// execute the streaming pipeline
		FlinkUtil.STREAM_ENV.execute("Flink Windowing Example");
	}
	
}
