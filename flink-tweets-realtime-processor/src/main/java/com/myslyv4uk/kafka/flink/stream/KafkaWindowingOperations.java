package com.myslyv4uk.kafka.flink.stream;

import com.myslyv4uk.kafka.consumer.KafkaUtil;
import com.myslyv4uk.kafka.flink.util.FlinkUtil;
import com.myslyv4uk.kafka.tweet.mapper.JacksonMapper;
import com.myslyv4uk.kafka.tweet.model.Tweet;
import com.myslyv4uk.kafka.tweet.serde.FlinkTweetDeserializationSchema;
import com.myslyv4uk.kafka.twittert.TwitterProducer;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;

/*
A Flink Program that reads a files stream, computes a Map and Reduce operation,
and writes to a file output
 */

public class KafkaWindowingOperations {
	
	
	public static void main(String[] args) throws Exception {
		
		/****************************************************************************
		 *                  Read Kafka Topic Stream into a DataStream.
		 ****************************************************************************/
		//Setup a Kafka Consumer on Flink
		FlinkKafkaConsumer<Tweet> kafkaConsumer = new FlinkKafkaConsumer<>
						("twitter-raw", //topic
										new FlinkTweetDeserializationSchema(), //Schema for data
										KafkaUtil.getKafkaConsumerProperties("flink-tweet-raw-group")); //connection properties
		
		//Setup to receive only new messages
		kafkaConsumer.setStartFromLatest();
		
		//Create the data stream
		DataStream<Tweet> tweetDataStream = FlinkUtil.STREAM_ENV.addSource(kafkaConsumer);
		//tweetDataStream.print("--- Received Record :");
		
		/****************************************************************************
		 *                  Use Sliding Windows.
		 ****************************************************************************/
		//Compute the count of events, minimum timestamp and maximum timestamp
		//for a sliding window interval of 10 seconds, sliding by 5 seconds
		DataStream<Tuple4<String, Integer, Long, Long>> slidingSummary = tweetDataStream
						.map(i -> new Tuple4<String, Integer, Long, Long>
										(String.valueOf(System.currentTimeMillis()), //Current Time
														1,      //Count each Record
														Long.parseLong(i.getTimestampMs()),   //Minimum Timestamp
														Long.parseLong(i.getTimestampMs())))  //Maximum Timestamp
						.returns(Types.TUPLE(Types.STRING, Types.INT, Types.LONG, Types.LONG))
						.windowAll(SlidingProcessingTimeWindows.of(
										Time.seconds(10), //Window Size
										Time.seconds(5)))  //Slide by 5
						.reduce((x, y) -> new Tuple4<String, Integer, Long, Long>(
										x.f0,
										x.f1 + y.f1,
										Math.min(x.f2, y.f2),
										Math.max(x.f3, y.f3)));

		//Pretty Print the tuples
		slidingSummary.map(new MapFunction<Tuple4<String, Integer, Long, Long>, Object>() {
			@Override
			public Object map(Tuple4<String, Integer, Long, Long> slidingSummary) {
				String minTime = getLocalDateDateFormatted(slidingSummary.f2);
				String maxTime = getLocalDateDateFormatted(slidingSummary.f3);
				System.out.println("Sliding Summary : "
								+ (new Date()) + " Start Time : " + minTime
								+ " End Time : " + maxTime
								+ " Count : " + slidingSummary.f1);
				return null;
			}
		});
		
		/****************************************************************************
		 *                  Use Session Windows.
		 ****************************************************************************/
		//Execute the same example as before using Session windows
		//Partition by User and use a window gap of 5 seconds.
		DataStream<Tuple4<String, Integer, Long, Long>> sessionSummary = tweetDataStream
						.map(i -> new Tuple4<String, Integer, Long, Long>
										(i.getUser().getName(), //Get user
														1,      //Count each Record
														Long.parseLong(i.getTimestampMs()),   //Minimum Timestamp
														Long.parseLong(i.getTimestampMs())))  //Maximum Timestamp
						.returns(Types.TUPLE(Types.STRING, Types.INT, Types.LONG, Types.LONG))
						.keyBy((KeySelector<Tuple4<String, Integer, Long, Long>, Object>) tuple -> tuple.f0) //Key by user
						.window(ProcessingTimeSessionWindows.withGap(Time.seconds(5)))
						.reduce((x, y) -> new Tuple4<String, Integer, Long, Long>(
										x.f0,
										x.f1 + y.f1,
										Math.min(x.f2, y.f2),
										Math.max(x.f3, y.f3)));

		//Pretty print
		sessionSummary.map(new MapFunction<Tuple4<String, Integer, Long, Long>, Object>() {
			@Override
			public Object map(Tuple4<String, Integer, Long, Long> sessionSummary) {
				String minTime = getLocalDateDateFormatted(sessionSummary.f2);
				String maxTime = getLocalDateDateFormatted(sessionSummary.f3);
				System.out.println("Session Summary : "
								+ (new Date()) + " User : " + sessionSummary.f0
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
		FlinkUtil.printHeader("Starting Tweets reader...");
		new TwitterProducer(JacksonMapper.getInstance()).start();
		
		// execute the streaming pipeline
		FlinkUtil.STREAM_ENV.execute("Flink Windowing Example");
		
	}
	
	private static String getLocalDateDateFormatted(Long timestamp) {
		return LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneId.systemDefault())
						.format(DateTimeFormatter.ofPattern("HH:mm:ss"));
	}
}
