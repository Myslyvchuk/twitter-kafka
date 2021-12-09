package com.myslyv4uk.kafka.flink.stream;

import com.myslyv4uk.kafka.consumer.KafkaUtil;
import com.myslyv4uk.kafka.flink.model.UserActivity;
import com.myslyv4uk.kafka.flink.stream.datagenerator.UserActivityStreamDataGenerator;
import com.myslyv4uk.kafka.flink.util.FlinkUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

public class UserActivityStreamProcessing {
	
	public static void main(String[] args) throws Exception {
		/****************************************************************************
		 *                  Read Kafka Topic Stream into a DataStream.
		 ****************************************************************************/
		//Setup a Kafka Consumer on Flink
		FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>
						("flink-kafka-user-activity", //topic
										new SimpleStringSchema(), //Schema for data
										KafkaUtil.getKafkaConsumerProperties("flink-user-activity-group")); //connection properties
		
		//Setup to receive only new messages
		kafkaConsumer.setStartFromLatest();
		
		//Create the data stream
		DataStream<String> userActivityStr = FlinkUtil.STREAM_ENV.addSource(kafkaConsumer);
		//tweetDataStream.print("--- Received Record :");
		
		//Convert each record to an Object
		DataStream<UserActivity> userActivity = userActivityStr
						.map(new MapFunction<String, UserActivity>() {
							@Override
							public UserActivity map(String userActivity) {
								System.out.println("--- Received Record : " + userActivity);
								return new UserActivity(userActivity);
							}
						});
		
		/****************************************************************************
		 *                  Use Tumbling Windows.
		 ****************************************************************************/
		//Compute the count of events, minimum timestamp and maximum timestamp
		//for a sliding window interval of 10 seconds, sliding by 5 seconds
		DataStream<Tuple3<String, String, Integer>> tumblingSummary = userActivity
						.map(i -> new Tuple3<>(i.getUser(), i.getAction(), 1))
						.returns(Types.TUPLE(Types.STRING, Types.STRING, Types.INT))
						.keyBy(new KeySelector<Tuple3<String, String, Integer>, Object>() {
							
							@Override
							public Object getKey(Tuple3<String, String, Integer> value) {
								return Tuple2.of(value.f0, value.f1);
							}
						})
						.windowAll(TumblingEventTimeWindows.of(Time.seconds(10)))  //Time size 10
						.reduce((x, y) -> new Tuple3<>
										(x.f0, x.f1, x.f2 + y.f2));
		
		tumblingSummary.print("User Action Summary :");
		
		//Pretty Print User Action 10 second Summary
		tumblingSummary
						.map(new MapFunction<Tuple3<String, String, Integer>, Object>() {
							@Override
							public Object map(Tuple3<String, String, Integer> summary) throws Exception {
								System.out.println("User Action Summary : "
												+ " User : " + summary.f0
												+ ", Action : " + summary.f1
												+ ", Total : " + summary.f2);
								return null;
							}
						});
		
		/****************************************************************************
		 *                  Setup data source and execute the Flink pipeline
		 ****************************************************************************/
		//Start the Kafka Stream generator on a separate thread
		FlinkUtil.printHeader("Starting Kafka Data Generator...");
		Thread kafkaThread = new Thread(new UserActivityStreamDataGenerator());
		kafkaThread.start();
		
		// execute the streaming pipeline
		FlinkUtil.STREAM_ENV.execute("Flink Windowing Example");
	}
}
