package com.myslyv4uk.kafka.flink.stream;

import com.myslyv4uk.kafka.consumer.KafkaUtil;
import com.myslyv4uk.kafka.flink.model.UserActivity;
import com.myslyv4uk.kafka.flink.stream.datagenerator.UserActivityStreamDataGenerator;
import com.myslyv4uk.kafka.flink.util.FlinkUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.io.IOException;

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
		 *                  Use Tumbling Windows to compute 10 sec summary
		 ****************************************************************************/
		//Compute the count of events
		DataStream<Tuple3<String, String, Integer>> tumblingSummary = userActivity
						.map(i -> new Tuple3<>(i.getUser(), i.getAction(), 1))
						.returns(Types.TUPLE(Types.STRING, Types.STRING, Types.INT))
						.keyBy((KeySelector<Tuple3<String, String, Integer>, Object>) value -> Tuple2.of(value.f0, value.f1)
						)
						.window(TumblingProcessingTimeWindows.of(Time.seconds(10)))  //Time size 10
						.reduce((x, y) -> new Tuple3<>(x.f0, x.f1, x.f2 + y.f2));
		
		//Pretty Print User Action 10 second Summary
		tumblingSummary.map(new MapFunction<Tuple3<String, String, Integer>, Object>() {
							@Override
							public Object map(Tuple3<String, String, Integer> summary) {
								System.out.println("User Action Summary : "
												+ " User : " + summary.f0
												+ ", Action : " + summary.f1
												+ ", Total : " + summary.f2);
								return null;
							}
						});
		
		/****************************************************************************
		 *                 Duration for each action
		 ****************************************************************************/
		//Compute the count of events
		DataStream<Tuple3<String, String, Long>> actionDuration = userActivity
						.map(i -> new Tuple3<>(i.getUser(), i.getAction(), i.getTimestamp()))
						.returns(Types.TUPLE(Types.STRING, Types.STRING, Types.LONG))
						.keyBy((KeySelector<Tuple3<String, String, Long>, Object>) value -> Tuple2.of(value.f0, value.f1))
						.map(new RichMapFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>>() {
							
							//Keep track of last event name
							private transient ValueState<String> lastActionName;
							//Keep track of last event timestamp
							private transient ValueState<Long> lastActionStart;
							
							@Override
							public void open(Configuration parameters) throws Exception {
								ValueStateDescriptor<String> nameDescriptor = new ValueStateDescriptor<>(
												"last-action-name", // the state name
												TypeInformation.of(new TypeHint<>() {}));
								lastActionName = getRuntimeContext().getState(nameDescriptor);
								
								ValueStateDescriptor<Long> startDescriptor = new ValueStateDescriptor<>(
																"last-action-start", // the state time
																TypeInformation.of(new TypeHint<>() {}));
								lastActionStart = getRuntimeContext().getState(startDescriptor);
							}
							
							@Override
							public Tuple3<String, String, Long> map(Tuple3<String, String, Long> event) throws IOException {
								//Default to publish
								String publishAction = "None";
								long publishDuration = 0L;
								
								//Check if its not the first event of the session
								if (lastActionName.value() != null && !event.f1.equals("Login")) {
										//Set the last event name
										publishAction = lastActionName.value();
										//Last event duration = difference in timestamps
										publishDuration = event.f2 - lastActionStart.value();
								}
							
								if ( event.f1.equals("Logout")) { 	//If logout event, unset the state trackers
									lastActionName.clear();
									lastActionStart.clear();
								}	else { //Update the state trackers with current event
									lastActionName.update(event.f1);
									lastActionStart.update(event.f2);
								}
								return new Tuple3<>(event.f0, publishAction, publishDuration); //Publish durations
							}
						});
		
		//Pretty Print User Action 10 second Summary
		actionDuration.map(new MapFunction<Tuple3<String, String, Long>, Object>() {
			@Override
			public Object map(Tuple3<String, String, Long> summary) {
				System.out.println("##########User Action Duration : "
								+ " User : " + summary.f0
								+ ", Action : " + summary.f1
								+ ", Total Duration : " + summary.f2);
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
