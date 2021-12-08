package com.myslyv4uk.kafka.flink.stream;

import com.myslyv4uk.kafka.flink.model.AuditTrail;
import com.myslyv4uk.kafka.flink.stream.datagenerator.FileStreamGenerator;
import com.myslyv4uk.kafka.flink.util.FlinkUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;

/*
A Flink Program to demonstrate working on keyed streams.
 */

public class KeyedStreamOperations {
	
	public static void main(String[] args) throws Exception {
		
		/****************************************************************************
		 *                 Key By User, find Running count by User
		 ****************************************************************************/
		// Set up the streaming execution environment
		System.out.println("\nTotal Parallel Task Slots : " + FlinkUtil.STREAM_ENV.getParallelism());
		
		//read data into csv seq with set parallelism 1
		final DataStream<String> auditTrailStr = FlinkUtil.readCSVIntoDataStream(FlinkUtil.STREAM_ENV);
		
		//Convert each record to a Tuple with user and a sum of duration
		DataStream<Tuple2<String, Integer>> userCounts = auditTrailStr
						.map((MapFunction<String, Tuple2<String, Integer>>) auditStr -> {
							System.out.println("--- Received Record : " + auditStr);
							AuditTrail auditTrail = new AuditTrail(auditStr);
							return new Tuple2<String, Integer>(auditTrail.getUser(), auditTrail.getDuration());
						})
						.keyBy(new KeySelector<Tuple2<String, Integer>, Object>() {
							@Override
							public Object getKey(Tuple2<String, Integer> userCount) {
								return userCount.f0;
							}
						})  //By user name
						.reduce((x, y) -> new Tuple2<>(x.f0, x.f1 + y.f1));
		//Print User and Durations.
		userCounts.print();
		
		/****************************************************************************
		 *                  Setup data source and execute the Flink pipeline
		 ****************************************************************************/
		//Start the File Stream generator on a separate thread
		FlinkUtil.printHeader("Starting File Data Generator...");
		Thread genThread = new Thread(new FileStreamGenerator());
		genThread.start();
		
		// execute the streaming pipeline
		FlinkUtil.STREAM_ENV.execute("Flink Streaming Keyed Stream Example");
	}
	
}
