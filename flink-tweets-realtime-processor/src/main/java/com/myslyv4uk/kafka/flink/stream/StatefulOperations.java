package com.myslyv4uk.kafka.flink.stream;

import com.myslyv4uk.kafka.flink.model.AuditTrail;
import com.myslyv4uk.kafka.flink.stream.datagenerator.FileStreamGenerator;
import com.myslyv4uk.kafka.flink.util.FlinkUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;

/*
A Flink Program to demonstrate working on keyed streams.
 */

public class StatefulOperations {
	
	public static void main(String[] args) throws Exception {
		
		/****************************************************************************
		 *                  Read CSV File Stream into a DataStream.
		 ****************************************************************************/
		
		//Define the text input format based on the directory
		TextInputFormat auditFormat = new TextInputFormat(new Path(FlinkUtil.RAW_DATA_DIR));
		
		//Create a Datastream based on the directory
		DataStream<String> auditTrailStr = FlinkUtil.STREAM_ENV_SEQ.readFile(auditFormat,
						FlinkUtil.RAW_DATA_DIR,    //Director to monitor
						FileProcessingMode.PROCESS_CONTINUOUSLY,
						1000); //monitor interval
		
		/****************************************************************************
		 *                 Using simple Stateful Operations
		 ****************************************************************************/
		//Convert each record to an Object
		DataStream<Tuple3<String, String, Long>> auditTrailState
						= auditTrailStr
						.map(new MapFunction<String, Tuple3<String, String, Long>>() {
							@Override
							public Tuple3<String, String, Long> map(String auditStr) {
								System.out.println("--- Received Record : " + auditStr);
								AuditTrail auditTrail = new AuditTrail(auditStr);
								return new Tuple3<>(auditTrail.getUser(), auditTrail.getOperation(), auditTrail.getTimestamp());
							}
						});
		
		//Measure the time interval between DELETE operations by the same User
		DataStream<Tuple2<String, Long>> deleteIntervals = auditTrailState
						.keyBy((KeySelector<Tuple3<String, String, Long>, Object>) value -> value.f0)
						.map(new RichMapFunction<Tuple3<String, String, Long>, Tuple2<String, Long>>() {
							private transient ValueState<Long> lastDelete;
							
							@Override
							public void open(Configuration config) {
								ValueStateDescriptor<Long> descriptor = new ValueStateDescriptor<>(
												"last-delete", // the state name
												TypeInformation.of(new TypeHint<>() {
												}));
								lastDelete = getRuntimeContext().getState(descriptor);
							}
							
							@Override
							public Tuple2<String, Long> map(Tuple3<String, String, Long> auditTrail) throws Exception {
								Tuple2<String, Long> retTuple = new Tuple2<>("No-Alerts", 0L);
								if (auditTrail.f1.equals("Delete")) { //If two deletes were done by the same user within 10 seconds
									if (lastDelete.value() != null) {
										long timeDiff = auditTrail.f2 - lastDelete.value();
										if (timeDiff < 3000L) {
											retTuple = new Tuple2<>(auditTrail.f0, timeDiff);
										}
									}
									lastDelete.update(auditTrail.f2);
								}
								return retTuple;//If no specific alert record was returned
							}
						})
						.filter((FilterFunction<Tuple2<String, Long>>) alert -> {
							if (alert.f0.equals("No-Alerts")) {
								return false;
							} else {
								System.out.println("\n!! DELETE Alert Received : User "
												+ alert.f0 + " executed 2 deletes within "
												+ alert.f1 + " ms" + "\n");
								return true;
							}
						});
		
		/****************************************************************************
		 *                  Setup data source and execute the Flink pipeline
		 ****************************************************************************/
		//Start the File Stream generator on a separate thread
		FlinkUtil.printHeader("Starting File Data Generator...");
		Thread genThread = new Thread(new FileStreamGenerator());
		genThread.start();
		
		// execute the streaming pipeline
		FlinkUtil.STREAM_ENV_SEQ.execute("Flink Streaming Stateful Operations Example");
	}
}
