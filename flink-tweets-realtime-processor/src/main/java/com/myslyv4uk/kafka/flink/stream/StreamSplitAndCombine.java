package com.myslyv4uk.kafka.flink.stream;

import com.myslyv4uk.kafka.flink.model.AuditTrail;
import com.myslyv4uk.kafka.flink.stream.datagenerator.FileStreamGenerator;
import com.myslyv4uk.kafka.flink.util.FlinkUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class StreamSplitAndCombine {
	
	public static void main(String[] args) throws Exception {
		
		/****************************************************************************
		 *         Split the Stream into two Streams based on Entity
		 ****************************************************************************/
		DataStream<String> auditTrailStr = FlinkUtil.readCSVIntoDataStream(FlinkUtil.STREAM_ENV);
		//Create a Separate Trail for Sales Rep operations
		final OutputTag<Tuple2<String, Integer>> salesRepTag = new OutputTag<>("sales-rep") {};
		
		//Convert each record to an Object
		SingleOutputStreamOperator<AuditTrail> customerTrail = auditTrailStr.process(
						new ProcessFunction<String, AuditTrail>() {
							@Override
							public void processElement(
											String auditStr,
											Context ctx,
											Collector<AuditTrail> collAudit) {
								System.out.println("--- Received Record : " + auditStr);
								//Convert String to AuditTrail Object
								AuditTrail auditTrail = new AuditTrail(auditStr);
								//Create output tuple with User and count
								Tuple2<String, Integer> entityCount = new Tuple2<>(auditTrail.getUser(), 1);
								
								if (auditTrail.getEntity().equals("Customer")) {
									//Collect main output for Customer as AuditTrail
									collAudit.collect(auditTrail);
								} else {
									//Collect side output for Sales Rep
									ctx.output(salesRepTag, entityCount);
								}
							}
						});
		
		//Convert side output into a data stream
		DataStream<Tuple2<String, Integer>> salesRepTrail = customerTrail.getSideOutput(salesRepTag);
		
		//Print Customer Record summaries
		MapCountPrinter.printCount(customerTrail.map(i -> i), "Customer Records in Trail : Last 5 secs");
		
		//Print Sales Rep Record summaries
		MapCountPrinter.printCount(salesRepTrail.map(i -> i), "Sales Rep Records in Trail : Last 5 secs");
		
		/****************************************************************************
		 *         Combine two streams into one
		 ****************************************************************************/

		ConnectedStreams<AuditTrail, Tuple2<String, Integer>> mergedTrail = customerTrail.connect(salesRepTrail);

		DataStream<Tuple3<String, String, Integer>> processedTrail = mergedTrail.map(new CoMapFunction<
						            AuditTrail, //Stream 1
						            Tuple2<String, Integer>, //Stream 2
						            Tuple3<String, String, Integer> //Output
						            >() {
			//Process Stream 1
			@Override
			public Tuple3<String, String, Integer> map1(AuditTrail auditTrail) {
				return new Tuple3<>("Stream-1", auditTrail.getUser(), 1);
			}
			//Process Stream 2
			@Override
			public Tuple3<String, String, Integer> map2(Tuple2<String, Integer> srTrail)  {
				return new Tuple3<>("Stream-2", srTrail.f0, 1);
			}
		});

		//Print the combined data stream
		processedTrail.map(new MapFunction<Tuple3<String, String, Integer>, Tuple3<String, String, Integer>>() {
			@Override
			public Tuple3<String, String, Integer> map(Tuple3<String, String, Integer> user) {
				System.out.println("--- Merged Record for User: " + user);
				return null;
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
		FlinkUtil.STREAM_ENV.execute("Flink Streaming Keyed Stream Example");
		
	}
	
}
