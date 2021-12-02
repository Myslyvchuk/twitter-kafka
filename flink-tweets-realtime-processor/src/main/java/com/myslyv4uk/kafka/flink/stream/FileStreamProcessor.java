package com.myslyv4uk.kafka.flink.stream;

import com.myslyv4uk.kafka.flink.model.AuditTrail;
import com.myslyv4uk.kafka.flink.stream.datagenerator.FileStreamGenerator;
import com.myslyv4uk.kafka.flink.util.Util;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class FileStreamProcessor {
	
	public static void main(String[] args) throws Exception {
		/****************************************************************************
		 *                  Read CSV File Stream into a DataStream.
		 ****************************************************************************/
		//Define the data directory to monitor for new files
		
		//Define the text input format based on the directory
		final TextInputFormat auditFormat = new TextInputFormat(new Path(Util.RAW_DATA_DIR));
		
		//Create a DataStream based on the directory
		final DataStream<String> auditTrailStr = Util.STR_ENV.readFile(auditFormat, Util.RAW_DATA_DIR,//Director to monitor
						FileProcessingMode.PROCESS_CONTINUOUSLY, 1000); //monitor interval
		
		//Convert each record to an Object
		final DataStream<AuditTrail> auditTrail = auditTrailStr
						.map(new MapFunction<String,AuditTrail>() {
							@Override
							public AuditTrail map(String auditStr) {
								System.out.println("--- Received Record : " + auditStr);
								return new AuditTrail(auditStr);
							}
						});
		
		/****************************************************************************
		 *                  Perform computations and write to output sink.
		 ****************************************************************************/
		//Print message for audit trail counts
		MapCountPrinter.printCount(auditTrail.map( i -> i), "Audit Trail : Last 5 secs");
		
		//Window by 5 seconds, count #of records and save to output
		DataStream<Tuple2<String,Integer>> recCount = auditTrail
						.map( i -> new Tuple2<String,Integer>(String.valueOf(System.currentTimeMillis()),1))
						.returns(Types.TUPLE(Types.STRING ,Types.INT))
						.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(5)))
						.reduce((x,y) -> new Tuple2<String, Integer>(x.f0, x.f1 + y.f1));
		
		Util.recreateDirectory(Util.FIVE_SEC_DATA_DIR);
		//Setup a streaming file sink to the output directory
		final StreamingFileSink<Tuple2<String,Integer>> countSink = StreamingFileSink
						.forRowFormat(new Path(Util.FIVE_SEC_DATA_DIR), new SimpleStringEncoder<Tuple2<String,Integer>>("UTF-8"))
						.build();
		//Add the file sink as sink to the DataStream.
		recCount.addSink(countSink);
		
		/****************************************************************************
		 *                  Setup data source and execute the Flink pipeline
		 ****************************************************************************/
		//Start the File Stream generator on a separate thread
		Util.printHeader("Starting File Data Generator...");
		Thread genThread = new Thread(new FileStreamGenerator());
		genThread.start();
		
		// execute the streaming pipeline
		Util.STR_ENV.execute("Flink Streaming Audit Trail Example");
	}
	
	
}
