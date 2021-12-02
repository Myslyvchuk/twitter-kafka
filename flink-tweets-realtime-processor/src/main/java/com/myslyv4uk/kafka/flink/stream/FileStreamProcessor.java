package com.myslyv4uk.kafka.flink.stream;

import com.myslyv4uk.kafka.flink.model.AuditTrail;
import com.myslyv4uk.kafka.flink.stream.datagenerator.FileStreamGenerator;
import com.myslyv4uk.kafka.flink.util.Util;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;

public class FileStreamProcessor {
	
	public static void main(String[] args) throws Exception {
		/****************************************************************************
		 *                  Read CSV File Stream into a DataStream.
		 ****************************************************************************/
		//Define the data directory to monitor for new files
		
		//Define the text input format based on the directory
		final TextInputFormat auditFormat = new TextInputFormat(new Path(Util.DATA_DIR));
		
		//Create a DataStream based on the directory
		final DataStream<String> auditTrailStr = Util.STR_ENV.readFile(auditFormat, Util.DATA_DIR,//Director to monitor
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
