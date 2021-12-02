package com.myslyv4uk.kafka.flink.util;

import lombok.experimental.UtilityClass;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

@UtilityClass
public class Util {
	/****************************************************************************
	 *                 Setup Flink environment.
	 ****************************************************************************/
	/*Get the execution environment.
	 While running inside IDE, it will create an embedded environment
	 While running inside a Flink installation, it will acquire the current context.
	 */
	public final ExecutionEnvironment EXC_ENV = ExecutionEnvironment.getExecutionEnvironment();
	
	/*	Set up the streaming execution environment
			Keeps the ordering of records. Else multiple threads can change
			sequence of printing.
	*/
	public final StreamExecutionEnvironment STR_ENV = StreamExecutionEnvironment.getExecutionEnvironment()
					.setParallelism(1);
	
	//Define the data directory to output the files
	public final String DATA_DIR = "flink-tweets-realtime-processor/data/raw_audit_trail";
	
	public void printHeader(String msg) {
		
		System.out.println("\n**************************************************************");
		System.out.println(msg);
		System.out.println("---------------------------------------------------------------");
	}
}
