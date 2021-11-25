package com.myslyv4uk.kafka.flink.util;

import lombok.experimental.UtilityClass;
import org.apache.flink.api.java.ExecutionEnvironment;

@UtilityClass
public class Util {
	
	/*Get the execution environment.
	 While running inside IDE, it will create an embedded environment
	 While running inside a Flink installation, it will acquire the current context.
	 */
	public final ExecutionEnvironment ENV = ExecutionEnvironment.getExecutionEnvironment();
	
	public void printHeader(String msg) {
		
		System.out.println("\n**************************************************************");
		System.out.println(msg);
		System.out.println("---------------------------------------------------------------");
	}
}
