package com.myslyv4uk.kafka.flink;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

import java.util.List;

@Slf4j
public class SimpleBatchJob {
	
	public static void main(String[] args) throws Exception {
		
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		List<String> products = List.of("Mouse", "Keyboard", "Webcam");
		
		DataSet<String> dataSetProducts = env.fromCollection(products);
		
		log.info("Total Products = {}", dataSetProducts.count());
	}
}
