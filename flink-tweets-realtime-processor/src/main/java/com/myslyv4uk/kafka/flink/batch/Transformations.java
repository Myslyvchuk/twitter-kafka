package com.myslyv4uk.kafka.flink.batch;

import com.myslyv4uk.kafka.flink.util.Util;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple7;

public class Transformations {
	
	public static void main(String[] args) throws Exception {
		
		Util.printHeader("Starting Transformation...");
		
		/****************************************************************************
		 *                  Read CSV file into a DataSet
		 ****************************************************************************/
		/*
			Make sure that the tuple with the correct number of elements is chosen
			to match the number of columns in the CSV file.
		*/
		
		DataSet<Tuple7<Integer,String, String, String, Integer, Double, String>> rawOrders =
						Util.ENV.readCsvFile("flink-tweets-realtime-processor/src/main/resources/sales_orders.csv")
						.ignoreFirstLine()
						.parseQuotedStrings('\"')
						.types(Integer.class, String.class, String.class, String.class, Integer.class, Double.class,String.class);
		
		Util.printHeader("Raw orders read from file");
		rawOrders.first(5).print();
		
	}
}
