package com.myslyv4uk.kafka.flink.batch;

import com.myslyv4uk.kafka.flink.util.Util;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple4;

public class StudentsScores {
	
	public static void main(String[] args) throws Exception {
		
		Util.printHeader("Total Score of each student and subject....");
		
		/****************************************************************************
		 *                     Total Score
		 ****************************************************************************/
		//Read students list
		DataSet<Tuple4<String, String, Double, Double>> studentsList =
						Util.ENV.readCsvFile("flink-tweets-realtime-processor/src/main/resources/sales_orders.csv")
										.ignoreFirstLine()
										.parseQuotedStrings('\"')
										.types(String.class, String.class, Double.class,Double.class);
		
		
	}
}
