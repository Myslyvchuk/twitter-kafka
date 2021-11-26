package com.myslyv4uk.kafka.flink.batch;

import com.myslyv4uk.kafka.flink.util.Util;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.configuration.Configuration;

import java.util.HashMap;
import java.util.Map;

public class BroadcastOperations {
	
	public static void main(String[] args) throws Exception {
		
		Util.printHeader("Starting Broadcast Operations....");
		
		/****************************************************************************
		 *                      Setting up the Broadcast Variable
		 ****************************************************************************/
		//Create a map of discount rates by product
		Map<String,Double> productDiscounts = new HashMap<>();
		productDiscounts.put("Mouse", 0.05);
		productDiscounts.put("Keyboard", 0.10);
		productDiscounts.put("Webcam",0.075);
		productDiscounts.put("Headset",0.10);
		
		DataSet<Map<String, Double>> dsDiscounts = Util.ENV.fromElements(productDiscounts);
		dsDiscounts.print();
		
		/****************************************************************************
		 *                      Read Orders and apply discounts by Order
		 ****************************************************************************/
		//Read raw order data
		DataSet<Tuple7<Integer, String, String, String, Integer, Double, String>> rawOrders =
						Util.ENV.readCsvFile("flink-tweets-realtime-processor/src/main/resources/sales_orders.csv")
						.ignoreFirstLine()
						.parseQuotedStrings('\"')
						.types(Integer.class, String.class, String.class, String.class, Integer.class, Double.class, String.class);

		//Define an RichMap function that takes a broadcast variable
		DataSet<Tuple2<Integer, Double>> orderNetValues =
						rawOrders
										.map(
														new RichMapFunction<Tuple7<Integer, String, String, String, Integer, Double, String>, //Input
																								Tuple2<Integer, Double>>() {     //Output (Order ID, Net Rate)
															//Instance variable to hold the discounts map
															private Map<String, Double> productDiscounts;
															
															//Additional function where broadcast variable can be read
															@Override
															public void open(Configuration params) {
																
																//Read broadcast variable into instance variable
																this.productDiscounts = (Map<String, Double>) this.getRuntimeContext()
																				.getBroadcastVariable("bcDiscounts").get(0);
															}
															
															@Override
															public Tuple2<Integer, Double> map(
																			Tuple7<Integer, String, String, String, Integer, Double, String> order) {
																
																//Compute net order value = Quantity * Rate * ( 1 - Discount )
																Double netRate = order.f4 * order.f5 * (1 - productDiscounts.get(order.f2));
																return new Tuple2<>(order.f0, netRate);
															}
														})
										//Pass Broadcast variable to the map function
										.withBroadcastSet(dsDiscounts, "bcDiscounts");
		
		Util.printHeader("Net Order Rates");
		orderNetValues.first(10).print();
	}
}
