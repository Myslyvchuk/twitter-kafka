package com.myslyv4uk.kafka.flink.batch;

import com.myslyv4uk.kafka.flink.streamoperations.FilterOrdersByDate;
import com.myslyv4uk.kafka.flink.streamoperations.FlatMapCustomerTag;
import com.myslyv4uk.kafka.flink.streamoperations.MapTotalOrderPrice;
import com.myslyv4uk.kafka.flink.util.Util;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.api.java.tuple.Tuple8;

import static org.apache.flink.api.java.aggregation.Aggregations.SUM;

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
		DataSet<Tuple7<Integer, String, String, String, Integer, Double, String>> rawOrders =
						Util.ENV.readCsvFile("flink-tweets-realtime-processor/src/main/resources/sales_orders.csv")
										.ignoreFirstLine()
										.parseQuotedStrings('\"')
										.types(Integer.class, String.class, String.class, String.class, Integer.class, Double.class, String.class);
		
		Util.printHeader("Raw orders read from file");
		rawOrders.first(5).print();
		
		/****************************************************************************
		 *                  Compute Total Order Value for each record
		 ****************************************************************************/
		
		DataSet<Tuple8<Integer, String, String, String, Integer, Double, String, Double>> computedOrders =
						rawOrders.map(new MapTotalOrderPrice());
		
		Util.printHeader("Orders with Order Value computed");
		computedOrders.first(5).print();
		
		/****************************************************************************
		 *                 Extracts Tags by Customer into a separate dataset
		 ****************************************************************************/
		
		DataSet<Tuple2<String, String>> customerTags = rawOrders.flatMap(new FlatMapCustomerTag());
		
		Util.printHeader("Customer and Tags extracted as separate dataset");
		customerTags.first(10).print();
		
		/****************************************************************************
		 *                 Filter Orders for First 10 days of November
		 ****************************************************************************/
		
		DataSet<Tuple8<Integer, String, String, String, Integer, Double, String, Double>> filteredOrders =
						computedOrders.filter(new FilterOrdersByDate());
		
		Util.printHeader("Orders filtered for first 10 days of November");
		filteredOrders.first(5).print();
		
		System.out.println("\nTotal orders in first 10 days = " + filteredOrders.count());
		
		/****************************************************************************
		 *                Aggregate across all orders
		 ****************************************************************************/
		
		//Use Projections to filter subset of columns
		DataSet<Tuple2<Integer,Double>> orderColumns = computedOrders.project(4,7);
		
		//Find Average Order Size and Total Order Value
		DataSet<Tuple2<Integer, Double>> totalOrders = orderColumns
										.aggregate(SUM,0) //Total Order Item Count
										.and(SUM,1); //Total Order Value
		
		//Extract the Summary row tuple, by converting the DataSet to a List and
		//fetching the first record
		Tuple2<Integer, Double> sumRow = totalOrders.collect().get(0);
		
		Util.printHeader("Aggregated Order Data ");
		System.out.println(" Total Order Value = "
						+ sumRow.f1
						+"  Average Order Items = "
						+ sumRow.f0 * 1.0 / computedOrders.count() );
		
		
	}
}
