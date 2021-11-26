package com.myslyv4uk.kafka.flink.batch;

import com.myslyv4uk.kafka.flink.model.ProductVendor;
import com.myslyv4uk.kafka.flink.streamoperations.OrderVendorJoinSelector;
import com.myslyv4uk.kafka.flink.util.Util;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple7;

public class JoinOperations {
	
	public static void main(String[] args) throws Exception {
		
		Util.printHeader("Starting the Dataset Join Program....");
		
		/****************************************************************************
		 *                      Load the datasets
		 ****************************************************************************/
		
		/* Load the Orders into a Tuple Dataset
		 */
		DataSet<Tuple7<Integer, String, String, String, Integer, Double, String>> rawOrders =
            Util.ENV.readCsvFile("flink-tweets-realtime-processor/src/main/resources/sales_orders.csv")
						.ignoreFirstLine()
						.parseQuotedStrings('\"')
						.types(Integer.class, String.class, String.class, String.class, Integer.class, Double.class, String.class);

            /* Load the Product vendor information into a POJO Dataset.
                CSVs can be loaded as and processed as classes.
            */
		DataSet<ProductVendor> productVendor =
            Util.ENV.readCsvFile("flink-tweets-realtime-processor/src/main/resources/product_vendor.csv")
						.ignoreFirstLine()
						.pojoType(ProductVendor.class, "product", "vendor");
		
		/****************************************************************************
		 *                      Join the datasets
		 ****************************************************************************/
		
		DataSet<Tuple2<String, Integer>> vendorOrders =
            rawOrders.join(productVendor)     //Second DataSet
						.where(2)           //Join Field from first Dataset
						.equalTo("product") //Join Field from second Dataset
						.with(new OrderVendorJoinSelector()); //Returned Data
		
		System.out.println("\n Summarized Item Count by Vendor : ");
		
		//Print total item count by vendor
		//Use a map function to do a pretty print.
		vendorOrders
						.groupBy(0)
						.sum(1)
						.print();
	}
	
}
