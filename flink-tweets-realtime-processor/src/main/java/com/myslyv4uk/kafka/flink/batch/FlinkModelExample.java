package com.myslyv4uk.kafka.flink.batch;

import com.myslyv4uk.kafka.flink.model.ProductVendor;
import com.myslyv4uk.kafka.flink.util.Util;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.List;

public class FlinkModelExample {
	
	public static void main(String[] args) throws Exception {
		Util.printHeader("Starting example with model...");
		
		DataSet<ProductVendor> productVendor =
						Util.EXC_ENV.readCsvFile("flink-tweets-realtime-processor/src/main/resources/product_vendor.csv")
										.ignoreFirstLine()
										.pojoType(ProductVendor.class, "product", "vendor");
		
		//Print the contents
		System.out.println("Product Vendor Details loaded : ");
		productVendor.print();
		
		//Compute Vendor Product Counts and print them
		DataSet<Tuple2<String,Integer>> vendorSummary = productVendor
						.map(i -> Tuple2.of(i.getVendor(), 1)) // Vendor and 1 count per record
						.returns(Types.TUPLE(Types.STRING ,Types.INT))
						.groupBy(0) //Group by Vendor
						.reduce( (sumRow,nextRow) ->   //Reduce operation
										Tuple2.of(sumRow.f0, sumRow.f1+ nextRow.f1));
		
		//Convert a dataset to a list and pretty print
		System.out.printf("\n%15s  %10s\n\n", "Vendor", "Products");
		
		List<Tuple2<String,Integer>> vendorList = vendorSummary.collect();
		for( Tuple2<String, Integer> vendorRecord :vendorList) {
			System.out.printf("%15s  %10s\n", vendorRecord.f0, vendorRecord.f1);
		}
	}
}
