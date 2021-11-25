package com.myslyv4uk.kafka.flink.batch;

import com.myslyv4uk.kafka.flink.util.Util;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.core.fs.FileSystem;

import java.util.List;

@Slf4j
public class SimpleBatchJob {
	
	public static void main(String[] args) throws Exception {
		
		//Create a list of products
		List<String> products = List.of("Mouse", "Keyboard", "Webcam");
		
		//Convert the list into a Flink DataSet
		DataSet<String> dataSetProducts = Util.ENV.fromCollection(products);
		
		 /* Count the number of items in the DataSet
				Flink uses lazy execution, so all code is executed only when
				an output is requested.
		*/
		log.info("Total Products = {}", dataSetProducts.count());
		
		// write results to a text file
		dataSetProducts.writeAsText("output/tempdata.csv", FileSystem.WriteMode.OVERWRITE);
		Util.ENV.execute();
		
		//Print execution plan
		log.info(Util.ENV.getExecutionPlan());
	}
}
