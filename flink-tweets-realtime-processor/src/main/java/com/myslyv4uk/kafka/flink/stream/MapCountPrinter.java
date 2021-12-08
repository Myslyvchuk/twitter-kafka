package com.myslyv4uk.kafka.flink.stream;

import com.myslyv4uk.kafka.flink.util.FlinkUtil;
import lombok.experimental.UtilityClass;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

@UtilityClass
public class MapCountPrinter {
	
	public void printCount(DataStream<Object> dataStream, String message) {
		
		//Print the summary
		dataStream
						.map(i -> new Tuple2<>(message, 1)) //Generate a counter record for each input record
						.returns(Types.TUPLE(Types.STRING, Types.INT))
						.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(5))) //Window by time = 5 seconds
						.reduce((x, y) -> (new Tuple2<>(x.f0, x.f1 + y.f1))) //Sum the number of records for each 5 second interval
						.map((MapFunction<Tuple2<String, Integer>, Integer>) recCount -> {
							FlinkUtil.printHeader(recCount.f0 + " : " + recCount.f1);
							return recCount.f1;
						});
	}
}
