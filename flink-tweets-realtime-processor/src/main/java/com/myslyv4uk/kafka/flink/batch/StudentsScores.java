package com.myslyv4uk.kafka.flink.batch;

import com.myslyv4uk.kafka.flink.util.Util;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;

public class StudentsScores {
	
	public static void main(String[] args) throws Exception {
		
		Util.printHeader("Total Score of each student and subject....");
		
		/****************************************************************************
		 *                     Total Score
		 ****************************************************************************/
		//Read students list
		DataSet<Tuple4<String, String, Double, Double>> studentsList =
						Util.ENV.readCsvFile("flink-tweets-realtime-processor/src/main/resources/student_scores.csv")
										.ignoreFirstLine()
										.parseQuotedStrings('\"')
										.types(String.class, String.class, Double.class,Double.class);
		
		System.out.println("First 5 students");
		studentsList.first(5).print();
		
		/****************************************************************************
		 *            1. Compute Total Score for each student and subject
		 ****************************************************************************/
		DataSet<Tuple3<String, String, Double>> computedTotalScores = studentsList
						.map(new MapFunction<Tuple4<String, String, Double, Double>, Tuple3<String, String, Double>>() {
							@Override
							public Tuple3<String, String, Double> map(Tuple4<String, String, Double, Double> studentList) throws Exception {
								return new Tuple3<>(studentList.f0, studentList.f1, studentList.f2 + studentList.f3);
							}
						});
		
		Util.printHeader("Total Score for each student and subject computed");
		computedTotalScores.first(5).print();
		
		/****************************************************************************
		 *                 Filter Subjects, get only physics
		 ****************************************************************************/
		DataSet<Tuple3<String, String, Double>> filteredPhysics = computedTotalScores.
						filter(new FilterFunction<Tuple3<String, String, Double>>() {
							@Override
							public boolean filter(Tuple3<String, String, Double> computedTotal) {
								return computedTotal.f1.equals("Physics");
							}
						});
		
		Util.printHeader("Subjects filtered for first physics");
		filteredPhysics.first(5).print();
		
		System.out.println("\nTotal students who learnt physics = " + filteredPhysics.count());
		
		
	}
}
