package com.myslyv4uk.kafka.flink.batch;

import com.myslyv4uk.kafka.flink.util.Util;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;

public class StudentsScores {
	
	public static void main(String[] args) throws Exception {
		
		Util.printHeader("Total Score of each student and subject....");
		
		/****************************************************************************
		 *                     Total Score
		 ****************************************************************************/
		//Read students list
		DataSet<Tuple4<String, String, Double, Double>> computedTotalScore =
						Util.ENV.readCsvFile("flink-tweets-realtime-processor/src/main/resources/student_scores.csv")
										.ignoreFirstLine()
										.parseQuotedStrings('\"')
										.types(String.class, String.class, Double.class,Double.class);
		
		System.out.println("First 5 students");
		computedTotalScore.first(5).print();
		
		/****************************************************************************
		 *            1. Compute Total Score for each student and subject
		 ****************************************************************************/
		DataSet<Tuple4<String, String, Integer, Double>> computedTotalScores = computedTotalScore
						.map(new MapFunction<Tuple4<String, String, Double, Double>, Tuple4<String, String, Integer, Double>>() {
							@Override
							public Tuple4<String, String, Integer, Double> map(Tuple4<String, String, Double, Double> studentList) {
								return new Tuple4<>(
												studentList.f0,
												studentList.f1,
												1, // acc for grouping
												studentList.f2 + studentList.f3);
							}
						});
		
		Util.printHeader("Total Score for each student and subject computed");
		computedTotalScores.first(5).print();
		
		/****************************************************************************
		 *                 2.Filter Subjects, get only physics
		 ****************************************************************************/
		DataSet<Tuple4<String, String, Integer, Double>> filteredPhysics = computedTotalScores
						.filter(new FilterFunction<Tuple4<String, String, Integer, Double>>() {
							@Override
							public boolean filter(Tuple4<String, String, Integer, Double> computedTotal) {
								return computedTotal.f1.equals("Physics");
							}
						});
		
		Util.printHeader("Subjects filtered for first physics");
		filteredPhysics.first(5).print();
		
		System.out.println("\nTotal students who learnt physics = " + filteredPhysics.count());
		
		/****************************************************************************
		 *               3.Compute Average Score by Students across Subjects
		 ****************************************************************************/
		DataSet<Tuple2<String, Double>> averageTotal = computedTotalScores.<Tuple3<String, Integer, Double>>
										project(0, 2, 3)
						.groupBy(0)
						.reduce(new ReduceFunction<Tuple3<String, Integer, Double>>() {
							@Override
							public Tuple3<String, Integer, Double> reduce(
											Tuple3<String, Integer, Double> rec1,
											Tuple3<String, Integer, Double> rec2) {
								return new Tuple3<>(rec1.f0, rec1.f1 + rec2.f1, rec1.f2 + rec2.f2);
							}
						})
						.map(i -> Tuple2.of(i.f0, i.f2 / i.f1))
						.returns(Types.TUPLE(Types.STRING, Types.DOUBLE));  //Summarize by Product
		
		Util.printHeader("Average total score for each student across all subjects");
		averageTotal.print();
		
		/****************************************************************************
		 *          4. Find Top Student by Subject (maximum total score )
		 ****************************************************************************/
		DataSet<Tuple3<String,String, Double>> highestTotalScorePerSubject = computedTotalScore
						.<Tuple3<String, String, Double>>project(0,1,3)
						.groupBy(1)
						.reduce(new ReduceFunction<Tuple3<String, String, Double>>() {
							@Override
							public Tuple3<String, String, Double> reduce(
											Tuple3<String, String, Double> rec1,
											Tuple3<String, String, Double> rec2) {
								return (rec1.f2 > rec2.f2)? rec1 : rec2;
							}
						});
		
		Util.printHeader("Top Student by Subject");
		highestTotalScorePerSubject
						.project(1,0,2) //Rearrange Tuple so Subject comes first
						.print();
		
	}
}
