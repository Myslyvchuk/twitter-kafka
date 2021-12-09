package com.myslyv4uk.kafka.flink.stream.datagenerator;

import com.devskiller.jfairy.Fairy;
import com.devskiller.jfairy.producer.person.Person;
import com.devskiller.jfairy.producer.person.PersonProperties;
import com.myslyv4uk.kafka.flink.util.FlinkUtil;
import com.opencsv.CSVWriter;
import lombok.SneakyThrows;

import java.io.FileWriter;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class FileStreamGenerator implements Runnable {
	
	private static final String ANSI_RESET = "\u001B[0m";
	private static final String ANSI_BLUE = "\u001B[34m";
	private static final Random RANDOM = new Random();
	
	public static void main(String[] args) {
		new FileStreamGenerator().run();
	}
	
	@SneakyThrows
	@Override
	public void run() {
		FlinkUtil.recreateDirectory(FlinkUtil.RAW_DATA_DIR);
		//Generate 100 sample audit records, one per each file
		for (int i = 0; i < 100; i++) {
			
			//Create a CSV Text array
			final Person person = getRandomPerson();
			String[] csvText = {
							String.valueOf(i),
							person.getFirstName(), //Generate a random user
							person.getMiddleName(),  //Generate a random entity
							person.getEmail(), // Generate random email
							String.valueOf(System.currentTimeMillis()), //Capture current timestamp
							String.valueOf(RANDOM.nextInt(10) + 1), //Generate a random duration for the operation
							String.valueOf(RANDOM.nextInt(4) + 1) //Generate a random value for number of changes
			};
			//Open a new file for this record
			FileWriter auditFile = new FileWriter(FlinkUtil.RAW_DATA_DIR + "/audit_trail_" + i + ".csv");
			try (CSVWriter auditCSV = new CSVWriter(auditFile)) {
				//Write the audit record and close the file
				auditCSV.writeNext(csvText);
				System.out.println(ANSI_BLUE + "FileStream Generator : Creating File : " + Arrays.toString(csvText) + ANSI_RESET);
				auditCSV.flush();
			}
			
			//Sleep for a random time ( 1 - 3 secs) before the next record.
			Thread.sleep(RANDOM.nextInt(2000) + 1);
		}
		
	}
	
	private static Person getRandomPerson() {
		Fairy fairy = Fairy.create();
		List<String> names = List.of("Bob", "Alice", "John", "Bohdan", "Alex", "Iren", "Ivan", "Jack", "Harry");
		List<String> email = List.of("Customer", "SalesRep");
		List<String> operations = List.of("Create", "Modify","Query","Delete");
		return fairy.person(
						PersonProperties.withFirstName(names.get(RANDOM.nextInt(9))),
						PersonProperties.withMiddleName(email.get(RANDOM.nextInt(2))),
							PersonProperties.withEmail(operations.get(RANDOM.nextInt(4))));
	}
}