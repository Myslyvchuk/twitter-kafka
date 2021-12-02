package com.myslyv4uk.kafka.flink.stream.datagenerator;

import com.devskiller.jfairy.Fairy;
import com.devskiller.jfairy.producer.person.Person;
import com.myslyv4uk.kafka.flink.util.Util;
import com.opencsv.CSVWriter;
import lombok.SneakyThrows;

import java.io.FileWriter;
import java.util.Arrays;
import java.util.Random;

public class FileStreamGenerator implements Runnable {
	
	private static final String ANSI_RESET = "\u001B[0m";
	private static final String ANSI_BLUE = "\u001B[34m";
	private static final Random RANDOM = new Random();
	
	public static void main(String[] args) {
		FileStreamGenerator fsdg = new FileStreamGenerator();
		fsdg.run();
	}
	
	@SneakyThrows
	@Override
	public void run() {
		Util.recreateDirectory(Util.RAW_DATA_DIR);
		//Generate 100 sample audit records, one per each file
		for (int i = 0; i < 100; i++) {
			
			//Create a CSV Text array
			final Person person = getRandomPerson();
			String[] csvText = {String.valueOf(i),
							person.getFirstName(), //Generate a random user
							person.getCompany().getName(), 	//Generate a random company
							person.getEmail(), // Generate random email
							String.valueOf(System.currentTimeMillis()), //Capture current timestamp
							String.valueOf(RANDOM.nextInt(10) + 1), //Generate a random duration for the operation
							String.valueOf(RANDOM.nextInt(4) + 1) //Generate a random value for number of changes
			};
			//Open a new file for this record
			FileWriter auditFile = new FileWriter(Util.RAW_DATA_DIR + "/audit_trail_" + i + ".csv");
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
		return fairy.person();
	}
}