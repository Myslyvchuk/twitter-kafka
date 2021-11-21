package com.myslyv4uk.kafka.tweet.model;

import lombok.Data;

import java.util.List;


@Data
public class BoundingBox {
		private String type;
		private List<List<List<Float>>> coordinates;
}
