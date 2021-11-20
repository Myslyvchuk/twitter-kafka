package com.myslyv4uk.kafka.tweet.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Data;

import java.util.List;

@Data
public class Media {
	
	private long id;
	@JsonProperty("id_str")
	private String idStr;
	@JsonProperty("media_url")
	private String mediaUrl;
	@JsonProperty("media_url_https")
	private String mediaUrlHttps;
	private List<Integer> indices;
	private String url;
	@JsonProperty("expanded_url")
	private String expandedUrl;
	@JsonProperty("display_url")
	private String displayUrl;
	private String type;
	private Size sizes;
	
	public static void main(String[] args) throws JsonProcessingException {
		String json = "\"sizes\": {\n" +
						"          \"large\": {\n" +
						"            \"w\": 1200,\n" +
						"            \"h\": 750,\n" +
						"            \"resize\": \"fit\"\n" +
						"          },\n" +
						"          \"thumb\": {\n" +
						"            \"w\": 150,\n" +
						"            \"h\": 150,\n" +
						"            \"resize\": \"crop\"\n" +
						"          },\n" +
						"          \"small\": {\n" +
						"            \"w\": 680,\n" +
						"            \"h\": 425,\n" +
						"            \"resize\": \"fit\"\n" +
						"          },\n" +
						"          \"medium\": {\n" +
						"            \"w\": 1200,\n" +
						"            \"h\": 750,\n" +
						"            \"resize\": \"fit\"\n" +
						"          }\n" +
						"        }\n" +
						"      }\n" +
						"\n";
		Size sizes1 = new ObjectMapper().readValue(json, Size.class);
		System.out.println(sizes1);
	}
}
