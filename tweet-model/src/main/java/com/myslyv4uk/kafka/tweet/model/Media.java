package com.myslyv4uk.kafka.tweet.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.List;
import java.util.Map;

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
	@JsonProperty("additional_media_info")
	private AdditionalMediaInfo additionalMediaInfo;
	private String url;
	@JsonProperty("expanded_url")
	private String expandedUrl;
	@JsonProperty("display_url")
	private String displayUrl;
	private String type;
	private Map<String, Measures> sizes;
	@JsonProperty("video_info")
	private VideoInfo videoInfo;
	@JsonProperty("source_status_id")
	private long sourceStatusId;
	@JsonProperty("source_status_id_str")
	private String sourceStatusIdStr;
	@JsonProperty("source_user_id")
	private long sourceUserId;
	@JsonProperty("source_user_id_str")
	private String sourceUserIdStr;
	private String description;
}
