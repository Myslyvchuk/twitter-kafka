package com.myslyv4uk.kafka.tweet.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.List;

@Data
public class Tweet {
	
	@JsonProperty("created_at")
	private String createdAt;
	private long id;
	@JsonProperty("id_str")
	private String idStr;
	private String text;
	@JsonProperty("display_text_range")
	private List<Integer> displayTextRange;
	private String source;
	private boolean truncated;
	@JsonProperty("in_reply_to_status_id")
	private Integer inReplyToStatusId;
	@JsonProperty("in_reply_to_status_id_str")
	private String inReplyToStatusIdStr;
	@JsonProperty("in_reply_to_user_id")
	private Integer inReplyToUserId;
	@JsonProperty("in_reply_to_user_id_str")
	private String inReplyToUserStr;
	@JsonProperty("in_reply_to_screen_name")
	private String inReplyToScreenName;
	private User user;
	private String geo;
	private String coordinates;
	private String place;
	private String contributors;
	@JsonProperty("retweeted_status")
	private Tweet retweetedStatus;
	@JsonProperty("quoted_status_id")
	private Long quotedStatusId;
	@JsonProperty("quoted_status_id_str")
	private String quoted_status_id_str;
	@JsonProperty("quoted_status")
	private Tweet quotedStatus;
	@JsonProperty("quoted_status_permalink")
	private QuotedStatusPermalink quotedStatusPermalink;
	@JsonProperty("is_quote_status")
	private boolean isQuoteStatus;
	@JsonProperty("extended_tweet")
	private ExtendedTweet extendedTweet;
	@JsonProperty("quote_count")
	private Integer quoteCount;
	@JsonProperty("reply_count")
	private Integer replyCount;
	@JsonProperty("retweet_count")
	private boolean retweetCount;
	@JsonProperty("favorite_count")
	private Integer favoriteCount;
	private Entity entities;
	private boolean favorited;
	private boolean retweeted;
	@JsonProperty("possibly_sensitive")
	private boolean possiblySensitive;
	@JsonProperty("filter_level")
	private String filterLevel;
	private String lang;
	@JsonProperty("timestamp_ms")
	private String timestampMs;
}
