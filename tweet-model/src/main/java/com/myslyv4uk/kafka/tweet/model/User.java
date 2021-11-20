package com.myslyv4uk.kafka.tweet.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.List;

@Data
public class User {
	
	private Long id;
	@JsonProperty("id_str")
	private String idStr;
	private String name;
	@JsonProperty("screen_name")
	private String screenName;
	private String location;
	private String url;
	private String description;
	@JsonProperty("translator_type")
	private String translatorType;
	@JsonProperty("protected")
	private boolean isProtected;
	private boolean verified;
	@JsonProperty("followers_count")
	private Integer followersCount;
	@JsonProperty("friends_count")
	private Integer friendsCount;
	@JsonProperty("listed_count")
	private Integer listedCount;
	@JsonProperty("favourites_count")
	private Integer favouritesCount;
	@JsonProperty("statuses_count")
	private Integer statusesCount;
	@JsonProperty("created_at")
	private String createdAt;
	@JsonProperty("utc_offset")
	private String utcOffset;
	@JsonProperty("time_zone")
	private String timeZone;
	@JsonProperty("geo_enabled")
	private String geoEnabled;
	private String lang;
	@JsonProperty("contributors_enabled")
	private boolean contributorsEnabled;
	@JsonProperty("is_translator")
	private boolean isTranslator;
	@JsonProperty("profile_background_color")
	private String profileBackgroundColor;
	@JsonProperty("profile_background_image_url")
	private String profileBackgroundImageUrl;
	@JsonProperty("profile_background_image_url_https")
	private String profileBackgroundImageUrlHttps;
	@JsonProperty("profile_background_tile")
	private boolean profileBackgroundTile;
	@JsonProperty("profile_link_color")
	private String profileLinkColor;
	@JsonProperty("profile_sidebar_border_color")
	private String profileSidebarBorderColor;
	@JsonProperty("profile_sidebar_fill_color")
	private String profileSidebarFillColor;
	@JsonProperty("profile_text_color")
	private String profileTextColor;
	@JsonProperty("profile_use_background_image")
	private boolean profileUseBackgroundImage;
	@JsonProperty("profile_image_url")
	private String profileImageUrl;
	@JsonProperty("profile_image_url_https")
	private String profileImageUrlHttps;
	@JsonProperty("profile_banner_url")
	private String profileBannerUrl;
	@JsonProperty("default_profile")
	private boolean defaultProfile;
	@JsonProperty("default_profile_image")
	private boolean defaultProfileImage;
	private String following;
	@JsonProperty("follow_request_sent")
	private String followRequestSent;
	private String notifications;
	@JsonProperty("withheld_in_countries")
	private List<String> withheldInCountries;
}
