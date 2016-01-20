package de.hs.osnabrueck.tenbeitel.twitter.graph.model;

import com.google.gson.Gson;

public class TwitterVertex {
	private static final String NOT_AVAILABLE = "N/A";

	private String clusterId;
	private String twitterId;
	private String createdDate;
	private String tweetMessage;
	private String userId;
	private String userScreenName;

	public TwitterVertex() {
	}

	public TwitterVertex(String clusterId, String twitterId, String createdDate, String tweetMessage, String userId,
			String userScreenName) {
		super();
		this.clusterId = clusterId;
		this.twitterId = twitterId;
		this.createdDate = createdDate;
		this.tweetMessage = tweetMessage;
		this.userId = userId;
		this.userScreenName = userScreenName;
	}

	public static TwitterVertex createEmptyVertex(String twitterId) {
		return new TwitterVertex(NOT_AVAILABLE, twitterId, NOT_AVAILABLE, NOT_AVAILABLE, NOT_AVAILABLE, NOT_AVAILABLE);
	}

	public String getClusterId() {
		return clusterId;
	}

	public void setClusterId(String clusterId) {
		this.clusterId = clusterId;
	}

	public String getTwitterId() {
		return twitterId;
	}

	public void setTwitterId(String twitterId) {
		this.twitterId = twitterId;
	}

	public String getCreatedDate() {
		return createdDate;
	}

	public void setCreatedDate(String createdDate) {
		this.createdDate = createdDate;
	}

	public String getTweetMessage() {
		return tweetMessage;
	}

	public void setTweetMessage(String tweetMessage) {
		this.tweetMessage = tweetMessage;
	}

	public String getUserId() {
		return userId;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}

	public String getUserScreenName() {
		return userScreenName;
	}

	public void setUserScreenName(String userScreenName) {
		this.userScreenName = userScreenName;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((twitterId == null) ? 0 : twitterId.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		TwitterVertex other = (TwitterVertex) obj;
		if (this.twitterId.equalsIgnoreCase(other.getTwitterId())) {
			return true;
		}
		return false;
	}

	@Override
	public String toString() {
		return "TwitterVertex [clusterId=" + clusterId + ",\n twitterId=" + twitterId + ",\n createdDate=" + createdDate
				+ ",\n tweetMessage=" + tweetMessage + ",\n userId=" + userId + ",\n userScreenName=" + userScreenName
				+ "]";
	}

	public String toJson() {
		Gson gson = new Gson();
		return gson.toJson(this);
	}

	public static TwitterVertex fromJson(String json) {
		Gson gson = new Gson();
		return gson.fromJson(json, TwitterVertex.class);
	}

}
