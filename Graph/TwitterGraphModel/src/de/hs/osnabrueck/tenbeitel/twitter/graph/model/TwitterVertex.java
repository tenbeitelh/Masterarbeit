package de.hs.osnabrueck.tenbeitel.twitter.graph.model;

import com.google.gson.Gson;

public class TwitterVertex {
	private String clusterId;
	private String twitterId;
	private String createdDate;
	private String tweetMessage;
	private String user;

	public TwitterVertex() {
	}

	public TwitterVertex(String clusterId, String twitterId, String createdDate, String tweetMessage, String user) {
		this.clusterId = clusterId;
		this.twitterId = twitterId;
		this.createdDate = createdDate;
		this.tweetMessage = tweetMessage;
		this.user = user;
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

	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((clusterId == null) ? 0 : clusterId.hashCode());
		result = prime * result + ((createdDate == null) ? 0 : createdDate.hashCode());
		result = prime * result + ((tweetMessage == null) ? 0 : tweetMessage.hashCode());
		result = prime * result + ((twitterId == null) ? 0 : twitterId.hashCode());
		result = prime * result + ((user == null) ? 0 : user.hashCode());
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
		if (clusterId == null) {
			if (other.clusterId != null)
				return false;
		} else if (!clusterId.equals(other.clusterId))
			return false;
		if (createdDate == null) {
			if (other.createdDate != null)
				return false;
		} else if (!createdDate.equals(other.createdDate))
			return false;
		if (tweetMessage == null) {
			if (other.tweetMessage != null)
				return false;
		} else if (!tweetMessage.equals(other.tweetMessage))
			return false;
		if (twitterId == null) {
			if (other.twitterId != null)
				return false;
		} else if (!twitterId.equals(other.twitterId))
			return false;
		if (user == null) {
			if (other.user != null)
				return false;
		} else if (!user.equals(other.user))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "[clusterId=" + clusterId + ",\n" + "twitterId=" + twitterId + ",\n" + " createdDate=" + createdDate
				+ ",\n" + " tweetMessage=" + tweetMessage + ",\n" + " user=" + user + "]";
	}

	public String toJson() {
		Gson gson = new Gson();
		return gson.toJson(this);
	}

	public TwitterVertex fromJson(String json) {
		Gson gson = new Gson();
		return gson.fromJson(json, TwitterVertex.class);
	}

}
