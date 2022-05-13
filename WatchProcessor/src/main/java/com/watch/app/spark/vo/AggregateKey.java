package com.watch.app.spark.vo;

import java.io.Serializable;
import java.util.Date;

/**
 * Key class for calculations
 */
public class AggregateKey implements Serializable {
	
	private String userId;
	private String showId;

	public AggregateKey(String userId, String showId) {
		super();
		this.userId = userId;
		this.showId = showId;
	}

	public String getUserId() {
		return userId;
	}

	public String getShowId() {
		return showId;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((userId == null) ? 0 : userId.hashCode());
		result = prime * result + ((showId == null) ? 0 : showId.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if(obj !=null && obj instanceof AggregateKey){
			AggregateKey other = (AggregateKey)obj;
			if(other.getUserId() != null && other.getShowId() != null){
				if((other.getUserId().equals(this.userId)) && (other.getShowId().equals(this.showId))){
					return true;
				} 
			}
		}
		return false;
	}
	

}
