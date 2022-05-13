package com.watch.app.kafka.vo;
import java.io.Serializable;
import java.util.Date;

import com.fasterxml.jackson.annotation.JsonFormat;

public class WatchData implements Serializable{

    private String userId;
    private String userGender;
    private String userAge;
    private String season;
    private String episode;
    private String showId;
    private String showName;
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd HH:mm:ss", timezone="MST")
    private Date timestamp;
    private double rating;
    private double episodeLength;

    public WatchData(){

    }

    public WatchData(String userId, String userGender, String showId, String episode, String season,
                   Date timestamp, double rating, double episodeLength) {
        super();
        this.userId = userId;
        this.userGender = userGender;
        this.showId = showId;
        this.season = season;
        this.episode = episode;
        this.timestamp = timestamp;
        this.rating = rating;
        this.episodeLength = episodeLength;
    }

    public String getUserId() {
        return userId;
    }

    public String getUserGender() {
        return userGender;
    }

    public String getShowId() {
        return showId;
    }

    public String getSeason() {
        return season;
    }

    public String getEpisode() {
        return episode;
    }

    public Date getTimestamp() {
        return timestamp;
    }
    public void setTimestamp(Date time) { timestamp=time;}

    public double getRating() {
        return rating;
    }

    public double getEpisodeLength() {
        return episodeLength;
    }

}
