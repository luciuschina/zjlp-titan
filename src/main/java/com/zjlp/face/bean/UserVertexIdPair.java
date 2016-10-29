package com.zjlp.face.bean;

import java.io.Serializable;

public class UserVertexIdPair implements Serializable {

    private String userId;

    private String vid;

    public UserVertexIdPair(String userId, String vid)  {
        this.userId = userId;
        this.vid = vid;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getVid() {
        return vid;
    }

    public void setVid(String vid) {
        this.vid = vid;
    }
}
