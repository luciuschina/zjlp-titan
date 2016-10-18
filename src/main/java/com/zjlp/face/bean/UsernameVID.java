package com.zjlp.face.bean;

import java.io.Serializable;

public class UsernameVID implements Serializable {

    private String userName;

    private String vid;

    public UsernameVID(String userName, String vid)  {
        this.userName = userName;
        this.vid = vid;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getVid() {
        return vid;
    }

    public void setVid(String vid) {
        this.vid = vid;
    }
}
