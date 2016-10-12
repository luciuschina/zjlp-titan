package com.zjlp.face.bean;

import java.io.Serializable;

/**
 * Created by root on 10/11/16.
 */
public class Relation implements Serializable {

    private String userName;

    private String friendName;

    public  Relation(String userName, String friendName)  {
        this.userName = userName;
        this.friendName = friendName;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getFriendName() {
        return friendName;
    }

    public void setFriendName(String friendName) {
        this.friendName = friendName;
    }
}
