package com.zjlp.face.titan;

import java.util.Map;

public interface ITitanDAO {

    /**
     * 增加一个好友关系
     * @param username
     * @param friendUsername
     */
    void addRelation(String username, String friendUsername);

    /**
     * 删除一个好友关系
     * @param username
     * @param friendUsername
     */
    void deleteRelation(String username, String friendUsername);

    /**
     * 查询二度好友
     * @param username
     * @param friends
     * @return
     */
    Map<String, Integer> getFriendsLevel(String username, String[] friends);

    /**
     * 查询共同好友数
     * @param username
     * @param friends
     * @return
     */
    Map<String, Integer> getComFriendsNum(String username, String[] friends);

    void closeTitanGraph();

}
