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
     * 查询一度和二度好友
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
    Map<Object, Long> getComFriendsNum(String username, String[] friends);

    /**
     * 对好友数多的用户的一二度好友进行提前缓存
     * @param username
     */
    void cacheFor(String username);

    void closeTitanGraph();

}
