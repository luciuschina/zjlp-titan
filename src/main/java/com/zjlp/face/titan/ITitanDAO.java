package com.zjlp.face.titan;

import java.util.List;
import java.util.Map;

public interface ITitanDAO {

    /**
     * 新增一个用户
     * @param userName
     * @return
     */
    String addUser(String userName);

    /**
     * 增加多个用户
     * @param userNames
     */
    void addUsers(List<String> userNames);

    /**
     * 增加一个好友关系
     * @param username
     * @param friendUsername
     * @param autoCommit true表示自动提交事物。false表示手动提交事务，适用于批量提交时。
     */
    void addRelation(String username, String friendUsername, Boolean autoCommit);

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
