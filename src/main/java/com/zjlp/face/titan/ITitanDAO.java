package com.zjlp.face.titan;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface ITitanDAO {

    /**
     * 增加一个好友关系
     * @param userId
     * @param friendUserId
     */
    void addRelation(String userId, String friendUserId);

    /**
     * 删除一个好友关系
     * @param userId
     * @param friendUserId
     */
    void deleteRelation(String userId, String friendUserId);

    /**
     * 查询一度和二度好友
     * @param userId
     * @param friends
     * @return
     */
    Map<String, Integer> getFriendsLevel(String userId, List<String> friends);

    /**
     * 查询共同好友数
     * @param userId
     * @param friends
     * @return
     */
    Map<Object, Long> getComFriendsNum(String userId, List<String> friends);

    /**
     * 查詢共同好友列表
     * @param userId
     * @param anotherUserIds
     * @return
     */
    Map<String, Set<String>> getMultiComFriends(String userId, List<String> anotherUserIds);

    Set<String> getComFriends(String userId, String anotherUserId);

}
