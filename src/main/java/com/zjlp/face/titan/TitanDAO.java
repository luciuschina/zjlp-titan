package com.zjlp.face.titan;

import com.zjlp.face.bean.Relation;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

import java.util.List;
import java.util.Map;

public interface TitanDAO {
    String addUser(String userName, Boolean autoCommit);

    String addUser(String userName);

    void addRelationByVID(String userId, String friendId, Boolean autoCommit);

    void addRelationByVID(String userId, String friendId);

    void addRelationByVID(String userId, String friendId,GraphTraversalSource g, Boolean autoCommit);

    void addUsers(String[] userNames);

    void addUsers(String[] userNames, int patchLength);

    void addRelation(Relation relation);

    void addRelationsByUsername(Relation[] relations);

    void addRelationsByUsername(Relation[] relations, int patchLength);

    void addRelationsByUsername(String username, List<String> friendsList);

    void dropUser(String userName);

    void dropUsers(String[] userNames);

    void dropRelation(Relation relation);

    void dropRelations(Relation[] relations);

    List<String> getOneDegreeFriends(String username);

    List<String> getOneDegreeFriends(String username, List<String> friends);

    List<String> getTwoDegreeFriends(String username);

    List<String> getTwoDegreeFriends(String username, List<String> friends);

    Map<String, Integer> getOneAndTwoDegreeFriends(String username, List<String> friends);

    Map<String, Integer> getComFriendsNum(String username, String[] friends);


    public void closeTitanGraph();
}
