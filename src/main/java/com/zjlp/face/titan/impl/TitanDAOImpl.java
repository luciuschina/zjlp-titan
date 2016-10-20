package com.zjlp.face.titan.impl;

import com.thinkaurelius.titan.core.SchemaViolationException;
import com.zjlp.face.bean.UsernameVID;
import com.zjlp.face.titan.IEsDAO;
import com.zjlp.face.titan.TitanCon;
import com.zjlp.face.titan.ITitanDAO;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.structure.T;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Service("TitanDAOImpl")
public class TitanDAOImpl extends TitanCon implements ITitanDAO, Serializable {
    private static IEsDAO esDAO = new EsDAOImpl();

    private static final Logger LOGGER = LoggerFactory.getLogger(TitanDAOImpl.class);

    /**
     * 新增一个用户
     *
     * @param userName
     * @return
     */
    public String addUser(String userName) {
        String vid = null;
        try {
            vid = getTitanGraph().addVertex(T.label, "person", "username", userName).id().toString();
            esDAO.create(new UsernameVID(userName, vid));
            getTitanGraph().tx().commit();
            LOGGER.debug("添加新用户:" + userName);
        } catch (IOException e) {
            LOGGER.error("往ES中插入doc失败: username '" + userName + "'", e);
        } catch (Exception e) {
            LOGGER.error("用户'" + userName + "'已经存在,插入失败", e);
            getTitanGraph().tx().rollback();
        }
        return vid;
    }

    /**
     * 通过顶点id，增加一个好友关系，给Spark批量处理用
     *
     * @param userVID
     * @param friendVID
     * @param autoCommit true表示自动提交事物。false表示手动提交事务，适用于批量提交时。
     */
    public void addRelationByVID(String userVID, String friendVID, Boolean autoCommit) throws Exception {
        GraphTraversalSource g = getGraphTraversal();
        try {
            g.V(userVID).next().addEdge("knows", g.V(friendVID).next());
            if (autoCommit) g.tx().commit();
            LOGGER.debug("添加好友关系:" + userVID + " -knows-> " + friendVID);
        } catch (SchemaViolationException e) {
            LOGGER.warn("已经存在这条边:" + userVID + " -knows-> " + friendVID, e);
        } catch (Exception e) {
            LOGGER.error("addRelationByVID出现异常", e);
            throw e;
        }
    }

    /**
     * 增加一个好友关系
     *
     * @param username
     * @param friendUsername
     * @param autoCommit     true表示自动提交事物。false表示手动提交事务，适用于批量提交时。
     */
    public void addRelation(String username, String friendUsername, Boolean autoCommit) {
        String userVID = esDAO.getVertexId(username);
        String friendVID = esDAO.getVertexId(friendUsername);
        if (userVID == null) {
            userVID = addUser(username);
        }
        if (friendVID == null) {
            friendVID = addUser(friendUsername);
        }
        try {
            addRelationByVID(userVID, friendVID, autoCommit);
        } catch (Exception e) {
            //主要为了防止 Titan的顶点已经删除，而ES的doc未删除的特殊情况
            if (!getGraphTraversal().V().has("username", username).hasNext()) {
                userVID = addUser(username);
            }
            if (!getGraphTraversal().V().has("username", friendUsername).hasNext()) {
                friendVID = addUser(username);
            }
            try{
                addRelationByVID(userVID, friendVID, autoCommit);
            } catch (Exception e2) {

            }
        }

    }

    public void addRelation(String username, String friendUsername) {
        addRelation(username, friendUsername, true);
    }

    /**
     * 删除一个好友关系
     *
     * @param username
     * @param friendUsername
     */
    public void deleteRelation(String username, String friendUsername) {
        try {
            GraphTraversalSource g = getGraphTraversal();
            g.V(esDAO.getVertexId(username)).outE() //.hasLabel("knows")
                    .where(__.otherV().values("username").is(friendUsername)).drop().iterate();
            g.tx().commit();
        } catch (FastNoSuchElementException e) {
            LOGGER.warn("deleteRelation 失败,username:" + username + ",找不到相应的 vertex id。", e);
        } catch (Exception e) {

        }
    }

    public Set getAllFriendVIDs(String userVID) {
        GraphTraversal oneDegreeFriends = getGraphTraversal().V(userVID).out("knows").id();
        return oneDegreeFriends.toSet();
    }

    private List<String> getOneDegreeFriends(String userVID, String[] friends) {
        GraphTraversal oneDegreeFriends = getGraphTraversal().V(userVID).out("knows").
                where(__.values("username").is(P.within(friends))).values("username");
        return oneDegreeFriends.toList();
    }


    private List<String> getTwoDegreeFriends(String userVID, String[] friends) {
        GraphTraversal twoDegreeFriends = getGraphTraversal().V(userVID).aggregate("u").out("knows").
                aggregate("f1").out("knows").
                where(__.values("username").is(P.within(friends))).
                where(P.without("f1")).
                where(P.without("u")).
                dedup().values("username");
        return twoDegreeFriends.toList();
    }

    /**
     * 获取二度好友
     * //TODO 如果查询效率低，尝试将friends的VID也查出来
     *
     * @param username
     * @param friends
     * @return
     */
    public Map<String, Integer> getFriendsLevel(String username, String[] friends) {
        Map<String, Integer> result = new HashMap<String, Integer>();
        String userVID = esDAO.getVertexId(username);
        if (userVID != null) {
            List<String> oneDegreeFriends = getOneDegreeFriends(userVID, friends);
            List<String> twoDegreeFriends = getTwoDegreeFriends(userVID, friends);
            for (String friend : twoDegreeFriends) {
                result.put(friend, 2);
            }
            for (String friend : oneDegreeFriends) {
                result.put(friend, 1);
            }
        }
        return result;
    }

    /**
     * 获取共同好友数
     *
     * @param username
     * @param friends
     * @return
     */
    public Map<String, Integer> getComFriendsNum(String username, String[] friends) {
        String userVID = esDAO.getVertexId(username);
        if (userVID == null) {
            return new HashMap<String, Integer>();
        } else {
            List usernameList = getGraphTraversal().V(userVID).
                    out("knows").out("knows").
                    where(__.values("username").is(P.within(friends))).
                    values("username").toList();
            return count(usernameList);
        }

    }

    private Map<String, Integer> count(List<Object> usernameList) {
        Map<String, Integer> hashMap = new HashMap<String, Integer>();
        Integer c;
        for (Object name : usernameList) {
            c = hashMap.get(name.toString());
            if (c == null) {
                hashMap.put(name.toString(), 1);
            } else {
                hashMap.put(name.toString(), c + 1);
            }
        }
        return hashMap;
    }

    public static void main(String[] args) {
        ITitanDAO d = new TitanDAOImpl();
        d.addRelation("lx11", "slh11");
        d.closeTitanGraph();
    }

    /*
    public void dropUser(String userName) {
        GraphTraversalSource g = getTitanGraph().traversal();
        try {
            g.V().has("username" , userName).drop().iterate();
            g.tx().commit();
            // ES中删除索引
            LOGGER.info("删除用户：" + userName);
        } catch (Exception e) {
            LOGGER.error("person" + userName + "删除失败" , e);
        }
    }*/

}
