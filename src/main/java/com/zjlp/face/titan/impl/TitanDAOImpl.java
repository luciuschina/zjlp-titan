package com.zjlp.face.titan.impl;

import com.thinkaurelius.titan.core.SchemaViolationException;
import com.thinkaurelius.titan.core.TitanGraph;
import com.zjlp.face.bean.UsernameVID;
import com.zjlp.face.titan.IEsDAO;
import com.zjlp.face.titan.ITitanDAO;
import com.zjlp.face.titan.TitanConPool;
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
import java.util.*;

@Service("TitanDAOImpl")
public class TitanDAOImpl extends TitanConPool implements ITitanDAO, Serializable {
    private static IEsDAO esDAO = new EsDAOImpl();

    private static final Logger LOGGER = LoggerFactory.getLogger(TitanDAOImpl.class);

    public void cacheFor(String username) {
        if (esDAO.ifCache(username)) {
            try {
                LOGGER.info("对"+username+"作提前缓存");
                getComFriendsNum(username, new String[]{username});
            } catch (Exception e) {
                LOGGER.error("cacheFor exception", e);
            }
        }
    }

    /**
     * 新增一个用户
     *
     * @param userName
     * @return
     */
    public String addUser(String userName, TitanGraph graph) {
        String vid = null;
        try {
            vid = graph.addVertex(T.label, "person", "username", userName).id().toString();
            esDAO.create(new UsernameVID(userName, vid));
            graph.tx().commit();
            LOGGER.debug("添加新用户:" + userName);
        } catch (IOException e) {
            LOGGER.error("往ES中插入doc失败: username '" + userName + "'", e);
        } catch (Exception e) {
            LOGGER.error("用户'" + userName + "'已经存在,插入失败", e);
            graph.tx().rollback();
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
    public void addRelationByVID(String userVID, String friendVID, Boolean autoCommit, TitanGraph graph) throws Exception {
        GraphTraversalSource g = graph.traversal();
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
        TitanGraph graph = getTitanGraph(username);
        if (userVID == null) {
            userVID = addUser(username, graph);
        }
        if (friendVID == null) {
            friendVID = addUser(friendUsername, graph);
        }
        try {
            addRelationByVID(userVID, friendVID, autoCommit, graph);
        } catch (Exception e) {

            //主要为了防止 Titan的顶点已经删除，而ES的doc未删除的特殊情况
            if (!graph.traversal().V().has("username", username).hasNext()) {
                userVID = addUser(username, graph);
            }
            if (!graph.traversal().V().has("username", friendUsername).hasNext()) {
                friendVID = addUser(username, graph);
            }
            try {
                addRelationByVID(userVID, friendVID, autoCommit, graph);
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
            GraphTraversalSource g = getTitanGraph(username).traversal();
            g.V(esDAO.getVertexId(username)).outE() //.hasLabel("knows")
                    .where(__.otherV().values("username").is(friendUsername)).drop().iterate();
            g.tx().commit();
        } catch (FastNoSuchElementException e) {
            LOGGER.warn("deleteRelation 失败,username:" + username + ",找不到相应的 vertex id。", e);
        } catch (Exception e) {

        }
    }

    public Set getAllFriendVIDs(String userVID) {
        GraphTraversal oneDegreeFriends = getTitanGraph().traversal().V(userVID).out("knows").id();
        return oneDegreeFriends.toSet();
    }

    private List<String> getOneDegreeFriends(String userVID, String[] friendsVID, TitanGraph graph) {
        GraphTraversal oneDegreeFriends = graph.traversal().V(userVID).out("knows").
                where(__.hasId(friendsVID)).values("username");
        return oneDegreeFriends.toList();
    }


    private List<String> getTwoDegreeFriends(String userVID, String[] friendsVID, TitanGraph graph) {
        GraphTraversal twoDegreeFriends = graph.traversal().V(userVID).aggregate("u").out("knows").
                aggregate("f1").out("knows").
                where(__.hasId(friendsVID)).
                where(P.without("f1")).
                where(P.without("u")).
                dedup().values("username");
        return twoDegreeFriends.toList();
    }

    /**
     * 获取二度好友
     *
     * @param username
     * @param friends
     * @return
     */
    public Map<String, Integer> getFriendsLevel(String username, String[] friends) {
        Map<String, Integer> result = new HashMap<String, Integer>();
        String userVID = esDAO.getVertexId(username);
        String[] friendsVID = esDAO.getVertexIds(friends);
        TitanGraph graph = getTitanGraph(username);
        if (userVID != null && friendsVID.length > 0) {
            List<String> oneDegreeFriends = getOneDegreeFriends(userVID, friendsVID, graph);
            List<String> twoDegreeFriends = getTwoDegreeFriends(userVID, friendsVID, graph);
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
    public Map<Object, Long> getComFriendsNum(String username, String[] friends) {
        String userVID = esDAO.getVertexId(username);
        String[] vids = esDAO.getVertexIds(friends);
        if (userVID == null || vids.length == 0) {
            LOGGER.info("不存在username:" + username + "顶点");
            return new HashMap<Object, Long>();
        } else {
            return getTitanGraph(username).traversal().V(userVID)
                    .aggregate("u")
                    .out("knows").out("knows")
                    .where(__.hasId(vids))
                    .where(P.without("u"))
                    .values("username").groupCount().next();
        }
    }


    public static void main(String[] args) {

       /* ITitanDAO d = new TitanDAOImpl();
        d.addRelation("111","222");
        d.addRelation("111", "222");*/
        List<String> list = new ArrayList();
        list.add("111111");
        list.add("22221111");
        String [] aa = list.toArray(new String [list.size()]);

        ITitanDAO d = new TitanDAOImpl();
        d.getFriendsLevel("111",aa);

        d.closeTitanGraph();
    }

}
