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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service("TitanDAOImpl")
public class TitanDAOImpl extends TitanCon implements ITitanDAO {
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
            vid = addUserGremlin(userName);
            getTitanGraph().tx().commit();
            esDAO.create(new UsernameVID(userName, vid));
            LOGGER.debug("插入新用户:" + userName);
        } catch (Exception e) {
            LOGGER.error("用户'" + userName + "'已经存在,插入失败" , e);
            getTitanGraph().tx().rollback();
        }
        return vid;
    }

    /**
     * 增加多个用户
     *
     * @param userNames
     */
    public void addUsers(List<String> userNames) {
        for (String userName : userNames) {
            addUser(userName);
        }
    }

    /**
     * 通过顶点id，增加一个好友关系，给Spark批量处理用
     *
     * @param userVID
     * @param friendVID
     * @param autoCommit true表示自动提交事物。false表示手动提交事务，适用于批量提交时。
     */
    public void addRelationByVID(String userVID, String friendVID, Boolean autoCommit) {
        GraphTraversalSource g = getGraphTraversal();
        try {
            g.V(userVID).next().addEdge("knows" , g.V(friendVID).next());
            if (autoCommit) g.tx().commit();
        } catch (FastNoSuchElementException e) {
            LOGGER.warn("没有这个vertex id:" + userVID + " or " + friendVID, e);
        } catch (SchemaViolationException e) {
            LOGGER.warn("已经存在这条边:" + userVID + " -knows-> " + friendVID, e);
        } catch (Exception e) {
            LOGGER.error("addRelationByVID出现异常" , e);
        }
    }

    private String addUserGremlin(String username) {
        return getTitanGraph().addVertex(T.label, "person" , "username" , username).id().toString();
    }

    /**
     * 增加一个好友关系
     *
     * @param username
     * @param friendUsername
     * @param autoCommit     true表示自动提交事物。false表示手动提交事务，适用于批量提交时。
     */
    public void addRelation(String username, String friendUsername, Boolean autoCommit) {
        GraphTraversalSource g = getGraphTraversal();
        try {
            g.V(esDAO.getVertexId(username)).next().addEdge("knows" , g.V(esDAO.getVertexId(friendUsername)).next());
            if (autoCommit) g.tx().commit();
        } catch (FastNoSuchElementException e) {
            LOGGER.warn("addRelation 失败,username:" + username + " or friendUsername:" + friendUsername + "找不到相应的 vertex id。" , e);
        } catch (SchemaViolationException e) {
            LOGGER.warn("已经存在这条边:" + username + " -knows-> " + friendUsername, e);
        } catch (Exception e) {
            LOGGER.error("addRelationByVID出现异常" , e);
        }
    }

/*    public void dropUser(String userName) {
        GraphTraversalSource g = getTitanGraph().traversal();
        try {
            g.V().has("username" , userName).drop().iterate();
            g.tx().commit();
            //TODO ES中删除索引
            LOGGER.info("删除用户：" + userName);
        } catch (Exception e) {
            LOGGER.error("person" + userName + "删除失败" , e);
        }
    }*/

    /**
     * 删除一个好友关系
     *
     * @param username
     * @param friendUsername
     */
    public void deleteRelation(String username, String friendUsername) {
        try {
            GraphTraversalSource g = getGraphTraversal();
            g.V(esDAO.getVertexId(username)).outE().hasLabel("knows")
                    .where(__.otherV().values("username").is(friendUsername)).drop().iterate();
            g.tx().commit();
        } catch (FastNoSuchElementException e) {
            LOGGER.warn("deleteRelation 失败,username:" + username + ",找不到相应的 vertex id。" , e);
        } catch (Exception e) {

        }
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
        String userVID = esDAO.getVertexId(username);
        List<String> oneDegreeFriends = getOneDegreeFriends(userVID, friends);
        List<String> twoDegreeFriends = getTwoDegreeFriends(userVID, friends);
        Map<String, Integer> result = new HashMap<String, Integer>();

        for (String friend : twoDegreeFriends) {
            result.put(friend, 2);
        }
        for (String friend : oneDegreeFriends) {
            result.put(friend, 1);
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
        List usernameList = getGraphTraversal().V(esDAO.getVertexId(username)).
                out("knows").out("knows").
                where(__.values("username").is(P.within(friends))).
                values("username").toList();
        return count(usernameList);
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

}
