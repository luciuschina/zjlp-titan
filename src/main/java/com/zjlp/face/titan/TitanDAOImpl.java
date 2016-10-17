package com.zjlp.face.titan;

import com.thinkaurelius.titan.core.SchemaViolationException;
import com.thinkaurelius.titan.core.TitanVertex;
import com.zjlp.face.bean.Relation;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TitanDAOImpl extends TitanCon implements TitanDAO {

    private static final Logger LOGGER = LoggerFactory.getLogger(TitanDAOImpl.class);

    public String addUser(String userName, Boolean autoCommit) {
        String vid = null;
        try {
            vid = addUserGremlin(userName);
            if (autoCommit) getTitanGraph().tx().commit();
            LOGGER.debug("插入新用户:" + userName);
        } catch (Exception e) {
            LOGGER.error("用户'" + userName + "'已经存在,插入失败", e);
            getTitanGraph().tx().rollback();
        }
        return vid;
    }

    public String addUser(String userName) {
        return addUser(userName, true);
    }

    public void addUsers(String[] userNames, int patchLength) {
        GraphTraversalSource g = getGraphTraversal();
        int len = userNames.length;
        LOGGER.info("len:" + len);
        int patchNum = len % patchLength == 0 ? (len / patchLength) : (len / patchLength + 1);
        for (int i = 0; i < patchNum; i++) {
            LOGGER.info("增加第" + (i + 1) + "/" + patchNum + "批，每批" + patchLength + "条...");
            int curPatchEnd = patchLength * (i + 1) < len ? patchLength * (i + 1) : len;
            try {
                for (int j = patchLength * i; j < curPatchEnd; j++) {
                    addUserGremlin(userNames[j]);
                    LOGGER.debug("add person:" + userNames[j]);
                }
                g.tx().commit();
            } catch (Exception e) {
                g.tx().rollback();
                //该批次由于插入失败,所以重新插入。改成每插入一条就提交一次
                addAndCommitUserOneByOne(userNames, patchLength * i, curPatchEnd);
            }
        }
    }

    public void addUsers(String[] userNames) {
        addUsers(userNames, 500);
    }

    public void addRelationByVID(String userId, String friendId, GraphTraversalSource g, Boolean autoCommit) {
        try {
            //System.out.println("增加" + userId + "->" + friendId);
            g.V(userId).next().addEdge("knows", g.V(friendId).next());
            if(autoCommit) g.tx().commit();
        } catch (FastNoSuchElementException e) {
            LOGGER.warn("没有这个vertex id:" + userId + " or " + friendId, e);
        } catch (SchemaViolationException e) {
            LOGGER.warn("已经存在这条边:" + userId + " -knows-> " + friendId, e);
        } catch (Exception e) {
            LOGGER.error("addRelationByVID出现异常",e);
        }


    }

    public void addRelationByVID(String userId, String friendId) {
        addRelationByVID(userId, friendId, true);
    }

    public void addRelationByVID(String userId, String friendId, Boolean autoCommit) {
        addRelationByVID(userId, friendId, getGraphTraversal(), autoCommit);
    }

    public void addRelation(Relation relation) {
        GraphTraversalSource g = getGraphTraversal();
        try {
            addRelationByUsername(relation, g);
            g.tx().commit();
            LOGGER.info("add " + relation.getUserName() + " -knows-> " + relation.getFriendName());
        } catch (Exception e) {
            LOGGER.info("插入失败", e);
            e.printStackTrace();
            g.tx().rollback();
        }
    }

    public void addRelationsByUsername(Relation[] relations, int patchLength) {
        GraphTraversalSource g = getGraphTraversal();
        int len = relations.length;
        LOGGER.info("len:" + len);
        int patchNum = len % patchLength == 0 ? (len / patchLength) : (len / patchLength + 1);
        for (int i = 0; i < patchNum; i++) {
            LOGGER.info("增加第" + (i + 1) + "/" + patchNum + "批Relations，每批" + patchLength + "条...");
            int curPatchEnd = patchLength * (i + 1) < len ? patchLength * (i + 1) : len;
            try {
                LOGGER.info("curPatchEnd:" + curPatchEnd);
                for (int j = patchLength * i; j < curPatchEnd; j++) {
                    addRelationByUsername(relations[j], g);
                    LOGGER.debug("add " + relations[j].getUserName() + " -knows-> " + relations[j].getFriendName());
                }
                g.tx().commit();
            } catch (Exception e) {
                g.tx().rollback();
                //该批次由于插入失败,所以重新插入。改成每插入一条就提交一次
                addAndCommitRelationsOneByOne(relations, patchLength * i, curPatchEnd, g);
            }
        }
    }

    public void addRelationsByUsername(Relation[] relations) {
        addRelationsByUsername(relations, 500);
    }

    public void addRelationsByUsername(String username, List<String> friendsList) {
        GraphTraversalSource g = getGraphTraversal();
        Vertex user = g.V().has("username", username).next();
        List<Vertex> friends = g.V().where(__.values("username").is(P.within(friendsList))).toList();
        for (Vertex friend : friends) {
            try {
                user.addEdge("knows", friend);
                g.tx().commit();
            } catch (Exception e) {
                g.tx().rollback();
                LOGGER.info("add failed:" + username + " -knows-> " + friend, e);
            }
        }

    }

    private void addRelationByUsername(Relation relation, GraphTraversalSource g) {
        g.V().has("username", relation.getUserName()).next().
                addEdge("knows", g.V().has("username", relation.getFriendName()).next());
    }

    private String addUserGremlin(String username) {
        return getTitanGraph().addVertex(T.label, "person", "username", username).id().toString();
    }

    private void addAndCommitRelationsOneByOne(Relation[] relations, int beginIndex, int endIndex, GraphTraversalSource g) {
        for (int j = beginIndex; j < endIndex; j++) {
            try {
                addRelationByUsername(relations[j], g);
                g.tx().commit();
            } catch (Exception e1) {
                LOGGER.error("Edge插入失败: " + relations[j].getUserName() + " -knows-> " + relations[j].getFriendName(), e1);
                g.tx().rollback();
            }
        }
    }

    private void addAndCommitUserOneByOne(String[] username, int beginIndex, int endIndex) {
        for (int j = beginIndex; j < endIndex; j++) {
            try {
                addUserGremlin(username[j]);
                getTitanGraph().tx().commit();
            } catch (Exception e1) {
                LOGGER.error("person 插入失败:已经存在 " + username[j]);
                getTitanGraph().tx().rollback();
            }
        }
    }

    public void dropUser(String userName) {
        GraphTraversalSource g = getTitanGraph().traversal();
        try {
            g.V().has("username", userName).drop().iterate();
            g.tx().commit();
            LOGGER.info("删除用户：" + userName);
        } catch (Exception e) {
            LOGGER.error("person" + userName + "删除失败", e);
        }
    }

    public void dropUsers(String[] userNames) {
        GraphTraversalSource g = getTitanGraph().traversal();
        for (String userName : userNames) {
            g.V().has("username", userName).drop().iterate();
        }
        g.tx().commit();
    }

    public void dropRelation(Relation relation) {
        GraphTraversalSource g = getGraphTraversal();
        g.V().has("username", relation.getUserName()).bothE().hasLabel("knows")
                .where(__.otherV().values("username").is(relation.getFriendName())).drop().iterate();
        g.tx().commit();
    }

    public void dropRelations(Relation[] relations) {
        GraphTraversalSource g = getGraphTraversal();
        for (Relation relation : relations) {
            g.V().has("username", relation.getUserName()).bothE().hasLabel("knows")
                    .where(__.otherV().values("username").is(relation.getFriendName())).drop().iterate();
        }
        g.tx().commit();
    }

    public List<String> getOneDegreeFriends(String username, List<String> friends) {
        GraphTraversal oneDegreeFriends = getGraphTraversal().V().
                has("username", username).out("knows").
                where(__.values("username").is(P.within(friends))).values("username");
        return oneDegreeFriends.toList();
    }

    public List<String> getOneDegreeFriends(String username) {
        GraphTraversal oneDegreeFriends = getGraphTraversal().V().
                has("username", username).out("knows").values("username");
        return oneDegreeFriends.toList();
    }


    public List<String> getTwoDegreeFriends(String username) {
        GraphTraversal twoDegreeFriends = getGraphTraversal().V().
                has("username", username).aggregate("u").out("knows").
                aggregate("f1").out("knows").where(P.without("f1")).where(P.without("u")).dedup().values("username");
        return twoDegreeFriends.toList();
    }

    public List<String> getTwoDegreeFriends(String username, List<String> friends) {
        GraphTraversal twoDegreeFriends = getGraphTraversal().V().
                has("username", username).aggregate("u").out("knows").
                aggregate("f1").out("knows").
                where(__.values("username").is(P.within(friends))).
                where(P.without("f1")).
                where(P.without("u")).
                dedup().values("username");
        return twoDegreeFriends.toList();
    }

    public Map<String, Integer> getOneAndTwoDegreeFriends(String username, List<String> friends) {
        List<String> oneDegreeFriends = getOneDegreeFriends(username, friends);
        List<String> twoDegreeFriends = getTwoDegreeFriends(username, friends);
        Map<String, Integer> result = new HashMap<String, Integer>();

        for (String friend : twoDegreeFriends) {
            result.put(friend, 2);
        }
        for (String friend : oneDegreeFriends) {
            result.put(friend, 1);
        }
        return result;
    }

    public Map<String, Integer> getComFriendsNum(String username, List<String> friends) {
        List usernameList = getGraphTraversal().V().has("username", username).
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

    public void testAddGraph() {
        String[] usernames = new String[7];
        usernames[0] = "001";
        usernames[1] = "002";
        usernames[2] = "003";
        usernames[3] = "004";
        usernames[4] = "005";
        usernames[5] = "006";
        usernames[6] = "007";
        addUsers(usernames);

        Relation[] rels = new Relation[18];
        rels[0] = new Relation("001", "002");
        rels[1] = new Relation("002", "001");
        rels[2] = new Relation("001", "003");
        rels[3] = new Relation("003", "001");
        rels[4] = new Relation("001", "004");
        rels[5] = new Relation("004", "001");
        rels[6] = new Relation("001", "007");
        rels[7] = new Relation("007", "001");
        rels[8] = new Relation("002", "003");
        rels[9] = new Relation("002", "004");
        rels[10] = new Relation("003", "002");
        rels[11] = new Relation("004", "002");
        rels[12] = new Relation("003", "007");
        rels[13] = new Relation("007", "003");
        rels[14] = new Relation("003", "005");
        rels[15] = new Relation("005", "003");
        rels[16] = new Relation("005", "006");
        rels[17] = new Relation("006", "005");
        addRelationsByUsername(rels);
    }

    public void testUndirected() {
        String[] usernames = new String[4];
        usernames[0] = "001";
        usernames[1] = "002";
        usernames[2] = "003";
        usernames[3] = "004";


        addUsers(usernames);

        String username = "001";
        List<String> friends = new ArrayList<String>();
        friends.add("002");
        friends.add("003");

        GraphTraversalSource g = getGraphTraversal();
        Vertex user = g.V().has("username", "001").next();

        List<Vertex> friendsVertex = g.V().where(__.values("username").is(P.within(friends))).toList();
        //has("username","002").toList();

        for (Vertex friend : friendsVertex) {
            user.addEdge("knows", friend);
        }

        //addRelationsByUsername(rels);
    }

    public static void main(String[] args) {

        TitanDAOImpl dao = new TitanDAOImpl();

        dao.addRelationByVID("20688", "41156784111");
        //dao.testAddGraph();
/*        TitanVertex tv = dao.getTitanGraph().addVertex(T.label, "person", "username", System.currentTimeMillis());
        System.out.println(tv.id());*/

        dao.closeTitanGraph();


    }
}
