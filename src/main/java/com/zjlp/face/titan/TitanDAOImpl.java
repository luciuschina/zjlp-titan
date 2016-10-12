package com.zjlp.face.titan;

import com.thinkaurelius.titan.core.TitanGraph;
import com.zjlp.face.bean.Relation;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.T;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by root on 10/3/16.
 */
public class TitanDAOImpl extends TitanCon implements TitanDAO {

    private static final Logger LOGGER = LoggerFactory.getLogger(TitanDAOImpl.class);

    public void addUser(String userName) {
        try {
            addUserGremlin(userName);
            getTitanGraph().tx().commit();
            LOGGER.debug("插入新用户:" + userName);
        } catch (Exception e) {
            LOGGER.error("用户'" + userName + "'已经存在,插入失败", e);
            getTitanGraph().tx().rollback();
        }
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

    public void addRelation(Relation relation) {
        GraphTraversalSource g = getGraphTraversal();
        try {
            addRelationGremlin(relation, g);
            g.tx().commit();
            LOGGER.info("add " + relation.getUserName() + " -knows-> " + relation.getFriendName());
        } catch (Exception e) {
            LOGGER.info("插入失败", e);
            e.printStackTrace();
            g.tx().rollback();
        }
    }

    public void addRelations(Relation[] relations, int patchLength) {
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
                    addRelationGremlin(relations[j], g);
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

    public void addRelations(Relation[] relations) {
        addRelations(relations, 500);
    }

    private void addRelationGremlin(Relation relation, GraphTraversalSource g) {
        g.V().has("username", relation.getUserName()).next().
                addEdge("knows", g.V().has("username", relation.getFriendName()).next());
    }

    private void addUserGremlin(String username) {
        getTitanGraph().addVertex(T.label, "person", "username", username);
    }

    private void addAndCommitRelationsOneByOne(Relation[] relations, int beginIndex, int endIndex, GraphTraversalSource g) {
        for (int j = beginIndex; j < endIndex; j++) {
            try {
                addRelationGremlin(relations[j], g);
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
        int length = userNames.length;
        GraphTraversalSource g = getTitanGraph().traversal();
        for(int i=0; i<length; i++) {
            g.V().has("username", userNames[i]).drop().iterate();
        }
        g.tx().commit();
    }

    public void dropRelation(Relation relation) {
        GraphTraversalSource g = getGraphTraversal();
       g.V().has("username",relation.getUserName()).bothE().hasLabel("knows")
                .where(__.otherV().values("username").is(relation.getFriendName())).drop().iterate();
       g.tx().commit();
    }

    public void dropRelations(Relation[] relations) {
        int length = relations.length;
        GraphTraversalSource g = getGraphTraversal();
        for(int i=0; i<length; i++) {
            g.V().has("username",relations[i].getUserName()).bothE().hasLabel("knows")
                    .where(__.otherV().values("username").is(relations[i].getFriendName())).drop().iterate();
        }
        g.tx().commit();
    }

    public static void main(String[] args) {
        TitanDAOImpl dao = new TitanDAOImpl();
        Relation[] rels = new Relation[2];
        rels[0] = new Relation("lucius", "sj");
        rels[1] = new Relation("lucius", "w2s");
        dao.dropRelations(rels);
        
        dao.closeTitanGraph();
    }
}
