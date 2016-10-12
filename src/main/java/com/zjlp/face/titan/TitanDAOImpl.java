package com.zjlp.face.titan;

import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.schema.TitanManagement;
import com.zjlp.face.bean.Relation;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.T;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Created by root on 10/3/16.
 */
public class TitanDAOImpl extends TitanCon {

    private static final Logger LOGGER = LoggerFactory.getLogger(TitanDAOImpl.class);


    public void addRelation(Relation relation) {
        GraphTraversalSource g = getGraphTraversal();
        try {
            addRelationGremlin(relation, g);
            g.tx().commit();
            LOGGER.info("add " + relation.getUserName() + " -knows-> " + relation.getFriendName());
        } catch (Exception e) {
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
                    LOGGER.info("add " + relations[j].getUserName() + " -knows-> " + relations[j].getFriendName());

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

    private void addAndCommitRelationsOneByOne(Relation[] relations, int beginIndex, int endIndex, GraphTraversalSource g) {
        for (int j = beginIndex; j < endIndex; j++) {
            try {
                addRelationGremlin(relations[j], g);
                g.tx().commit();
            } catch (Exception e1) {
                e1.printStackTrace();
                LOGGER.info("Edge插入失败:已经存在 " + relations[j].getUserName() + " -knows-> " + relations[j].getFriendName());
                g.tx().rollback();
            }
        }
    }


    public void deleteUsername(String userName) {

    }

    public void deleteUsernames(List<String> userNames) {

    }

    public void deleteRelation(Relation relation) {

    }

    public void deleteRelations(List<Relation> relations) {

    }

    public static void main(String[] args) {
        TitanDAOImpl dao = new TitanDAOImpl();


        TitanGraph graph = dao.getTitanGraph();
        TitanManagement mgmt = graph.openManagement();

       for(int i = 1 ; i<100; i++) {
            graph.addVertex(T.label,"person", "username", i+"");
        }
        graph.tx().commit();

        Relation[] ary = new Relation[90];
        for (int i = 1; i < 91; i++) {
            ary[i - 1] = new Relation("99", i + "");
        }


        dao.addRelations(ary, 10);
        dao.closeTitanGraph();

       /* TitanManagement mgmt = graph.openManagement();
        mgmt.makePropertyKey("username").dataType(String.class).make();
        //mgmt.commit();

        PropertyKey name1 = mgmt.getPropertyKey("username");

        mgmt.buildIndex("usernameIndex", Vertex.class).addKey(name1).unique().buildCompositeIndex();

        mgmt.commit();*/

/*        try{
            graph.addVertex(T.label,"person", "username", "lucius");
            graph.tx().commit();
        }catch (Exception e) {
            graph.tx().rollback();
        }
        try{
            graph.addVertex(T.label,"person", "username", "lucius1");
            graph.tx().commit();
        }catch (Exception e) {
            graph.tx().rollback();
        }
        try{
            graph.addVertex(T.label,"person", "username", "lucius");
            graph.tx().commit();
        }catch (Exception e) {
            graph.tx().rollback();
        }*/

 /*       Vertex lucius = graph.addVertex(T.label, "person", "username", "lucius");
        Vertex sj = graph.addVertex(T.label, "person", "username", "sj");
        lucius.addEdge("knows", sj);
        graph.tx().commit();
        sj.addEdge("knows",lucius);
        graph.tx().commit();

        graph.*/

        //graph.addVertex("username", "lucius");

        // see org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory.generateClassic()
    /*     graph.addVertex("username", "lucius");
        graph.addVertex("username", "lucius");*/

        /*final Vertex vadas = graph.addVertex("name", "vadas", "age", 27);
        final Vertex lop = graph.addVertex("name", "lop", "lang", "java");
        final Vertex josh = graph.addVertex("name", "josh", "age", 32);
        final Vertex ripple = graph.addVertex("name", "ripple", "lang", "java");
        final Vertex peter = graph.addVertex("name", "peter", "age", 35);
        marko.addEdge("knows", vadas, "weight", 0.5f);
        marko.addEdge("knows", josh, "weight", 1.0f);
        marko.addEdge("created", lop, "weight", 0.4f);
        josh.addEdge("created", ripple, "weight", 1.0f);
        josh.addEdge("created", lop, "weight", 0.4f);
        peter.addEdge("created", lop, "weight", 0.2f);

        GraphTraversalSource g = graph.traversal();

        Vertex fromNode = g.V().has("name", "marko").next();
        Vertex toNode = g.V().has("name", "peter").next();
        ArrayList list = new ArrayList();
         g.V(fromNode).repeat(both().simplePath()).until(is(toNode)).limit(1).path().fill(list);
        LOGGER.info(list.toString());*/
        //graph.close();
    }
}
