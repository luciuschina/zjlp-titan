package com.zjlp.face.titan;

import com.thinkaurelius.titan.core.Cardinality;
import com.thinkaurelius.titan.core.Multiplicity;
import com.thinkaurelius.titan.core.PropertyKey;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.schema.SchemaAction;
import com.thinkaurelius.titan.core.schema.TitanManagement;
import com.thinkaurelius.titan.core.util.TitanCleanup;
import com.thinkaurelius.titan.graphdb.database.management.ManagementSystem;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;

public class TitanInit extends TitanCon {

    private static final Logger LOGGER = LoggerFactory.getLogger(TitanInit.class);

    /**
     * 为username这个顶点的属性建立索引. 为了保证username的唯一性.
     * 如果不建立索引，也能保证username属性的唯一性，那么可以不建立索引。
     */
    public void createIndex() {
        TitanManagement mgmt = getTitanGraph().openManagement();
        mgmt.buildIndex("usernameIndex" , Vertex.class).
                addKey(mgmt.getPropertyKey("username")).
                unique().buildCompositeIndex();
        mgmt.commit();
        LOGGER.info("为username创建索引");
    }

    /**
     * 清空图,删除所有的顶点、边和索引
     */
    public void cleanTitanGraph() {
        TitanGraph graph = getTitanGraph();
        graph.close();
        TitanCleanup.clear(graph);
        LOGGER.info("清空tatin中的所有数据");
    }

    public void createVertexLabel() {
        TitanManagement mgmt = getTitanGraph().openManagement();
        mgmt.makePropertyKey("username").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        mgmt.makeVertexLabel("person").make();
        mgmt.commit();
    }

    public void createEdgeLabel() {
        TitanManagement mgmt = getTitanGraph().openManagement();
        mgmt.makeEdgeLabel("knows").multiplicity(Multiplicity.SIMPLE).make();
        mgmt.commit();
    }

    /**
     * 删除ES索引
     */
    public void dropTitanEsIndex() {
        //TODO 后面实现
    }

    /**
     * 创建ES索引
     */
    public void createTitanEsIndex() {
        //TODO 后面实现
    }

    public static void main(String[] args) {
        new TitanInit().run();
    }

    public void usernameUnique() {
        TitanGraph graph = getTitanGraph();
        graph.tx().rollback();  //Never create new indexes while a transaction is active
        TitanManagement mgmt = graph.openManagement();
        PropertyKey username = mgmt.getPropertyKey("username");
        mgmt.buildIndex("usernameUnique", Vertex.class).addKey(username).unique().buildCompositeIndex();
        mgmt.commit();
        //Wait for the index to become available
        try {
            ManagementSystem.awaitGraphIndexStatus(graph, "usernameUnique").call();
        } catch (InterruptedException e) {
            LOGGER.error("InterruptedException",e);
        }
        //Reindex the existing data
        mgmt = graph.openManagement();
        try {
            mgmt.updateIndex(mgmt.getGraphIndex("usernameUnique"), SchemaAction.REINDEX).get();
        } catch (InterruptedException e) {
            LOGGER.error("InterruptedException" , e);
        } catch (ExecutionException e) {
            LOGGER.error("ExecutionException" , e);
        }
        mgmt.commit();
    }

    public void run(){
        cleanTitanGraph();
        createVertexLabel();
        createEdgeLabel();
        closeTitanGraph();
    }
}
