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

import java.time.temporal.ChronoUnit;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class TitanInit extends TitanConPool {

    private static final Logger LOGGER = LoggerFactory.getLogger(TitanInit.class);

    /**
     * 清空图,删除所有的顶点、边和索引
     */
    public void cleanTitanGraph() {
        TitanGraph graph = getTitanGraph();
        graph.close();
        TitanCleanup.clear(graph);
        LOGGER.info("清空tatin中的所有数据");
    }

    public void createVertexLabel(TitanGraph graph) {
        TitanManagement mgmt = graph.openManagement();
        mgmt.makePropertyKey("username").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        mgmt.makeVertexLabel("person").make();
        mgmt.commit();
    }
    public void killOtherTitanInstances(TitanGraph graph) {
        TitanManagement mgmt = graph.openManagement();
        //更改GLOBAL_OFFLINE属性前需要先关闭其他Titan实例
        Iterator<String> it = mgmt.getOpenInstances().iterator();
        while (it.hasNext()) {
            String nxt = it.next();
            if (!nxt.contains("current"))
                mgmt.forceCloseInstance(nxt);
        }
        mgmt.commit();
    }
    public void setGlobalOfflineOption(String key, Object value) {
        TitanGraph graph = getTitanGraph();
        killOtherTitanInstances(graph);
        TitanManagement mgmt = graph.openManagement();
        //设置GLOBAL_OFFLINE属性
        mgmt.set(key, value);
        mgmt.commit();
    }

    public void setDBcacheTime() {
        setGlobalOfflineOption("cache.db-cache-time", 1800000);
    }

    public void createEdgeLabel(TitanGraph graph) {
        TitanManagement mgmt = graph.openManagement();
        mgmt.makeEdgeLabel("knows").multiplicity(Multiplicity.SIMPLE).make();
        mgmt.commit();
    }

    public void usernameUnique(TitanGraph graph) {

        graph.tx().rollback();  //Never create new indexes while a transaction is active
        TitanManagement mgmt = graph.openManagement();
        PropertyKey username = mgmt.getPropertyKey("username");
        mgmt.buildIndex("usernameUnique", Vertex.class).addKey(username).unique().buildCompositeIndex();
        mgmt.commit();
        //Wait for the index to become available
        try {
            ManagementSystem.awaitGraphIndexStatus(graph, "usernameUnique")
                    .timeout(60, ChronoUnit.MINUTES) // set timeout to 60 min
                    .call();
        } catch (InterruptedException e) {
            LOGGER.error("InterruptedException", e);
        }
        //Reindex the existing data
        mgmt = graph.openManagement();
        try {
            mgmt.updateIndex(mgmt.getGraphIndex("usernameUnique"), SchemaAction.REINDEX).get();
        } catch (InterruptedException e) {
            LOGGER.error("InterruptedException", e);
        } catch (ExecutionException e) {
            LOGGER.error("ExecutionException", e);
        }
        mgmt.commit();
    }

    public void run() {

        cleanTitanGraph();
        TitanGraph graph = getTitanGraph();
        createVertexLabel(graph);
        createEdgeLabel(graph);
        closeTitanGraph();
    }

    public static void main(String[] args) {
        new TitanInit().run();
    }
}
