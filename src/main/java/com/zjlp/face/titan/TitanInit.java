package com.zjlp.face.titan;

import com.thinkaurelius.titan.core.Cardinality;
import com.thinkaurelius.titan.core.EdgeLabel;
import com.thinkaurelius.titan.core.Multiplicity;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.schema.TitanManagement;
import com.thinkaurelius.titan.core.util.TitanCleanup;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by root on 10/11/16.
 */
public class TitanInit extends TitanCon {

    private static final Logger LOGGER = LoggerFactory.getLogger(TitanInit.class);

    /**
     * 为username这个顶点的属性建立唯一索引
     */
    public void createIndex() {
        TitanManagement mgmt = getTitanGraph().openManagement();
        mgmt.buildIndex("usernameIndex", Vertex.class).
                addKey(mgmt.getPropertyKey("username")).
                indexOnly(mgmt.getVertexLabel("person")).
                unique().buildCompositeIndex();
        mgmt.commit();
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

    public static void main(String[] args) {
        TitanInit ti = new TitanInit();
        ti.cleanTitanGraph();
        ti.createVertexLabel();
        ti.createEdgeLabel();
        ti.createIndex();
        ti.closeTitanGraph();
    }
}
