package com.zjlp.face.titan;

import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

/**
 * Created by root on 10/11/16.
 */
public class TitanCon {
    private TitanGraph graph;

    public void closeTitanGraph() {
        if (graph != null && graph.isOpen())
            graph.close();
    }

    public TitanGraph getTitanGraph() {
        if (graph == null || graph.isClosed())
            graph = TitanFactory.open("/data/work/luciuschina/zjlp-titan/src/main/resources/titan-cassandra.properties");
        return graph;
    }

    public GraphTraversalSource getGraphTraversal() {
        return getTitanGraph().traversal();
    }
}
