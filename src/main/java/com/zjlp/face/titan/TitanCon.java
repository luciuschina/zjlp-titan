package com.zjlp.face.titan;

import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.zjlp.face.spark.base.Props;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

public class TitanCon {
    public TitanGraph graph;

    public void closeTitanGraph() {
        if (graph != null) {
            graph.close();
        }

    }

    public TitanGraph getTitanGraph() {
        if(graph == null || graph.isClosed()) {
            graph = TitanFactory.open(Props.get("titan-cassandra"));
        }
        return graph;
    }

    public GraphTraversalSource getGraphTraversal() {
        return getTitanGraph().traversal();
    }
}
