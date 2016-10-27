package com.zjlp.face.titan;

import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.zjlp.face.spark.base.Props;

public class TitanConPool {
    private int poolSize = Integer.valueOf(Props.get("titan-con-pool-size"));
    private TitanGraph[] graphs = new TitanGraph[poolSize];

    public void closeTitanGraph() {
        for (TitanGraph graph : graphs) {
            if (graph != null && graph.isOpen())
                graph.close();
        }
    }

    public TitanGraph getTitanGraph(String username) {
        return getTitanGraph(Math.abs(username.hashCode()));
    }

    public TitanGraph getTitanGraph() {
        return getTitanGraph((int) System.currentTimeMillis());
    }

    public TitanGraph getTitanGraph(int j) {
        int i = j % poolSize;
        if (graphs[i] == null || graphs[i].isClosed()) {
            graphs[i] = TitanFactory.open(Props.get("titan-cassandra"));
        }
        return graphs[i];
    }

}
