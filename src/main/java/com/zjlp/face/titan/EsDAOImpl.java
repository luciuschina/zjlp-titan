package com.zjlp.face.titan;

import com.zjlp.face.bean.UsernameVID;
import com.zjlp.face.spark.base.Props;
import com.zjlp.face.spark.utils.EsUtils;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;


public class EsDAOImpl {
    private static final Logger LOGGER = LoggerFactory.getLogger(EsDAOImpl.class);
    private Client esClient = null;

    public Client getEsClient() {
        if (esClient == null) {
            esClient = EsUtils.getEsClient(Props.get("es.cluster.name"), Props.get("es.nodes"), Integer.valueOf(Props.get("es.client.port")));
        }
        return esClient;
    }

    public void multiCreate(List<UsernameVID> items) {
        Client client = getEsClient();
        BulkRequestBuilder bulkRequest = client.prepareBulk();
        for (UsernameVID item:items) {
            try {
                bulkRequest.add(client.prepareIndex("titan-es", "rel", item.getUserName())
                        .setSource(jsonBuilder()
                                .startObject()
                                .field("vertexId", item.getVid())
                                .endObject()));
            } catch (Exception e) {
                LOGGER.error("ES插入索引失败.username:" + item.getUserName() + ",vertexId:" + item.getVid(), e);
            }
        }
        bulkRequest.get();
    }

    public String getVertexId(String username) {
        SearchResponse response = getEsClient().prepareSearch("titan-es").setTypes("rel")
                .setQuery(QueryBuilders.idsQuery().ids(username))
                .setExplain(false).execute().actionGet();

        SearchHit[] results = response.getHits().getHits();
        if (results != null && results.length > 0)
            return results[0].getSource().get("vertexId").toString();
        else {
            LOGGER.warn("return null!. ES的titan-es这个索引中没有这个id:"+username);
            return null;
        }

    }

    public void closeClient() {
        esClient.close();
    }

    public static void main(String[] args) {
        EsDAOImpl dao = new EsDAOImpl();


        long b1 = System.currentTimeMillis();
        System.out.println(dao.getVertexId("123dddd"));
        System.out.println((System.currentTimeMillis()-b1) + "ms");
        long b2 = System.currentTimeMillis();
        System.out.println(dao.getVertexId("1236"));
        System.out.println((System.currentTimeMillis()-b2) + "ms");
        System.out.println(dao.getVertexId("123"));
        System.out.println(dao.getVertexId("123dddd"));
        System.out.println(dao.getVertexId("123dddd"));

    }
}
