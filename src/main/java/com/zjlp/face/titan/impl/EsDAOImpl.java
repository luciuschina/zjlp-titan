package com.zjlp.face.titan.impl;

import com.zjlp.face.bean.UsernameVID;
import com.zjlp.face.spark.base.Props;
import com.zjlp.face.spark.utils.EsUtils;
import com.zjlp.face.titan.IEsDAO;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

@Service("EsDAOImpl")
public class EsDAOImpl implements IEsDAO {
    private static final Logger LOGGER = LoggerFactory.getLogger(EsDAOImpl.class);
    private Client esClient = null;
    private String titanEsIndex = Props.get("titan-es-index");

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
                bulkRequest.add(client.prepareIndex(titanEsIndex, "rel", item.getUserName())
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

    public void create(UsernameVID item) throws IOException {
        getEsClient().prepareIndex(titanEsIndex, "rel", item.getUserName())
                .setSource(jsonBuilder().startObject().field("vertexId" , item.getVid()).endObject()).get();
    }

    public String getVertexId(String username) {
        SearchResponse response = getEsClient().prepareSearch(titanEsIndex).setTypes("rel")
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

    public String[] getVertexIds(String[] usernames) {
        MultiGetResponse multiGetItemResponses = getEsClient().prepareMultiGet()
                .add(titanEsIndex, "rel", usernames)
                .get();
        List<String> vids = new ArrayList<String>();
        for (MultiGetItemResponse itemResponse : multiGetItemResponses) {
            GetResponse response = itemResponse.getResponse();
            if (response.isExists()) {
                vids.add(response.getSourceAsMap().get("vertexId").toString());
            }
        }
        int listSize = vids.size();
        String[] result = new String[listSize];
        for(int i=0; i<listSize;i++){
            result[i] = vids.get(i);
        }
        return result;
    }

    public void closeClient() {
        esClient.close();
    }

    public static void main(String[] args) {

    }
}
