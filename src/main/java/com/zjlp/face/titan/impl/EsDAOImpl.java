package com.zjlp.face.titan.impl;

import com.zjlp.face.spark.base.IfCache;
import com.zjlp.face.spark.base.Props;
import com.zjlp.face.spark.base.UserVertexId;
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
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

@Service("EsDAOImpl")
public class EsDAOImpl implements IEsDAO ,Serializable {
    private static final Logger LOGGER = LoggerFactory.getLogger(EsDAOImpl.class);
    private Client esClient = null;
    private String titanEsIndex = Props.get("titan-es-index");

    public Client getEsClient() {
        if (esClient == null) {
            esClient = EsUtils.getEsClient(Props.get("es.cluster.name"), Props.get("es.nodes"), Integer.valueOf(Props.get("es.client.port")));
        }
        return esClient;
    }

    public void multiCreate(List<UserVertexId> items) {
        if (items.isEmpty()) return;
        Client client = getEsClient();
        BulkRequestBuilder bulkRequest = client.prepareBulk();
        for (UserVertexId item : items) {
            try {
                bulkRequest.add(client.prepareIndex(titanEsIndex, "rel", item.userId())
                        .setSource(jsonBuilder()
                                .startObject()
                                .field("vertexId", item.vertexId())
                                .endObject()));
            } catch (Exception e) {
                LOGGER.error("ES插入索引失败.userId:" + item.userId() + ",vertexId:" + item.vertexId(), e);
            }
        }
        bulkRequest.get();
    }

    public void multiCreateIfCache(List<IfCache> items) {
        if (items.isEmpty()) return;
        Client client = getEsClient();
        BulkRequestBuilder bulkRequest = client.prepareBulk();
        for (IfCache item : items) {
            try {
                bulkRequest.add(client.prepareIndex(titanEsIndex, "ifcache", item.userId())
                        .setSource(jsonBuilder()
                                .startObject()
                                .field("isCached", item.isCached())
                                .endObject()));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        bulkRequest.get();
    }

    public void create(UserVertexId item) throws IOException {
        getEsClient().prepareIndex(titanEsIndex, "rel", item.userId())
                .setSource(jsonBuilder().startObject().field("vertexId", item.vertexId()).endObject()).get();
    }

    public String getVertexId(String userId) {
        SearchResponse response = getEsClient().prepareSearch(titanEsIndex).setTypes("rel")
                .setQuery(QueryBuilders.idsQuery().ids(userId))
                .setExplain(false).execute().actionGet();
        SearchHit[] results = response.getHits().getHits();
        if (results != null && results.length > 0)
            return results[0].getSource().get("vertexId").toString();
        else {
            LOGGER.warn("return null!. ES的titan-es这个索引中没有这个id:" + userId);
            return null;
        }
    }

    public List<String> getHotUsers() {
        SearchResponse response = getEsClient().prepareSearch(titanEsIndex).setTypes("ifcache")
                .setFrom(0).setSize(10000)
                .setQuery(QueryBuilders.termQuery("isCached", true))
                .setExplain(false).execute().actionGet();
        SearchHit[] hits = response.getHits().getHits();
        List<String> hotUserList = new ArrayList();
        for (SearchHit hit : hits) {
            hotUserList.add(hit.getId());
        }

        return hotUserList;
    }

    public String[] getVertexIds(List<String> friends) {
        if (friends.isEmpty()) return null;
        MultiGetResponse multiGetItemResponses = getEsClient().prepareMultiGet()
                .add(titanEsIndex, "rel", friends)
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
        for (int i = 0; i < listSize; i++) {
            result[i] = vids.get(i);
        }
        return result;
    }

    public void multiDelete(String[] userIds) throws Exception {
        if (userIds.length == 0) return;
        Client client = getEsClient();
        BulkRequestBuilder bulkRequest = client.prepareBulk();
        for (String userId : userIds) {
            bulkRequest.add(client.prepareDelete(titanEsIndex, "rel", userId));
        }
        bulkRequest.get();
    }
    public void delete(String userId) throws Exception {
        multiDelete(new String[]{userId});
    }

    public static void main(String[] args) {
        EsDAOImpl d = new EsDAOImpl();
        d.getHotUsers();
    }
}
