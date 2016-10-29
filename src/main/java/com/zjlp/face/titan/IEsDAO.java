package com.zjlp.face.titan;

import com.zjlp.face.bean.UserVertexIdPair;
import org.elasticsearch.client.Client;

import java.io.IOException;
import java.util.List;

public interface IEsDAO {
    Client getEsClient();

    /**
     * 插入多个用户信息
     * @param items
     */
    void multiCreate(List<UserVertexIdPair> items);

    /**
     * 插入一个用户信息
     * @param item
     */
    void create(UserVertexIdPair item) throws IOException;


    /**
     * 根据userId获取顶点id
     * @param userId
     * @return
     */
    String getVertexId(String userId);


    String[] getVertexIds(List<String> friends);

    boolean ifCache(String userId);

    void closeClient();
}
