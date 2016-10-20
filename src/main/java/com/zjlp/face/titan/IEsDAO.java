package com.zjlp.face.titan;

import com.zjlp.face.bean.UsernameVID;
import org.elasticsearch.client.Client;

import java.io.IOException;
import java.util.List;

/**
 * Created by lingxin on 10/18/16.
 */
public interface IEsDAO {
    Client getEsClient();

    /**
     * 插入多个用户信息
     * @param items
     */
    void multiCreate(List<UsernameVID> items);

    /**
     * 插入一个用户信息
     * @param item
     */
    void create(UsernameVID item) throws IOException;


    /**
     * 根据username获取顶点id
     * @param username
     * @return
     */
    String getVertexId(String username);

    void closeClient();
}
