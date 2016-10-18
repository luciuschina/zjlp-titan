package com.zjlp.face.titan;

import com.zjlp.face.bean.UsernameVID;
import org.elasticsearch.client.Client;

import java.util.List;

/**
 * Created by root on 10/18/16.
 */
public interface IEsDAO {
    Client getEsClient();
    void multiCreate(List<UsernameVID> items);
    String getVertexId(String username);
    void closeClient();
}
