package com.zjlp.face.titan;

import java.util.List;

public interface IEsDAO {

    String getVertexId(String userId);

    String[] getVertexIds(List<String> friends);

    List<String> getHotUsers();

}
