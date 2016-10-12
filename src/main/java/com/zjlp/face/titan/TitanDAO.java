package com.zjlp.face.titan;

import com.zjlp.face.bean.Relation;

import java.util.List;

/**
 * Created by root on 10/11/16.
 */
public interface TitanDAO {
    void addUsername(String userName);

    void addUsernames(List<String> userNames);

    void addRelation(Relation relation);

    void addRelations(List<Relation> relations);

    void deleteUsername(String userName);

    void deleteUsernames(List<String> userNames);

    void deleteRelation(Relation relation);

    void deleteRelations(List<Relation> relations);
}
