package com.zjlp.face.titan;

import com.zjlp.face.bean.Relation;

public interface TitanDAO {
    public void addUser(String userName);

    public void addUsers(String[] userNames);

    public void addUsers(String[] userNames, int patchLength);

    public void addRelation(Relation relation);

    public void addRelations(Relation[] relations);

    public void addRelations(Relation[] relations, int patchLength);

    public void dropUser(String userName);

    public void dropUsers(String[] userNames);

    public void dropRelation(Relation relation);

    public void dropRelations(Relation[] relations);
}
