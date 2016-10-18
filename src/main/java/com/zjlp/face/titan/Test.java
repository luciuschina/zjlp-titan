package com.zjlp.face.titan;

import com.zjlp.face.titan.impl.TitanDAOImpl;

import java.util.Map;

/**
 * Created by root on 10/17/16.
 */
public class Test {
    public static void main(String[] args) {
        ITitanDAO titanDao = new TitanDAOImpl();
        titanDao.getComFriendsNum("18707042242", "18042486488,18707042242".split(","));
        System.out.println("上面那次不算。第一次加载初始化耗时比一般的多");
        Map m = titanDao.getComFriendsNum(args[0], args[1].split(","));

        System.out.println(m);
        if (args.length > 2) {
            Map m2 = titanDao.getComFriendsNum(args[2], args[3].split(","));
            System.out.println(m2);
        }
    }
}
