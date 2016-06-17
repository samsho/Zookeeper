package com.zk.curator.sync;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

import java.util.List;

/**
 * 迁移zookeeper节点及节点数据
 */
public class MoveZkNode {

    public static void main(String[] args) throws Exception {

        //旧zk配置
//        ZooKeeper oldzk = new ZooKeeper("hmaster.localdomain:2181,hslave02.localdomain:2181,hslave01.localdomain:2181", 60000, null);
        ZooKeeper oldzk = new ZooKeeper("hslave11:2181,hslave12:2181,hslave13:2181,hslave14:2181,hslave15:2181,hslave16:2181,hslave17:2181", 60000, null);
        //新zk配置
//        ZooKeeper newzk = new ZooKeeper("kmaster:2181,kslave01:2181,kslave02:2181", 60000, null);
        ZooKeeper newzk = new ZooKeeper("ZK-186-002:2201,ZK-186-003:2201,ZK-186-004:2201,ZK-186-005:2201,ZK-186-006:2201,ZK-186-007:2201,ZK-186-008:2201", 60000, null);
        //迁移的节点
        String node = "/hbasesdk";//手动新建
        List<String> children = oldzk.getChildren(node, false);
        move(oldzk, newzk, children, node);
        oldzk.close();
        newzk.close();
    }

    private static void move(ZooKeeper oldzk, ZooKeeper newzk, List<String> children, String parent)
            throws KeeperException, InterruptedException {
        if (children == null || children.isEmpty()) {
            return;
        } else {
            for (String child : children) {
                String c = parent + "/" + child;
                System.out.println(c);
                byte[] data = oldzk.getData(c, false, null);
                if (newzk.exists(c, false) == null) {
                    newzk.create(c, data, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                } else {
                    newzk.setData(c, data, -1, null, null);
                }
                move(oldzk, newzk, oldzk.getChildren(c, false), c);
            }
        }
    }
}