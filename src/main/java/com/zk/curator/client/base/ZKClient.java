package com.zk.curator.client.base;

import org.apache.zookeeper.*;

import java.io.IOException;
import java.util.List;

/**
 * ClassName: ZKClient
 * Description:
 * Date: 2016/6/17 16:08
 *
 * @author SAM SHO
 * @version V1.0
 */
public class ZKClient {

    private static final String zkHost = "master:2181,slave1:2181,slave2:2181";
    private static ZooKeeper zk = null;

    static {
        // 创建一个与服务器的连接 需要(服务端的 ip+端口号)(session过期时间)(Watcher监听注册)
        try {
            zk = new ZooKeeper(zkHost, 3000, new Watcher() {
                // 监控所有被触发的事件
                public void process(WatchedEvent event) {
                    System.out.println("状态 ： " + event.getState());
                    System.out.println("path : " + event.getPath());
                    System.out.println("已经触发了 " + event.getType() + " 事件！");
                }
            });
        } catch (IOException e) {
        }
    }

    public static void main(String[] args) throws Exception {
//        ZKClient.createNode();
        ZKClient.getData();
//        ZKClient.setNode();
//        ZKClient.delete();
    }

    /**
     * CreateMode:
     * PERSISTENT (持续的，相对于EPHEMERAL，不会随着client的断开而消失)
     * PERSISTENT_SEQUENTIAL（持久的且带顺序的）
     * EPHEMERAL (短暂的，生命周期依赖于client session)
     * EPHEMERAL_SEQUENTIAL  (短暂的，带顺序的)
     */
    public static void createNode() throws KeeperException, InterruptedException {
        // 创建一个目录节点
        zk.create("/zkStudy", "zkStudy".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        // 创建一个子目录节点
        zk.create("/zkStudy/zkChild1", "child".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        System.out.println(new String(zk.getData("/zkStudy", false, null)));
    }

    /**
     * 获取数据
     *
     * @throws KeeperException
     * @throws InterruptedException
     */
    public static void getData() throws KeeperException, InterruptedException {
        String str = new String(zk.getData("/zkStudy", true, null));
        System.out.println("节点 ： " + str);

        // 取出子目录节点列表
        List<String> children = zk.getChildren("/zkStudy", true);
        System.out.println("子节点： " + children);
    }

    public static void setNode() throws KeeperException, InterruptedException {
        // 创建另外一个子目录节点
//        zk.create("/zkStudy/zkChild2", "child2".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
//        System.out.println(zk.getChildren("/zkStudy", true));

        // 修改子目录节点数据
        zk.setData("/zkStudy/zkChild2", "changeChild".getBytes(), -1);
        String str = new String(zk.getData("/zkStudy/zkChild2", true, null));
        System.out.println(str);
    }

    public static void delete() throws KeeperException, InterruptedException {

        //删除整个子目录   -1代表version版本号，-1是删除所有版本
        zk.delete("/zkStudy", -1);
    }
}
