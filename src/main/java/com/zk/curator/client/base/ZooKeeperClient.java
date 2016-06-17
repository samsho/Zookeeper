
package com.zk.curator.client.base;

import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.ZooDefs.Ids;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

public class ZooKeeperClient implements Watcher {

    Logger logger = Logger.getLogger(ZooKeeperClient.class);
    private ZooKeeper zk;
    private static CountDownLatch connectSemaphore = new CountDownLatch(1);
    private static final String basePath = "/hbasesdk";

    /**
     * @param zkServerString
     * @param sessionTimeout
     * @throws Exception
     */
    public ZooKeeperClient(String zkServerString, int sessionTimeout)
            throws Exception {
        try {
            zk = new ZooKeeper(zkServerString, sessionTimeout, this);
            connectSemaphore.await();
        } catch (IOException e) {
            logger.error("init zkclient ", e);
            throw e;
        }
    }

    /**
     * @param basePath
     * @param path
     * @param data
     * @param mode
     * @throws KeeperException
     * @throws InterruptedException
     */
    private void addNode(String basePath, String path, String data,
                         CreateMode mode) throws KeeperException, InterruptedException {
        zk.create(basePath + "/" + path, data.getBytes(), Ids.OPEN_ACL_UNSAFE,
                mode);
    }

    /**
     * @param nodePath
     * @param version
     * @throws InterruptedException
     * @throws KeeperException
     */
    public void deleteNode(String nodePath, int version)
            throws InterruptedException, KeeperException {
        try {
            zk.delete(nodePath, version);
        } catch (InterruptedException e) {
            throw e;
        } catch (KeeperException e) {
            if (!(e instanceof KeeperException.NoNodeException)) {
                logger.error("delete node  error", e);
                throw e;
            }

        }
    }

    public void process(WatchedEvent event) {
        if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
            logger.info("zkclient connect to zk cluster successful");
            connectSemaphore.countDown();
        }
    }

}
