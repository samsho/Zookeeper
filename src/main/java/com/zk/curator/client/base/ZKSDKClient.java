package com.zk.curator.client.base;

import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.util.List;
import java.util.concurrent.CountDownLatch;

public class ZKSDKClient implements Watcher {

    private static Logger logger = Logger.getLogger(ZKSDKClient.class);
    private ZooKeeper zk;
    private static CountDownLatch connectSemaphore = new CountDownLatch(1);
    private static final String basePath = "/hbasesdk";
    private static final String CLUSTER_CHANGE = "/hbasesdk/clusterstatus";
    private String tokenPath;
    private static ZKSDKClient zkclient;

    public void process(WatchedEvent event) {

        if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
            if (event.getType() == Watcher.Event.EventType.None) {
                logger.info("zkclient connect to zk cluster successful");
                connectSemaphore.countDown();
            } else {

                String eventPath = event.getPath();
                logger.info("event path is===" + eventPath);
                // cluster status monitor
                if (eventPath.contains(CLUSTER_CHANGE)) {
                    try {

                        String[] nodeArr = splitZKPath(eventPath);
                        if (nodeArr.length >= 4) {

                            String id = nodeArr[3];
                            String status = new String(zk.getData(eventPath,
                                    this, null));
                            int clusterStatus = Integer.parseInt(status);
                        } else {
                        }

                    } catch (KeeperException e) {
                        logger.error("get hbase cluster status error", e);
                    } catch (InterruptedException e) {
                        logger.error("get hbase cluster status error", e);
                    }
                } else {

                    String[] nodeArr = splitZKPath(eventPath);
                    if (nodeArr.length == 5) {
                        String endNode = nodeArr[4];
                        String tableName = nodeArr[3];

                        if (event.getType() == Watcher.Event.EventType.NodeDeleted) {
                            logger.info("hbasesdkclient got a node delete event....."
                                    + event.getPath());

                            try {
                                zk.exists(eventPath, this);
                            } catch (KeeperException e) {
                                logger.error(
                                        "hbasesdkclient monitor node exist error",
                                        e);
                            } catch (InterruptedException e) {
                                logger.error(
                                        "hbasesdkclient monitor node exist error",
                                        e);
                            }

                            if (endNode.equals("acl")) {

                            } else if (endNode.equals("index")) {
                            }

                        } else if (event.getType() == Watcher.Event.EventType.NodeCreated
                                || event.getType() == Watcher.Event.EventType.NodeDataChanged) {
                            logger.info("hbasesdkclinet got a "
                                    + (event.getType() == Watcher.Event.EventType.NodeCreated ? "node Create Event "
                                    : "Node data change Event ")
                                    + event.getPath());
                            String nodeData = "0";
                            try {
                                nodeData = new String(zk.getData(eventPath,
                                        this, null));
                            } catch (KeeperException e) {
                                logger.error(
                                        "hbasesdkclient get node data error", e);
                            } catch (InterruptedException e) {
                                logger.error(
                                        "hbasesdkclient get node data error", e);
                            }

                            if (endNode.equals("acl")) {

                                int acl = Integer.parseInt(nodeData);

                            } else if (endNode.equals("index")) {
                            }

                        }
                    } else {
                        try {

                            List<String> tableList = zk.getChildren(eventPath, this);
                            for (String table : tableList) {

                                String tokenTablePath = tokenPath + "/" + table;
                                String tokenTableAclPath = tokenTablePath + "/acl";
                                String tokenTableIndexPath = tokenTablePath + "/index";

                                if (zk.exists(tokenTableAclPath, this) != null) {
                                    int acl = Integer.parseInt(new String(zk.getData(
                                            tokenTableAclPath, this, null)));
                                }
                                if (zk.exists(tokenTableIndexPath, this) != null) {
                                    String indexInfos = new String(zk.getData(
                                            tokenTableIndexPath, this, null));
                                }

                            }

                            zk.exists(tokenPath, this);
                        } catch (KeeperException e) {
                            logger.error("", e);
                        } catch (InterruptedException e) {
                            logger.error("", e);
                        }

                    }
                }

            }

        }

    }

    /**
     * 初始化获取数据
     * @param token
     * @param zkHost
     * @param port
     * @param sessionTimeout
     * @throws Exception
     */
    public ZKSDKClient(String token, String zkHost, int port, int sessionTimeout)
            throws Exception {
        try {
            zk = new ZooKeeper(zkHost + ":" + port, sessionTimeout, this);
            connectSemaphore.await();
            tokenPath = basePath + "/" + token;
            logger.info("tokenPath is ------------" + tokenPath);
            Stat tokenNode = zk.exists(tokenPath, this);
            if (tokenNode == null) {
                throw new RuntimeException(
                        "please connect HBaseManager admin to confirm this token has already add to HBaseManager");
            }
            List<String> tableList = zk.getChildren(tokenPath, this);
            for (String table : tableList) {

                String tokenTablePath = tokenPath + "/" + table;
                String tokenTableAclPath = tokenTablePath + "/acl";
                String tokenTableIndexPath = tokenTablePath + "/index";

                if (zk.exists(tokenTableAclPath, this) != null) {
                    int acl = Integer.parseInt(new String(zk.getData(tokenTableAclPath, this, null)));
                }
                if (zk.exists(tokenTableIndexPath, this) != null) {
                    String indexInfos = new String(zk.getData(tokenTableIndexPath, this, null));
                }
            }
            // monitor zk status change
            zk.exists(CLUSTER_CHANGE, this);
            List<String> clusters = zk.getChildren(CLUSTER_CHANGE, this);
            for (String cluster : clusters) {
                String clusterPath = CLUSTER_CHANGE + "/" + cluster;
            }

        } catch (Exception e) {
            logger.error("init zkClient ", e);
            throw e;
        }
    }

    public String[] splitZKPath(String zkPath) {
        return zkPath.split("/");
    }

    public static ZKSDKClient getZKClient(String token, String zkHost,
                                          int port, int sessionTimeout) throws Exception {

        synchronized (ZKSDKClient.class) {

            if (zkclient == null) {
                try {
                    zkclient = new ZKSDKClient(token, zkHost, port,
                            sessionTimeout);
                } catch (Exception e) {
                    logger.error("get zkClient exception");
                    throw e;
                }
            }
        }

        return zkclient;
    }

    public static ZKSDKClient getZKClient() {

        if (zkclient == null) {
            throw new RuntimeException("init zkclient first");
        }
        return zkclient;
    }

}
