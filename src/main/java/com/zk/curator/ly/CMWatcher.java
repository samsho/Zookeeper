package com.zk.curator.ly;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.log4j.Logger;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

/**
 * ClassName: CMWatcher
 * Description: 实现curator的重新注册机制
 * Date: 2015/8/3 19:27
 *
 * @author jeffonia
 * @version V1.0
 * @since JDK 1.7
 */
public class CMWatcher implements CuratorWatcher {
    private static final Logger logger = Logger.getLogger(CMWatcher.class);
    private CuratorFramework curatorFramework;
    /**
     * 需要监控的znode path
     */
    private String path;
    /**
     * 监控事件触发后的处理逻辑块
     */
    private ICMEventHandler cmEventHandler;


    public CMWatcher(CuratorFramework curatorFramework, String path, ICMEventHandler iCMEventHandler) {
        this.cmEventHandler = iCMEventHandler;
        this.curatorFramework = curatorFramework;
        this.path = path;
        try {
            // 构造方法中注册watcher
            register();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void process(WatchedEvent event) throws Exception {
        if (event.getType() == Watcher.Event.EventType.NodeDataChanged) {
            logger.error("node data changed!!!");
            cmEventHandler.call();
        } else {
            logger.error("Unhandled event happened: " + event.getType().toString());
        }

        // 每次在使用过该watcher后，再次注册watcher
        register();
    }

    /**
     * 注册watcher
     *
     * @throws Exception
     */
    private void register() throws Exception {
        logger.error("Register watcher for " + path);
        curatorFramework.checkExists().usingWatcher(this).forPath(path);
    }
}
