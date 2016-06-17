package com.zk.curator.ly;

import org.apache.curator.framework.CuratorFramework;

/**
 * ClassName: ICMEventHandler
 * Description:事件处理接口
 * Date: 2015/7/29 14:47
 *
 * @author jeffonia
 * @version V1.0
 * @since JDK 1.7
 */
public interface ICMEventHandler<T>
{
    CuratorFramework curatorFramework = null;
    String path = null;

    /**
     * 事件处理中心
     * TODO:针对每一个事件，定义相应的处理方法。将事件处理一一分开
     * @throws Exception
     */
    void call() throws Exception;
}
