package com.zk.curator.ly;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 */
public class ZKClientFactory {
	private static Charset charset = Charset.forName("utf-8");
	private static ConcurrentHashMap<String, CuratorFramework> map = new ConcurrentHashMap();

	public static CuratorFramework getZkConnection(String zkHost) {
		CuratorFramework zkClient =  map.get(zkHost);
		if (null == zkClient) {
			zkClient = CuratorFrameworkFactory
					.builder()
					.connectString(zkHost)
//					.namespace("/clusterMonitor")
					.sessionTimeoutMs(30000)
					.retryPolicy(new ExponentialBackoffRetry(2000, 5))
					.build();
			map.put(zkHost, zkClient);
		}
		return zkClient;
	}

	public static Charset getCharset() {
		return charset;
	}



}
