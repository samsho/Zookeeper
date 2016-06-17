/*
 * Project Name:clustermonitor 
 * File Name:Cluster 
 * Package Name:utils 
 * Date:2015/7/2015:33 
 * Copyright (c) 2015, LY.com All Rights Reserved.
 */
package com.zk.curator.ly;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.transaction.CuratorTransaction;
import org.apache.curator.utils.EnsurePath;
import org.apache.zookeeper.CreateMode;

import java.nio.charset.Charset;
import java.util.List;

//import com.ly.dc.clustermonitor.model.SystemLog;

/**
 * ClassName: ClusterUtil
 * Description:
 * Date: 2015/7/20 15:33
 *
 * @author zg09823
 * @version V1.0
 * @since JDK 1.7
 */
public class ClusterUtil {
//	private String clusterName;
	private static Path path = new Path();

	private static Charset charset = Charset.forName("utf-8");



	/**
	 * 在zookeeper中建立 cluster信息存储的 znode。
	 *  分为 /clusterMonitor/cluster/name/nodes 和 /clusterMonitor/cluster/name/conf
	 * @param clusterName
	 * @param zkClient
	 * @throws Exception
	 */
	public static void createPath(String clusterName, CuratorFramework zkClient) throws Exception {

		EnsurePath clusterPath = new EnsurePath(path.getClusterRoot().append(clusterName).build());
		clusterPath.ensure(zkClient.getZookeeperClient());

		EnsurePath nodesPath = new EnsurePath(path.getNodePath(clusterName).build());
		nodesPath.ensure(zkClient.getZookeeperClient());

		EnsurePath confPath = new EnsurePath(path.getConfPath(clusterName).build());
		confPath.ensure(zkClient.getZookeeperClient());

	}

	/**
	 * 获取指定集群的配置文件
	 * @param clusterName
	 * @param zkClient
	 * @return
	 * @throws Exception
	 */
	public static String getConf(String clusterName,CuratorFramework zkClient) throws Exception {
		isConfPathExist(clusterName,zkClient);
		return new String(zkClient.getData().forPath(path.getConfPath(clusterName).build()),charset);
	}

	public static byte[] getConfInBytes(String clusterName,CuratorFramework zkClient) throws Exception {
		isConfPathExist(clusterName,zkClient);
		return zkClient.getData().forPath(path.getConfPath(clusterName).build());
	}


	private static void isConfPathExist(String clusterName,CuratorFramework zkClient) throws Exception {
		Preconditions.checkState(zkClient.checkExists().forPath(path.getConfPath(clusterName).build()) != null,
				"指定集群的配置文件目录不存在");
	}

	/**
	 * 获取指定集群中所有的节点
	 * @param clusterName
	 * @param zkClient
	 * @return
	 * @throws Exception
	 */
	public static List<String> getNodes(String clusterName,CuratorFramework zkClient) throws Exception {
		checkNodePathExist(clusterName,zkClient);
		List<String> nodesList = zkClient.getChildren().forPath(path.getNodePath(clusterName).build());
		return nodesList;
	}

	/**
	 * 设置指定节点的配置文件
	 * @param clusterName
	 * @param zkClient
	 * @param conf
	 * @throws Exception
	 */
	public static void setConf(String clusterName, CuratorFramework zkClient, String conf) throws Exception {
		isConfPathExist(clusterName,zkClient);
		String confPath =path.getConfPath(clusterName).build();
		zkClient.setData().forPath(confPath, conf.getBytes(charset));
	}

	public static void setConf(String clusterName, CuratorFramework zkClient, byte[] conf) throws Exception {
		isConfPathExist(clusterName,zkClient);
		String confPath =path.getConfPath(clusterName).build();
		zkClient.setData().forPath(confPath, conf);
	}

	private static void checkNodePathExist(String clusterName, CuratorFramework zkClient) throws Exception {
//		System.out.println(path.getNodePath(clusterName).build());
//		System.out.println(zkClient);
		Preconditions.checkState(zkClient.checkExists().forPath(path.getNodePath(clusterName).build()) != null,
				"指定集群的节点信息目录不存在");
	}

	/**
	 * 在指定集群中增加一个节点
	 * @param clusterName 指定集群名称
	 * @param node 指定的节点名称
	 * @param zkClient  zk长连接
	 * @throws Exception
	 */
	public static void addNode(String clusterName,String node,CuratorFramework zkClient) throws Exception {
		String pathToConf = path.getConfPath(clusterName).build();
		String nodePath = path.getNodePath(clusterName).append(node).build();
		String clientClusterPath = path.getClientPath().append(node).append(clusterName).build();
		String clientPath = path.getClientPath().append(node).build();
		Preconditions.checkState(
				zkClient.checkExists().forPath(nodePath) == null && zkClient.checkExists().forPath(clientClusterPath) == null,
				"指定节点已经存在");

		CuratorTransaction transaction = zkClient.inTransaction();
		if(zkClient.checkExists().forPath(clientPath) == null){
			transaction
				.create().withMode(CreateMode.PERSISTENT).forPath(nodePath)
				.and()
				.create().withMode(CreateMode.PERSISTENT).forPath(clientPath)
				.and()
				.create().withMode(CreateMode.PERSISTENT).forPath(clientClusterPath, pathToConf.getBytes(charset))
				.and()
				.commit();
		}else {
			transaction
				.create().withMode(CreateMode.PERSISTENT).forPath(nodePath)
				.and()
				.create().withMode(CreateMode.PERSISTENT).forPath(clientClusterPath, pathToConf.getBytes(charset))
				.and()
				.commit();
		}

	}

	/**
	 * 删除指定集群的指定节点
	 * @param clusterName 指定集群名称
	 * @param node 指定的节点名称
	 * @param zkClient  zk长连接
	 * @throws Exception
	 */
	public static void deleteNode(String clusterName,String node,CuratorFramework zkClient) throws Exception {
		String nodePath = path.getNodePath(clusterName).append(node).build();
		String clientPath = path.getClientPath().append(node).append(clusterName).build();
		//		String clientClusterPath = Paths.getClientClusterPath(IP, clusterName);
		Preconditions.checkState(
				zkClient.checkExists().forPath(nodePath) != null && zkClient.checkExists().forPath(clientPath) != null,
				"指定节点已经不存在");
		List<String> listOfPath = Lists.newArrayList(zkClient.getChildren().forPath(nodePath));

		for (int index = 0; index < listOfPath.size(); index++) {
			String fullPath = path.getNodePath(clusterName).append(node).append(listOfPath.get(index)).build();
			listOfPath.remove(index);
			listOfPath.add(index, fullPath);
		}

		CuratorTransaction transaction = zkClient.inTransaction();

		for (String s : listOfPath) {
			transaction = transaction.delete().forPath(s).and();
		}
		transaction
				.delete().forPath(nodePath)
				.and()
				.delete().forPath(clientPath)
				.and()
				.commit();
	}









}
