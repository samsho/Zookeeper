package com.zk.curator.ly;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.transaction.CuratorTransaction;
import org.apache.curator.utils.EnsurePath;

import java.nio.charset.Charset;

import java.util.List;


/**
 * ClassName: ClusterManager
 * Description:
 * Date: 2015/7/17 15:01
 *
 * @author sam-sho
 * @version V1.0
 * @since JDK 1.7
 */
public class ClusterManager {
	private String rootPath;
	private CuratorFramework zkClient;
	private static Path path = new Path();

	private final  String clusterListPath = path.getClusterRoot().build();
	private final String clientPath = path.getClientPath().build();

	public ClusterManager(final CuratorFramework zkClient) throws Exception {

		this.rootPath = path.getClusterRoot().build();
		this.zkClient = zkClient;
		this.ensureRootDir();

	}

	/**
	 * 确保/clusterMonitor/cluster 以及 /cluster/client路径存在
	 * @throws Exception
	 */
	private void ensureRootDir() throws Exception {
		EnsurePath clusterRootPath = new EnsurePath(this.rootPath);
		clusterRootPath.ensure(this.zkClient.getZookeeperClient());

		EnsurePath clientPath = new EnsurePath(path.getClientPath().build());
		clientPath.ensure(this.zkClient.getZookeeperClient());
	}

	/**
	 * 获取到所有的集群名称
	 * @return 一个包含所有集群的名称的list
	 * @throws Exception
	 */
	public List<String> getAllClusters() throws Exception {
		return this.zkClient.getChildren().forPath(clusterListPath);
	}

	/**
	 * 增加一个集群，配置文件为空
	 * @param clusterName
	 * @throws Exception
	 */
	public void addCluster(String clusterName) throws Exception {
		addCluster(clusterName, "");
	}

	/**
	 * 获取所有带有配置文件的集群名称
	 * @return
	 * @throws Exception
	 */
	public List<String> getAllClustersWithConf() throws Exception{
		List<String> allClusters = this.getAllClusters();
		List<String> allClustersWithConf = Lists.newArrayList();
		for(String clusterName:allClusters){
			String confPath = path.getConfPath(clusterName).build();
			byte [] conf = zkClient.getData().forPath(confPath);
			if(conf != null){
				String conf_string = new String(conf, Charset.forName("utf-8"));
				if(!Strings.isNullOrEmpty(conf_string)){
					allClustersWithConf.add(clusterName);
				}
			}
		}
		return allClustersWithConf;
	}



	/**
	 * 增加一个集群
	 * @param clusterName
	 * @param conf
	 * @throws Exception
	 */
	public void addCluster(String clusterName, String conf) throws Exception {
		Preconditions.checkArgument(zkClient.checkExists().forPath(path.getClusterRoot().append(clusterName).build()) ==null,"cluster 已存在");
		ClusterUtil.createPath(clusterName,zkClient);
		ClusterUtil.setConf(clusterName, zkClient, conf);

	}

	/**
	 * 根据集群名称，删除一个集群
	 * @param clusterName
	 * @throws Exception
	 */
	public void deleteCluster(String clusterName) throws Exception {

		Preconditions.checkArgument(this.getAllClusters().contains(clusterName), "集群 " + clusterName + "不存在");
		CuratorTransaction transaction = zkClient.inTransaction();
		for(String node:ClusterUtil.getNodes(clusterName,zkClient)){
			String nodePath = path.getNodePath(clusterName).append(node).build();
			String clientPath = path.getClientPath().append(node).append(clusterName).build();
			Preconditions.checkState(
					zkClient.checkExists().forPath(nodePath) != null
							&& zkClient.checkExists().forPath(clientPath) != null,
					"指定节点已经不存在");
			List<String> listOfPath = NodeUtil.getAllPid(clusterName,zkClient,node);

			for (int index = 0; index < listOfPath.size(); index++) {
				String fullPath = path.getNodePath(clusterName).append(node).append(listOfPath.get(index)).build();
				listOfPath.remove(index);
				listOfPath.add(index, fullPath);
			}
			for (String s : listOfPath) {
				transaction = transaction.delete().forPath(s).and();
			}
			transaction = transaction.delete()
				.forPath(nodePath)
				.and()
				.delete().forPath(clientPath).and();
		}
		transaction.delete()
				.forPath(path.getConfPath(clusterName).build())
				.and()
				.delete()
				.forPath(path.getNodePath(clusterName).build())
				.and()
				.delete().forPath(path.getClusterRoot().append(clusterName).build())
				.and().commit();

	}


}
