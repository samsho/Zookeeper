
package com.zk.curator.ly;

import org.apache.curator.framework.CuratorFramework;

import java.nio.charset.Charset;
import java.util.List;

/**
 * ClassName: NodeUtil
 * Description:
 * Date: 2015/7/24 10:29
 *
 * @author zg09823
 * @version V1.0
 * @since JDK 1.7
 */
public class NodeUtil {

	/**
	 * 获取节点下所有的进程
	 * @param clusterName
	 * @param zkClient
	 * @param node
	 * @return
	 * @throws Exception
	 */
	public static List<String> getAllPid(String clusterName, CuratorFramework zkClient, String node) throws Exception {
		String nodePath = path.getNodePath(clusterName).append(node).build();
		List<String> pids = zkClient.getChildren().forPath(nodePath);
		return pids;
	}

	/**
	 * 获取指定进程的监控状态
	 * @param clusterName
	 * @param zkClient
	 * @param node
	 * @param pid
	 * @return
	 * @throws Exception
	 */
	public static NodeStatus getPidStatus(String clusterName,CuratorFramework zkClient,String node,String pid) throws  Exception{
		String statusPath = path.getNodePath(clusterName).append(node).append(pid).build();
		byte[] status = zkClient.getData().forPath(statusPath);
		if(status == null){
			return NodeStatus.UNKNOWN;

		}else{
			NodeStatus nodeStatus = NodeStatus.valueOf(new String(status,charset));
			return nodeStatus;
		}
	}

	private static Charset charset= Charset.forName("utf-8");
	private static Path path = new Path();

}
