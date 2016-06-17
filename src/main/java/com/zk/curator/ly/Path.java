package com.zk.curator.ly;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;

import java.nio.charset.Charset;
import java.util.List;

/**
 * ClassName: Paths
 * Description:
 * Date: 2015/7/15 17:43
 *
 * @author zg09823
 * @version V1.0
 * @since JDK 1.7
 */
public class Path {

	private static final String CONF = "conf";
	private static final String NODES = "nodes";
	private static final String SEPERATOR = "/";
//	private static final String INFO = "info";
	private static final String HEARTBEAT = "heartbeat";
	private static final String CLIENT = "client";
	private static final String NAMESPACE = "clusterMonitor";
	private static final String CLUSTER = "cluster";
	private static List<String> strings = Lists.newArrayList(Strings.nullToEmpty(null), NAMESPACE);
	private static Joiner joiner = Joiner.on(SEPERATOR).skipNulls();

	public static Charset getCharset() {
		return charset;
	}

	private static Charset charset = Charset.forName("utf-8");


	public PathBuilder create(){
		return new PathBuilder(strings);
	}

	public PathBuilder getNodePath(String clusterName){
		return this.getClusterRoot().append(clusterName).append(NODES);
	}

	public  PathBuilder getConfPath(String clusterName){
		return this.getClusterRoot().append(clusterName).append(CONF);
	}

	public PathBuilder getClientPath(){
		List<String> newStrings = Lists.newArrayList(strings);
		newStrings.add(CLIENT);
		return new PathBuilder(newStrings);
	}



	public  PathBuilder getClusterRoot(){
		List<String> newStrings = Lists.newArrayList(strings);
		newStrings.add(CLUSTER);
		return new PathBuilder(newStrings);
	}

	public class PathBuilder{
		private List<String> fullPath;

		public PathBuilder(List<String> fullPath) {
			this.fullPath = fullPath;
		}

		public PathBuilder append(String path){
		fullPath.add(path);
		return new Path.PathBuilder(fullPath);
	}
		public String build(){
			return joiner.join(fullPath);
		}

	}






	public static void main(String[] args) {

	}
}
