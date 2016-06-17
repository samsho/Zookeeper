package com.zk.curator.client.curator;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;

/**
 * ClassName: CuratorClient
 * Description:
 * Date: 2016/6/17 20:51
 *
 * @author SAM SHO
 * @version V1.0
 */
public class CuratorClient {

    private static CuratorFramework client = getZKClient("master:2181,slave1:2181,slave2:2181");
    private static final String PATH = "/zkCurator";


    public static CuratorFramework getZKClient(String zkHost) {
        client = CuratorFrameworkFactory
                .builder()
                .connectString(zkHost)
//                .namespace("/zkCurator")
                .sessionTimeoutMs(30000)
                .retryPolicy(new ExponentialBackoffRetry(2000, 5))
                .build();
        return client;
    }

    public static void create() throws Exception {
        client.create().forPath(PATH, "zkCurator".getBytes());
        client.create().withMode(CreateMode.PERSISTENT).forPath(PATH+"/child", "child".getBytes());
    }

    public static void get() throws Exception {
        client.getData().watched().forPath(PATH);
    }



    public static void main(String[] args) {
        try {
            client.start();

            client.create().forPath(PATH, "I love messi".getBytes());

            byte[] bs = client.getData().forPath(PATH);
            System.out.println("�½��Ľڵ㣬dataΪ:" + new String(bs));

            client.setData().forPath(PATH, "I love football".getBytes());

            // ��������backgroundģʽ�»�ȡ��data����ʱ��bs����Ϊnull
            byte[] bs2 = client.getData().watched().inBackground().forPath(PATH);
            System.out.println("�޸ĺ��dataΪ" + new String(bs2 != null ? bs2 : new byte[0]));

            client.delete().forPath(PATH);
            Stat stat = client.checkExists().forPath(PATH);

            // Stat���Ƕ�zonde�������Ե�һ��ӳ�䣬 stat=null��ʾ�ڵ㲻���ڣ�
            System.out.println(stat);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            CloseableUtils.closeQuietly(client);
        }
    }
}
