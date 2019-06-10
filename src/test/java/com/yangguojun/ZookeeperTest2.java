package com.yangguojun;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.*;
import org.apache.curator.retry.RetryOneTime;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class ZookeeperTest2 {

    private String PATH = "/dcp009";

    static CuratorFramework client = null;

    @Test
    public void testLargeWatchBug() {
        try {

            CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder().connectString("172.19.100.180:2181")
                    .retryPolicy(new RetryOneTime(1000))
                    .connectionTimeoutMs(10000)
                    .sessionTimeoutMs(10000);
            client = builder.build();
            client.start();

//            prepareData();
            startWatch();
            Thread.sleep(600000L);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private void prepareData() throws Exception {
        int childNum = 1024;
        char[] chars = new char[1024];
        Arrays.fill(chars, 'x');
        String nameSuffix = new String(chars);
        client.create().forPath(PATH);
        for (int i = 0; i < childNum; i++) {
            String childPath = PATH + "/" + i + "-" + nameSuffix;
            client.create().forPath(childPath);
        }
    }

    private void startWatch() throws Exception {
        List<String> list = client.getChildren().forPath(PATH);
        for(String path : list){
            PathChildrenCache watcher = new PathChildrenCache(client, PATH + "/" + path, true);
            watcher.getListenable().addListener(new PathChildrenCacheListener() {
                                                    @Override
                                                    public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
                                                        ChildData data = event.getData();
                                                        if (data == null) {
                                                            System.out.println("No data in event[" + event + "]");
                                                        } else {
                                                            System.out.println("Receive event: "
                                                                    + "type=[" + event.getType() + "]"
                                                                    + ", path=[" + data.getPath() + "]"
                                                                    + ", data=[" + new String(data.getData()) + "]"
                                                                    + ", stat=[" + data.getStat().toString().trim() + "]");
                                                        }
                                                    }
                                                }

            );
            watcher.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);

        };
    }

}
