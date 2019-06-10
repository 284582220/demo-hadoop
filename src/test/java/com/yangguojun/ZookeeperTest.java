package com.yangguojun;

import com.yangguojun.curator.*;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

/**
 * Created by jicexosl on 2016/12/1.
 */
public class ZookeeperTest {


    @Before
    public void init() {
        ZkUtil.init("172.19.100.180:2181,172.19.100.180:2182,172.19.100.180:2183,172.19.100.180:2184", 2000, "user:password");
    }


    @Test
    public void createTest() throws Exception {
        ZkUtil.createData("/test2", "test1");
    }


    @Test
    public void getDataTest() throws Exception {
        for(int i=0; i< 10; i++){
            Thread.sleep(3000L);
            System.out.println(ZkUtil.getChildren("/kafkaRoot"));
        }
    }


    @Test
    public void getChildrenTest() throws Exception {
        System.out.println(ZkUtil.getChildren("/test"));
    }


    @Test
    public void setDataTest() throws Exception {
//        ZkUtil.setData("/dcp", "test2");
//        System.out.println(ZkUtil.getData("/dcp"));

        int childNum = 1024;
        char[] chars = new char[1024];
        Arrays.fill(chars, 'x');
        String nameSuffix = new String(chars);
        ZkUtil.setData("/dcp", "test");
        for (int i = 0; i < childNum; i++) {
            String childPath = "/dcp" + "/" + i + "-" + nameSuffix;
            ZkUtil.createData(childPath, childPath);
//            ZkUtil.setData(childPath, childPath);
        }
    }


    @Test
    public void createEphemeral() throws Exception {
        ZkUtil.createEphemeral("/dcp/test13", "value13");
        Thread.sleep(5000);
    }


    @Test
    public void deleteTest() throws Exception {
        //ZkUtil.delete("/dcp2");
        ZkUtil.deleteWithChildren("/dcp2");
    }


    @Test
    public void watcherTest() throws Exception {
        ZkUtil.setPathWatcher("/dcp", new PathEventListener() {
            @Override
            public void pathEventHandle(PathEvent event) {
                System.out.println(event.getType());
                System.out.println(event.getPath());
                System.out.println(event.getValue());
            }
        });

        Thread.sleep(1000);

        ZkUtil.setData("/dcp", "911");
    }

    @Test
    public void startPathWatcherTest() throws Exception {

        ZkUtil.startPathWatcher("/dcp");

        Thread.sleep(10000L);
    }


    @Test
    public void createSequential() throws Exception {
        System.out.println(ZkUtil.createSequential("/dcp/seq", "seq"));
    }


    @Test
    public void testExists() throws Exception {
        System.out.println(ZkUtil.exists("/abc"));
    }


    @Test
    public void testNodeWatcher() {
        try {
            ZkUtil.setData("/aclte", "27");
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        try {
            ZkUtil.setNodeWatcher("/dcp", new NodeEventListener() {
                @Override
                public void nodeEventHandle(NodeEvent event) {
                    System.out.println(event.getType());
                }
            });
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            Thread.sleep(1000000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


    @Test
    public void setAclTest(){
        try {
            ZkUtil.setAcl("/aclx/cc");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    @Test
    public void aclTest() {
        try {
            ZkUtil.createData("/aclnode/test", "2");
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            ZkUtil.setNodeWatcher("/aclnode/test", new NodeEventListener() {
                @Override
                public void nodeEventHandle(NodeEvent event) {
                    System.out.println(event.getType());
                    System.out.println(event.getValue());
                }
            });
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            Thread.sleep(1000000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }



}
