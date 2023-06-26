package edu.stanford.futuredata.uniserve.integration;

import edu.stanford.futuredata.uniserve.broker.Broker;
import edu.stanford.futuredata.uniserve.coordinator.Coordinator;
import edu.stanford.futuredata.uniserve.coordinator.DefaultAutoScaler;
import edu.stanford.futuredata.uniserve.coordinator.DefaultLoadBalancer;
import edu.stanford.futuredata.uniserve.datastore.DataStore;
import edu.stanford.futuredata.uniserve.interfaces.SerializablePredicate;
import edu.stanford.futuredata.uniserve.localcloud.LocalDataStoreCloud;
import edu.stanford.futuredata.uniserve.relationalmock.*;
import edu.stanford.futuredata.uniserve.relationalmock.rowbuilders.RMRowBuilder;
import org.apache.commons.io.FileUtils;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.nio.file.Path;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.fail;


public class RMTest {
    private static final Logger logger = LoggerFactory.getLogger(edu.stanford.futuredata.uniserve.integration.RMTest.class);
    private static String zkHost = "127.0.0.1";
    private static Integer zkPort = 2181;
    public static void cleanUp(String zkHost, int zkPort) {
        // Clean up ZooKeeper
        String connectString = String.format("%s:%d", zkHost, zkPort);
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        CuratorFramework cf = CuratorFrameworkFactory.newClient(connectString, retryPolicy);
        cf.start();
        try {
            for (String child : cf.getChildren().forPath("/")) {
                if (!child.equals("zookeeper")) {
                    cf.delete().deletingChildrenIfNeeded().forPath("/" + child);
                }
            }
        } catch (Exception e) {
            logger.info("Zookeeper cleanup failed: {}", e.getMessage());
        }
        // Clean up directories.
        try {
            FileUtils.deleteDirectory(new File("/var/tmp/KVUniserve0"));
            FileUtils.deleteDirectory(new File("/var/tmp/KVUniserve1"));
            FileUtils.deleteDirectory(new File("/var/tmp/KVUniserve2"));
            FileUtils.deleteDirectory(new File("/var/tmp/KVUniserve3"));
        } catch (IOException e) {
            logger.info("FS cleanup failed: {}", e.getMessage());
        }
    }

    @BeforeAll
    static void startUpCleanUp() {
        cleanUp(zkHost, zkPort);
    }

    @Test
    public void testDynamicLogic(){
        Coordinator coordinator = new Coordinator(null, new DefaultLoadBalancer(), new DefaultAutoScaler(),
                zkHost, zkPort, "127.0.0.1", 7778);
        coordinator.runLoadBalancerDaemon = false;
        coordinator.startServing();
        LocalDataStoreCloud ldsc1 = new LocalDataStoreCloud();
        DataStore<RMRow, RMShard> dataStoreOne = new DataStore<>(ldsc1,
                new RMShardFactory(), Path.of(String.format("/var/tmp/RMUniserve%d", 1)), zkHost, zkPort, "127.0.0.1", 8200, -1, false
        );
        dataStoreOne.runPingDaemon = false;
        dataStoreOne.startServing();
        LocalDataStoreCloud ldsc2 = new LocalDataStoreCloud();
        DataStore  dataStoreTwo = new DataStore<>(ldsc2,
                new RMShardFactory(), Path.of(String.format("/var/tmp/RMUniserve%d", 2)), zkHost, zkPort, "127.0.0.1", 8201, -1, false
        );
        dataStoreTwo.runPingDaemon = false;
        assertTrue(dataStoreTwo.startServing());
        Broker broker = new Broker(zkHost, zkPort, null);
        RMQueryEngine rmQueryEngine = new RMQueryEngine(broker);
        assertTrue(rmQueryEngine.createTable("People", 1));
        List<RMRow> rowList = new ArrayList<>();
        for(int i = 0; i<50; i++){
            rowList.add(new RMRowBuilder().setPartitionKey(i).setAge(i).build());
        }
        /*Insert*/
        assertTrue(rmQueryEngine.insertPersons(rowList, "People"));

        SerializablePredicate<RMRow> filterPredicate = (RMRow row) -> row.getAge() < 20;
        List<RMRow> res = rmQueryEngine.filter(filterPredicate, "People");

        for(RMRow row: res){
            assertTrue(row.getAge()<20);
            logger.info("Age: {}", row.getAge() );
        }

        dataStoreOne.shutDown();
        dataStoreTwo.shutDown();
        coordinator.stopServing();
        broker.shutdown();
        try {
            ldsc1.clear();
            ldsc2.clear();
        } catch (IOException e) {
            fail();
        }
    }

    @Test
    public void testReadNotWrittenShardBug(){
        Coordinator coordinator = new Coordinator(null, new DefaultLoadBalancer(), new DefaultAutoScaler(),
                zkHost, zkPort, "127.0.0.1", 7778);
        coordinator.startServing();


        LocalDataStoreCloud ldsc1 = new LocalDataStoreCloud();
        DataStore<RMRow, RMShard> dataStoreOne = new DataStore<>(ldsc1,
                new RMShardFactory(), Path.of(String.format("/var/tmp/RMUniserve%d", 1)),
                zkHost, zkPort, "127.0.0.1", 8200, -1, false
        );
        dataStoreOne.startServing();

        Broker broker = new Broker(zkHost, zkPort, null);

        RMQueryEngine rmQueryEngine = new RMQueryEngine(broker);
        assertTrue(rmQueryEngine.createTable("People", 2));

        assertTrue(rmQueryEngine.insertPersons(List.of(new RMRowBuilder().setAge(51).setPartitionKey(51).build()), "People"));
        SerializablePredicate<RMRow> filterPredicate1 = (RMRow row) -> row.getAge() > 50;
        List<RMRow> res = rmQueryEngine.filter(filterPredicate1, "People");
        dataStoreOne.shutDown();
        coordinator.stopServing();
        broker.shutdown();
        try {
            ldsc1.clear();
        } catch (IOException e) {
            fail();
        }
    }
}

