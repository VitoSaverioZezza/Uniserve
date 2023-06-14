package edu.stanford.futuredata.uniserve.integration;

import edu.stanford.futuredata.uniserve.broker.Broker;
import edu.stanford.futuredata.uniserve.coordinator.Coordinator;
import edu.stanford.futuredata.uniserve.coordinator.DefaultAutoScaler;
import edu.stanford.futuredata.uniserve.coordinator.DefaultLoadBalancer;
import edu.stanford.futuredata.uniserve.datastore.DataStore;
import edu.stanford.futuredata.uniserve.interfaces.RetrieveAndCombineQueryPlan;
import edu.stanford.futuredata.uniserve.localcloud.LocalDataStoreCloud;
import edu.stanford.futuredata.uniserve.relationalmock.RMQueryEngine;
import edu.stanford.futuredata.uniserve.relationalmock.RMRowPerson;
import edu.stanford.futuredata.uniserve.relationalmock.RMShard;
import edu.stanford.futuredata.uniserve.relationalmock.RMShardFactory;
import edu.stanford.futuredata.uniserve.relationalmock.queryplans.RMFilterPeopleByAgeOnReadQueryPlan;
import edu.stanford.futuredata.uniserve.relationalmock.rowbuilders.RMRowPersonBuilder;
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
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RMTests {

    private static final Logger logger = LoggerFactory.getLogger(RMTests.class);

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
    public void testFilter(){
        Coordinator coordinator = new Coordinator(null, new DefaultLoadBalancer(), new DefaultAutoScaler(), zkHost, zkPort, "127.0.0.1", 7778);
        coordinator.runLoadBalancerDaemon = false;
        coordinator.startServing();

        LocalDataStoreCloud ldsc1 = new LocalDataStoreCloud();
        DataStore<RMRowPerson, RMShard> dataStoreOne = new DataStore<>(ldsc1,
                new RMShardFactory(), Path.of(String.format("/var/tmp/KVUniserve%d", 1)), zkHost, zkPort, "127.0.0.1", 8200, -1, false
        );
        dataStoreOne.runPingDaemon = false;
        dataStoreOne.startServing();

        LocalDataStoreCloud ldsc2 = new LocalDataStoreCloud();
        DataStore<RMRowPerson, RMShard>  dataStoreTwo = new DataStore<>(ldsc2,
                new RMShardFactory(), Path.of(String.format("/var/tmp/RMUniserve%d", 2)), zkHost, zkPort, "127.0.0.1", 8201, -1, false
        );
        dataStoreTwo.runPingDaemon = false;
        assertTrue(dataStoreTwo.startServing());

        Broker broker = new Broker(zkHost, zkPort, null);
        RMQueryEngine queryEngine = new RMQueryEngine(broker);


        assertTrue(queryEngine.createTable("People", 1));
        List<RMRowPerson> rowList = new ArrayList<>();
        for(int i = 0; i<50; i++){
            rowList.add(new RMRowPersonBuilder().setPartitionKey(i).setAge(i).build());
        }
        /*Insert*/
        assertTrue(queryEngine.insertPersons(rowList, "People"));
        /*filter on write, results are not persisted*/
        List<RMRowPerson> filterResults = queryEngine.filterByAge(10, 25, true, false, rowList);
        for(RMRowPerson resultRow: filterResults){
            assertTrue(resultRow.getAge()>10);
            assertTrue(resultRow.getAge()<25);
        }

        /*filter on read*/
        filterResults = queryEngine.filterByAge(10, 25, false, false, null);

        for(RMRowPerson resultRow: filterResults){
            assertTrue(resultRow.getAge()>10);
            assertTrue(resultRow.getAge()<25);
        }

        int sum= 0, count = 0, actualValue;
        for(int i = 11; i<25; i++){
            sum = sum + i;
            count = count +1;
        }
        actualValue = sum/count;
        assertEquals(actualValue, queryEngine.avgAges(10,25,"People"));
        assertEquals(actualValue, queryEngine.avgAges(10,25, rowList));

        dataStoreOne.shutDown();
        dataStoreTwo.shutDown();
        coordinator.stopServing();
        broker.shutdown();
        try {
            ldsc1.clear();
            ldsc2.clear();
        } catch (IOException e) {
            assert(false);
        }
    }
}
