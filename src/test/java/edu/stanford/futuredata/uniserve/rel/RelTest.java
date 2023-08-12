package edu.stanford.futuredata.uniserve.rel;

import edu.stanford.futuredata.uniserve.broker.Broker;
import edu.stanford.futuredata.uniserve.coordinator.Coordinator;
import edu.stanford.futuredata.uniserve.coordinator.DefaultAutoScaler;
import edu.stanford.futuredata.uniserve.coordinator.DefaultLoadBalancer;
import edu.stanford.futuredata.uniserve.datastore.DataStore;
import edu.stanford.futuredata.uniserve.integration.KVStoreTests;
import edu.stanford.futuredata.uniserve.interfaces.RetrieveAndCombineQueryPlan;
import edu.stanford.futuredata.uniserve.localcloud.LocalDataStoreCloud;
import edu.stanford.futuredata.uniserve.rel.queryplans.SimpleReadAll;
import edu.stanford.futuredata.uniserve.rel.queryplans.SubquerySimpleRead;
import edu.stanford.futuredata.uniserve.relational.RelReadQueryResults;
import edu.stanford.futuredata.uniserve.relational.RelRow;
import edu.stanford.futuredata.uniserve.relational.RelShard;
import edu.stanford.futuredata.uniserve.relational.RelShardFactory;
import edu.stanford.futuredata.uniserve.relationalapi.API;
import org.apache.commons.io.FileUtils;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class RelTest {
    private static final Logger logger = LoggerFactory.getLogger(KVStoreTests.class);

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
            FileUtils.deleteDirectory(new File("/var/tmp/RelUniserve0"));
            FileUtils.deleteDirectory(new File("/var/tmp/RelUniserve1"));
            FileUtils.deleteDirectory(new File("/var/tmp/RelUniserve2"));
            FileUtils.deleteDirectory(new File("/var/tmp/RelUniserve3"));
        } catch (IOException e) {
            logger.info("FS cleanup failed: {}", e.getMessage());
        }
    }

    @BeforeAll
    static void startUpCleanUp() {
        cleanUp(zkHost, zkPort);
    }

    @AfterEach
    private void unitTestCleanUp() {
        cleanUp(zkHost, zkPort);
    }

    @Test
    public void simpleRelTest(){
        Coordinator coordinator = new Coordinator(
                null,
                new DefaultLoadBalancer(),
                new DefaultAutoScaler(),
                zkHost, zkPort,
                "127.0.0.1", 7777);
        coordinator.runLoadBalancerDaemon = false;
        coordinator.startServing();

        LocalDataStoreCloud ldsc = new LocalDataStoreCloud();

        DataStore<RelRow, RelShard> dataStore = new DataStore<>(ldsc,
                new RelShardFactory(),
                Path.of("/var/tmp/RelUniserve"),
                zkHost, zkPort,
                "127.0.0.1", 8000,
                -1,
                false
        );
        dataStore.startServing();

        Broker broker = new Broker(zkHost, zkPort);

        List<RelRow> rowsOne = new ArrayList<>();
        List<RelRow> rowsTwo = new ArrayList<>();
        for(Integer i = 0; i<20; i++){
            rowsOne.add(new RelRow(i, i+1, i+2));
            rowsTwo.add(new RelRow(i, i+1, i+2, i+3));
        }

        API api = new API();
        api.start(zkHost, zkPort);
        api.createTable("TableOne").attributes("A", "B", "C").keys("A").shardNumber(10).build().run();
        api.createTable("TableTwo").attributes("D", "E", "F", "G").keys("D").shardNumber(10).build().run();
        api.write().table(broker, "TableOne").data(rowsOne).build().run();
        api.write().table(broker, "TableTwo").data(rowsTwo).build().run();


        RetrieveAndCombineQueryPlan<RelShard, Object> readQP = new SimpleReadAll();
        RelReadQueryResults rqr = (RelReadQueryResults) broker.retrieveAndCombineReadQuery(readQP);

        for(Object r: rqr.getData()){
            RelRow row = (RelRow) r;
            System.out.print("AD: "+row.getField(0).toString() + " BE: " + row.getField(1).toString() + " CF: " + row.getField(2).toString());
            if(row.getField(3) != null){
                System.out.print(" G: " + row.getField(3).toString());
            }else{
                System.out.print(" G: null");
            }
            System.out.println("");
        }
        SubquerySimpleRead subqRQP = new SubquerySimpleRead();
        //subqRQP.setRQRInput("Results", rqr);
        RelReadQueryResults test = broker.retrieveAndCombineReadQuery(subqRQP);
        for(RelRow row: test.getData()){
            System.out.println("t: " + row.getField(0).toString());
        }
        try {
            ldsc.clear();
        }catch (Exception e){
            ;
        }
    }

    @Test
    public void testIntermediate(){
        Coordinator coordinator = new Coordinator(
                null,
                new DefaultLoadBalancer(),
                new DefaultAutoScaler(),
                zkHost, zkPort,
                "127.0.0.1", 7777);
        coordinator.runLoadBalancerDaemon = false;
        coordinator.startServing();

        LocalDataStoreCloud ldsc = new LocalDataStoreCloud();

        DataStore<RelRow, RelShard> dataStore = new DataStore<>(ldsc,
                new RelShardFactory(),
                Path.of("/var/tmp/RelUniserve"),
                zkHost, zkPort,
                "127.0.0.1", 8000,
                -1,
                false
        );
        dataStore.startServing();

        Broker broker = new Broker(zkHost, zkPort);

        List<RelRow> rowsOne = new ArrayList<>();
        List<RelRow> rowsTwo = new ArrayList<>();
        for(Integer i = 0; i<20; i++){
            rowsOne.add(new RelRow(i, i+1, i+2));
            rowsTwo.add(new RelRow(i, i+1, i+2, i+3));
        }

        API api = new API();
        api.start(zkHost, zkPort);
        api.createTable("TableOne").attributes("A", "B", "C").keys("A").shardNumber(10).build().run();
        api.createTable("TableTwo").attributes("D", "E", "F", "G").keys("D").shardNumber(10).build().run();
        api.write().table(broker, "TableOne").data(rowsOne).build().run();
        api.write().table(broker, "TableTwo").data(rowsTwo).build().run();

        RelReadQueryResults results = api.read()
                .select("T1A.TableOne.A", "T2.G")
                .from(api.read()
                        .select("TableOne.A")
                                .from("TableOne")
                                .where("TableOne.A==7 || TableOne.A==12")
                                .build()
                        ,"T1A")
                .from("TableTwo", "T2")
                .where("TableTwo.G == T1A.TableOne.A")
                .build()
                .run(broker);

        List<RelRow> data = results.getData();
        int count = 0;
        for(RelRow row: data){
            System.out.print("Row #" + count + ": ");
            count++;
            for(int i = 0; i< row.getSize(); i++){
                System.out.print(row.getField(i) + ", ");
            }
            System.out.println();
        }
    }
}
