package edu.stanford.futuredata.uniserve.rel;

import edu.stanford.futuredata.uniserve.broker.Broker;
import edu.stanford.futuredata.uniserve.coordinator.Coordinator;
import edu.stanford.futuredata.uniserve.coordinator.DefaultAutoScaler;
import edu.stanford.futuredata.uniserve.coordinator.DefaultLoadBalancer;
import edu.stanford.futuredata.uniserve.datastore.DataStore;
import edu.stanford.futuredata.uniserve.integration.KVStoreTests;
import edu.stanford.futuredata.uniserve.interfaces.ReadQueryResults;
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
import org.apache.zookeeper.common.Time;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

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
    public void a(){

    }

    @Test
    public void simpleQueriesTests(){
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

        List<RelRow> actorRows = new ArrayList<>();
        List<RelRow> filmRows = new ArrayList<>();

        String actorFilePath = "/home/vsz/Scrivania/Uniserve/src/test/java/edu/stanford/futuredata/uniserve/rel/ActorTestFile.txt";
        String filmFilePath = "/home/vsz/Scrivania/Uniserve/src/test/java/edu/stanford/futuredata/uniserve/rel/FilmTestFile.txt";

        int filmCount = 0;
        try (BufferedReader br = new BufferedReader(new FileReader(filmFilePath))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] parts = line.split(",");
                if (parts.length == 2) {
                    filmRows.add(new RelRow(filmCount, parts[0], Integer.valueOf(parts[1])));
                    filmCount++;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        int count = 0;
        try (BufferedReader br = new BufferedReader(new FileReader(actorFilePath))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] parts = line.split(",");
                if (parts.length == 3) {
                    actorRows.add(new RelRow(count, parts[0], parts[1],Integer.valueOf(parts[2]), new Random().nextInt(filmCount)));
                    count++;
                }else{
                    System.out.println("No match for " + line);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        API api = new API();
        api.start(zkHost, zkPort);
        api.createTable("Actors").attributes("ID", "FullName", "DateOfBirth", "Salary", "FilmID").shardNumber(10000).keys("ID").build().run();
        api.createTable("Films").attributes("ID", "Director", "Budget").keys("ID").shardNumber(10000).build().run();
        api.write().table(broker, "Actors").data(actorRows).build().run();
        api.write().table(broker, "Films").data(filmRows).build().run();

        //aliases tests
        RelReadQueryResults results = api.read()
                .select("Actors.FullName", "A.Salary")
                .alias("Full Name", "Salary")
                .from("Actors", "A")
                .build()
                .run(broker);
        List<RelRow> data = results.getData();

        List<RelRow> notMatching = new ArrayList<>();
        for(int i = 0; i<actorRows.size(); i++){
            RelRow originalRow = actorRows.get(i);
            boolean match = false;
            for(int j = 0; j<data.size() && !match; j++){
                RelRow queryRow = data.get(j);
                if(originalRow.getField(1).equals(queryRow.getField(0)) && originalRow.getField(3).equals(queryRow.getField(1))){
                    match = true;
                }
            }
            if(!match){
                notMatching.add(originalRow);
            }
        }
        assertTrue(notMatching.isEmpty());

        //join
        RelReadQueryResults joinResults = api.read()
                .select("A.FullName", "F.Director", "F.ID")
                .alias("ActorName", "DirectorName", "")
                .from("Actors", "A")
                .from("Films", "F")
                .where("A.FilmID == F.ID && DirectorName == \"Christopher Nolan\"")
                .build()
                .run(broker);
        List<RelRow> joinResRows = joinResults.getData();

        List<List<Object>> joinTest = new ArrayList<>();
        for(RelRow film: filmRows){
            for(RelRow actor: actorRows){
                if(film.getField(1).equals("Christopher Nolan") && film.getField(0).equals(actor.getField(4))){
                    joinTest.add(List.of(actor.getField(1), film.getField(1)));
                }
            }
        }
        for(int i = 0; i<joinTest.size(); i++){
            boolean match = false;
            List<Object> testRow = joinTest.get(i);
            for(int j = joinResRows.size()-1 ; j>=0 && !match; j--){
                RelRow resRow = joinResRows.get(j);
                if(resRow.getField(0).equals(testRow.get(0)) && resRow.getField(1).equals(resRow.getField(1))){
                    match = true;
                    joinResRows.remove(resRow);
                }
            }
        }
        if(!joinResRows.isEmpty()){
            printRowList(joinResRows);
        }
        System.out.println();
        if(!joinResRows.isEmpty()){
            List<RelRow> rerere = new ArrayList<>();
            for(List<Object> testRow: joinTest){
                rerere.add(new RelRow(testRow.toArray()));
            }
            printRowList(rerere);
        }
        assertTrue(joinResRows.isEmpty());
    }

    private void printRowList(List<RelRow> data){
        for(RelRow row:data){
            StringBuilder rowBuilder = new StringBuilder();
            rowBuilder.append("Row #" + data.indexOf(row) + " ");
            for (int j = 0; j<row.getSize()-1; j++){
                rowBuilder.append(row.getField(j) + ", ");
            }
            rowBuilder.append(row.getField(row.getSize()-1));
            System.out.println(rowBuilder.toString());
        }
    }

    @Test
    public void aggregateTest(){
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
        List<RelRow> actorRows = new ArrayList<>();
        List<RelRow> filmRows = new ArrayList<>();
        String actorFilePath = "/home/vsz/Scrivania/Uniserve/src/test/java/edu/stanford/futuredata/uniserve/rel/ActorTestFile.txt";
        String filmFilePath = "/home/vsz/Scrivania/Uniserve/src/test/java/edu/stanford/futuredata/uniserve/rel/FilmTestFile.txt";
        int totFilmBudget = 0, avgFilmBudget = 0, minBudget = Integer.MAX_VALUE, maxBudget = Integer.MIN_VALUE;
        int filmCount = 0;
        int count = 0;
        try (BufferedReader br = new BufferedReader(new FileReader(filmFilePath))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] parts = line.split(",");
                if (parts.length == 2) {
                    int budget = Integer.parseInt(parts[1]);
                    filmRows.add(new RelRow(filmCount, parts[0], budget));
                    filmCount++;
                    totFilmBudget += budget;
                    minBudget = Integer.min(minBudget, budget);
                    maxBudget = Integer.max(maxBudget, budget);
                }
            }
            avgFilmBudget = totFilmBudget / filmCount;
        } catch (IOException e) {
            e.printStackTrace();
        }
        try (BufferedReader br = new BufferedReader(new FileReader(actorFilePath))) {
            String line;
            Random rng = new Random(Time.currentElapsedTime());
            while ((line = br.readLine()) != null) {
                String[] parts = line.split(",");
                if (parts.length == 3) {
                    int random = rng.nextInt();
                    if(random <0){
                        random *= -1;
                    }
                    actorRows.add(new RelRow(count, parts[0], parts[1], Integer.valueOf(parts[2]), random % filmCount));
                    count++;
                } else {
                    System.out.println("No match for " + line);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        API api = new API();
        api.start(zkHost, zkPort);
        api.createTable("Actors").attributes("ID", "FullName", "DateOfBirth", "Salary", "FilmID").shardNumber(10).keys("ID").build().run();
        api.createTable("Films").attributes("ID", "Director", "Budget").keys("ID").shardNumber(10).build().run();
        api.write().table(broker, "Actors").data(actorRows).build().run();
        api.write().table(broker, "Films").data(filmRows).build().run();
        RelReadQueryResults totBudget = api.read()
                .select()
                .from("Films")
                .sum("Films.Budget", "TotBudget")
                .count("Films.Budget", "Count")
                .avg("Films.Budget", "AvgFilmBudget")
                .max("Films.Budget", "MaxBudget")
                .min("Films.Budget", "MinBudget")
                .build().run(broker);
        assertEquals(totFilmBudget, (int) (Integer) totBudget.getData().get(0).getField(0));
        assertEquals(filmCount, (int) (Integer) totBudget.getData().get(0).getField(1));
        assertEquals(avgFilmBudget, (int) (Integer) totBudget.getData().get(0).getField(2));
        assertEquals(maxBudget, (int) (Integer) totBudget.getData().get(0).getField(3));
        assertEquals(minBudget, (int) (Integer) totBudget.getData().get(0).getField(4));

        RelReadQueryResults totalActorEarnings = api.read()
                .select("F.Director")
                .alias("DirectorName")
                .sum("A.Salary", "TotalActorsEarnings")
                .count("A.Salary", "NumFilms")
                .from("Actors", "A")
                .from("Films", "F")
                .where("A.FilmID == F.ID")
                .having("NumFilms > 1")
                .build().run(broker);
        System.out.println(totalActorEarnings.getFieldNames());
        printRowList(totalActorEarnings.getData());

        Map<String, Integer> sums = new HashMap<>();
        Map<String, Integer> counts = new HashMap<>();

        for(int i = 0; i<filmRows.size(); i++){
            RelRow filmRow = filmRows.get(i);
            for(int j = 0; j<actorRows.size(); j++){
                RelRow actorRow = actorRows.get(j);
                if(actorRow.getField(4).equals(filmRow.getField(0))){
                    if(sums.containsKey((String) filmRow.getField(1))){
                        sums.put((String) filmRow.getField(1), (sums.get((String) filmRow.getField(1))+(Integer)actorRow.getField(3)));
                    }else{
                        sums.put((String) filmRow.getField(1), (Integer) actorRow.getField(3));
                    }
                    if(counts.containsKey((String) filmRow.getField(1))){
                        counts.put((String) filmRow.getField(1), (sums.get((String) filmRow.getField(1))+1));
                    }else{
                        counts.put((String) filmRow.getField(1), 1);
                    }
                }
            }
        }
        for(RelRow resRow: totalActorEarnings.getData()){
            String directorName = (String) resRow.getField(0);
            if(counts.containsKey(directorName) && counts.get(directorName)>1){
                assertEquals(sums.get(directorName), (Integer) resRow.getField(1));
            }
        }
    }
}
