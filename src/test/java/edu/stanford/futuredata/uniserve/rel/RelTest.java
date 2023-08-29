package edu.stanford.futuredata.uniserve.rel;

import edu.stanford.futuredata.uniserve.broker.Broker;
import edu.stanford.futuredata.uniserve.coordinator.Coordinator;
import edu.stanford.futuredata.uniserve.coordinator.DefaultAutoScaler;
import edu.stanford.futuredata.uniserve.coordinator.DefaultLoadBalancer;
import edu.stanford.futuredata.uniserve.datastore.DataStore;
import edu.stanford.futuredata.uniserve.integration.KVStoreTests;
import edu.stanford.futuredata.uniserve.localcloud.LocalDataStoreCloud;
import edu.stanford.futuredata.uniserve.relational.RelReadQueryResults;
import edu.stanford.futuredata.uniserve.relational.RelRow;
import edu.stanford.futuredata.uniserve.relational.RelShard;
import edu.stanford.futuredata.uniserve.relational.RelShardFactory;
import edu.stanford.futuredata.uniserve.relationalapi.API;
import edu.stanford.futuredata.uniserve.relationalapi.ReadQuery;
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
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.*;

import static edu.stanford.futuredata.uniserve.localcloud.LocalDataStoreCloud.deleteDirectoryRecursion;
import static java.lang.Thread.sleep;
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

    public static void a() throws IOException{
        Path LDSC = Path.of("src/main/LocalCloud/");
        if (Files.isDirectory(LDSC, LinkOption.NOFOLLOW_LINKS)) {
            try (DirectoryStream<Path> entries = Files.newDirectoryStream(LDSC)) {
                for (Path entry : entries) {
                    deleteDirectoryRecursion(entry);
                }
            }
        }
    }


    @BeforeAll
    static void startUpCleanUp() throws IOException {
        a();
        cleanUp(zkHost, zkPort);
    }

    @AfterEach
    private void unitTestCleanUp() throws IOException {
        a();
        cleanUp(zkHost, zkPort);
    }
    @Test
    public void clean(){}


    private void printRowList(List<RelRow> data) {
        for (RelRow row : data) {
            StringBuilder rowBuilder = new StringBuilder();
            rowBuilder.append("Row #" + data.indexOf(row) + " ");
            for (int j = 0; j < row.getSize() - 1; j++) {
                rowBuilder.append(row.getField(j) + ", ");
            }
            rowBuilder.append(row.getField(row.getSize() - 1));
            System.out.println(rowBuilder.toString());
        }
    }
    //TODO: test stored queries on all the previous cases
    //TODO: test on multiple JVMs (also not a problem, is only broker-side parsing of reliable queries)

    @Test
    public void simpleQueriesTests() {
        Coordinator coordinator = new Coordinator(
                null,
                new DefaultLoadBalancer(),
                new DefaultAutoScaler(),
                zkHost, zkPort,
                "127.0.0.1", 7777);
        coordinator.runLoadBalancerDaemon = false;
        coordinator.startServing();

        int numServers = 10;

        List<LocalDataStoreCloud> ldscList = new ArrayList<>();
        List<DataStore> dataStores = new ArrayList<>();
        for(int i = 0; i<numServers; i++) {
            ldscList.add(new LocalDataStoreCloud());
            DataStore<RelRow, RelShard> dataStore = new DataStore<>(ldscList.get(i),
                    new RelShardFactory(),
                    Path.of("/var/tmp/RelUniserve"),
                    zkHost, zkPort,
                    "127.0.0.1", 8000 + i,
                    -1,
                    false
            );
            dataStore.startServing();
            dataStores.add(dataStore);
        }

        Broker broker = new Broker(zkHost, zkPort);

        List<RelRow> actorRows = new ArrayList<>();
        List<RelRow> filmRows = new ArrayList<>();

        String actorFilePath = "/home/vsz/Scrivania/Uniserve/src/test/java/edu/stanford/futuredata/uniserve/rel/actorfilms/ActorTestFile.txt";
        String filmFilePath = "/home/vsz/Scrivania/Uniserve/src/test/java/edu/stanford/futuredata/uniserve/rel/actorfilms/FilmTestFile.txt";

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
                    actorRows.add(new RelRow(count, parts[0], parts[1], Integer.valueOf(parts[2]), new Random().nextInt(filmCount)));
                    count++;
                } else {
                    System.out.println("No match for " + line);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        API api = new API(broker);
        api.createTable("Actors").attributes("ID", "FullName", "DateOfBirth", "Salary", "FilmID").shardNumber(20).keys("ID").build().run();
        api.createTable("Films").attributes("ID", "Director", "Budget").keys("ID").shardNumber(20).build().run();
        api.write().table("Actors").data(actorRows).build().run();
        api.write().table("Films").data(filmRows).build().run();

        System.out.println("----- BASIC QUERIES TESTING -----");

        System.out.println("\tTEST ----- Select All Actors, single table query");
        RelReadQueryResults allActors = api.read()
                .select()
                .from("Actors")
                .build().run(broker);
        assertEquals(allActors.getData().size(), count);
        assertEquals(allActors.getFieldNames(), new ArrayList<>(Arrays.asList("Actors.ID", "Actors.FullName", "Actors.DateOfBirth", "Actors.Salary", "Actors.FilmID")));
        for(RelRow writtenRow: actorRows){
            boolean present = false;
            for(RelRow readRow: allActors.getData()){
                boolean match = true;
                for(int i = 0; i<readRow.getSize() && match; i++){
                    match = readRow.getField(i).equals(writtenRow.getField(i));
                }
                if(match){
                    present = true;
                }
            }
            assertTrue(present);
        }

        System.out.println("\tTEST ----- Select All Films, single table query");
        RelReadQueryResults allFilms = api.read()
                .select()
                .from("Films")
                .build().run(broker);
        assertEquals(allFilms.getData().size(), filmCount);
        assertEquals(allFilms.getFieldNames(), new ArrayList<>(Arrays.asList("Films.ID", "Films.Director", "Films.Budget")));
        for(RelRow writtenRow: filmRows){
            boolean present = false;
            for(RelRow readRow: allFilms.getData()){
                boolean match = true;
                for(int i = 0; i<readRow.getSize() && match; i++){
                    match = readRow.getField(i).equals(writtenRow.getField(i));
                }
                if(match){
                    present = true;
                }
            }
            assertTrue(present);
        }

        System.out.println("\tTEST ----- Projection on single table");
        RelReadQueryResults actorSimpleProj = api.read()
                .select("Actors.FullName", "Actors.Salary")
                .from("Actors")
                .build()
                .run(broker);
        assertEquals(actorSimpleProj.getData().size(), count);
        assertEquals(actorSimpleProj.getFieldNames(), new ArrayList<>(Arrays.asList("Actors.FullName", "Actors.Salary")));
        for(RelRow row: actorSimpleProj.getData()){
            assertEquals(row.getSize(), 2);
        }
        for(RelRow readRow: actorSimpleProj.getData()){
            boolean present = false;
            for(RelRow writtenRow: actorRows){
                if(readRow.getField(0).equals(writtenRow.getField(1)) &&
                        readRow.getField(1).equals(writtenRow.getField(3))){
                    present = true;
                }
            }
            assertTrue(present);
        }

        System.out.println("\tTEST ----- Projection on single table with aliases on fields");
        RelReadQueryResults actorSimpleProjAlias = api.read()
                .select("Actors.FullName", "Actors.Salary")
                .alias("FullName", "Salary")
                .from("Actors")
                .build()
                .run(broker);
        assertEquals(actorSimpleProjAlias.getData().size(), count);
        assertEquals(actorSimpleProjAlias.getFieldNames(), new ArrayList<>(Arrays.asList("FullName", "Salary")));
        for(RelRow row: actorSimpleProjAlias.getData()){
            assertEquals(row.getSize(), 2);
        }
        for(RelRow readRow: actorSimpleProjAlias.getData()){
            boolean present = false;
            for(RelRow writtenRow: actorRows){
                if(readRow.getField(0).equals(writtenRow.getField(1)) &&
                        readRow.getField(1).equals(writtenRow.getField(3))){
                    present = true;
                }
            }
            assertTrue(present);
        }

        System.out.println("\tTEST ----- Projection on single table with aliases on fields and table");
        RelReadQueryResults actorSimpleProjAliasTable = api.read()
                .select("Actors.FullName", "A.Salary")
                .alias("FullName", "Salary")
                .from("Actors", "A")
                .build()
                .run(broker);
        assertEquals(actorSimpleProjAliasTable.getData().size(), count);
        assertEquals(actorSimpleProjAliasTable.getFieldNames(), new ArrayList<>(Arrays.asList("FullName", "Salary")));
        for(RelRow row: actorSimpleProjAliasTable.getData()){
            assertEquals(row.getSize(), 2);
        }
        for(RelRow readRow: actorSimpleProjAliasTable.getData()){
            boolean present = false;
            for(RelRow writtenRow: actorRows){
                if(readRow.getField(0).equals(writtenRow.getField(1)) &&
                        readRow.getField(1).equals(writtenRow.getField(3))){
                    present = true;
                }
            }
            assertTrue(present);
        }

        System.out.println("\tTEST ----- Cartesian product");
        RelReadQueryResults cartesianProduct = api.read()
                .select()
                .from("Actors")
                .from("Films")
                .build()
                .run(broker);
        assertEquals(cartesianProduct.getData().size(), count*filmCount);
        assertEquals(cartesianProduct.getFieldNames(), new ArrayList<>(
                Arrays.asList("Films.ID", "Films.Director", "Films.Budget","Actors.ID", "Actors.FullName", "Actors.DateOfBirth", "Actors.Salary", "Actors.FilmID")));
        for(RelRow row: cartesianProduct.getData()){
            assertEquals(row.getSize(), 8);
        }
        for(RelRow readRow: cartesianProduct.getData()){
            boolean presentActor = false;
            for(RelRow writtenRow: actorRows){
                if(readRow.getField(3).equals(writtenRow.getField(0)) &&
                        readRow.getField(4).equals(writtenRow.getField(1)) &&
                        readRow.getField(5).equals(writtenRow.getField(2)) &&
                        readRow.getField(6).equals(writtenRow.getField(3)) &&
                        readRow.getField(7).equals(writtenRow.getField(4))
                ){
                    presentActor = true;
                }
            }
            assertTrue(presentActor);
            boolean presentFilm = false;
            for(RelRow writtenRow: filmRows){
                if(readRow.getField(0).equals(writtenRow.getField(0)) &&
                        readRow.getField(1).equals(writtenRow.getField(1)) &&
                        readRow.getField(2).equals(writtenRow.getField(2))
                ){
                    presentFilm = true;
                }
            }
            assertTrue(presentFilm);
        }

        System.out.println("----- PREDICATE TESTING -----");

        System.out.println("\tTEST ----- Select on Single table, single field");
        RelReadQueryResults selectActors = api.read()
                .select()
                .from("Actors")
                .where("Actors.FullName == \"Johnny Depp\" || Actors.FullName == \"Brad Pitt\"")
                .build().run(broker);
        assertEquals(selectActors.getData().size(), 2);
        assertEquals(selectActors.getFieldNames(), new ArrayList<>(Arrays.asList("Actors.ID", "Actors.FullName", "Actors.DateOfBirth", "Actors.Salary", "Actors.FilmID")));
        for(RelRow readRow: selectActors.getData()){
            boolean present = false;
            for(RelRow writtenRow: actorRows){
                boolean match = true;
                for(int i = 0; i<readRow.getSize() && match; i++){
                    match = readRow.getField(i).equals(writtenRow.getField(i));
                }
                if(match){
                    present = true;
                }
            }
            assertTrue(present);
        }

        System.out.println("\tTEST ----- Select on Single Table, multiple fields");
        RelReadQueryResults selectFilms = api.read()
                .select()
                .from("Films")
                .where("Films.ID > 5 && Films.Budget > 50000000")
                .build().run(broker);
        int selectFilmsRowCount = 0;
        for(RelRow writtenRow: filmRows){
            if((int) writtenRow.getField(0) > 5 && (int) writtenRow.getField(2) > 50000000){
                selectFilmsRowCount++;
                boolean contain = false;
                for(RelRow readRow: selectFilms.getData()){
                    if(readRow.getField(0).equals(writtenRow.getField(0)) &&
                        readRow.getField(1).equals(writtenRow.getField(1)) &&
                            readRow.getField(2).equals(writtenRow.getField(2))
                    ){
                        contain = true;
                    }
                }
                assertTrue(contain);
            }
        }
        assertEquals(selectFilms.getData().size(), selectFilmsRowCount);
        assertEquals(selectFilms.getFieldNames(), new ArrayList<>(Arrays.asList("Films.ID", "Films.Director", "Films.Budget")));


        System.out.println("\tTEST ----- Selection and Projection on single table");
        RelReadQueryResults actorSelProj = api.read()
                .select("Actors.FullName", "Actors.Salary")
                .from("Actors")
                .where("Actors.FullName == \"Julia Roberts\"")
                .build()
                .run(broker);
        assertEquals(actorSelProj.getData().size(), 1);
        assertEquals(actorSelProj.getFieldNames(), new ArrayList<>(Arrays.asList("Actors.FullName", "Actors.Salary")));
        for(RelRow row: actorSimpleProj.getData()){
            assertEquals(row.getSize(), 2);
        }
        for(RelRow readRow: actorSelProj.getData()){
            boolean present = false;
            for(RelRow writtenRow: actorRows){
                if(readRow.getField(0).equals(writtenRow.getField(1)) &&
                        readRow.getField(1).equals(writtenRow.getField(3))){
                    present = true;
                }
            }
            assertTrue(present);
        }



        System.out.println("\tTEST ----- Selection and Projection on single table with aliases on fields");
        RelReadQueryResults actorSelectProjAlias = api.read()
                .select("Actors.FullName", "Actors.Salary")
                .alias("FullName", "Salary")
                .from("Actors")
                .where("Actors.FullName == \"Julia Roberts\" || FullName == \"Vin Diesel\"")
                .build()
                .run(broker);
        assertEquals(actorSelectProjAlias.getData().size(), 2);
        assertEquals(actorSelectProjAlias.getFieldNames(), new ArrayList<>(Arrays.asList("FullName", "Salary")));
        for(RelRow row: actorSelectProjAlias.getData()){
            assertEquals(row.getSize(), 2);
        }
        for(RelRow readRow: actorSelectProjAlias.getData()){
            boolean present = false;
            for(RelRow writtenRow: actorRows){
                if(readRow.getField(0).equals(writtenRow.getField(1)) &&
                        readRow.getField(1).equals(writtenRow.getField(3))){
                    present = true;
                }
            }
            assertTrue(present);
        }

        System.out.println("\tTEST ----- Selection and Projection on single table with aliases on fields and table");
        RelReadQueryResults actorSelectProjAliasTable = api.read()
                .select("Actors.FullName", "A.Salary")
                .alias("FullName", "Salary")
                .from("Actors", "A")
                .where("Actors.FullName == \"Julia Roberts\" || FullName == \"Vin Diesel\" || A.FullName == \"Keanu Reeves\"")
                .build()
                .run(broker);
        assertEquals(actorSelectProjAliasTable.getData().size(), 3);
        assertEquals(actorSelectProjAliasTable.getFieldNames(), new ArrayList<>(Arrays.asList("FullName", "Salary")));
        for(RelRow row: actorSelectProjAliasTable.getData()){
            assertEquals(row.getSize(), 2);
        }
        for(RelRow readRow: actorSelectProjAliasTable.getData()){
            boolean present = false;
            for(RelRow writtenRow: actorRows){
                if(readRow.getField(0).equals(writtenRow.getField(1)) &&
                        readRow.getField(1).equals(writtenRow.getField(3))){
                    present = true;
                }
            }
            assertTrue(present);
        }

        System.out.println("\tTEST ----- Join");
        RelReadQueryResults joinResults = api.read()
                .select("A.FullName", "F.Director", "F.ID")
                .alias("ActorName", "DirectorName", "")
                .from("Actors", "A")
                .from("Films", "F")
                .where("A.FilmID == F.ID && DirectorName == \"Christopher Nolan\"")
                .build()
                .run(broker);
        int joinSize = 0;
        for(RelRow actorWrittenRow: actorRows){
            for(RelRow filmWrittenRow: filmRows){
                if(actorWrittenRow.getField(4).equals(filmWrittenRow.getField(0)) &&
                        filmWrittenRow.getField(1).equals("Christopher Nolan")){
                    joinSize++;
                    boolean isContainedInResults = false;
                    for(RelRow joinRow: joinResults.getData()){
                        if(joinRow.getField(0).equals(actorWrittenRow.getField(1)) &&
                                joinRow.getField(1).equals(filmWrittenRow.getField(1)) &&
                                joinRow.getField(2).equals(filmWrittenRow.getField(0))
                        ){
                            isContainedInResults = true;
                        }
                    }
                    assertTrue(isContainedInResults);
                }
            }
        }
        for(RelRow resRow: joinResults.getData()){
            assertEquals(resRow.getSize(), 3);
        }
        assertEquals(joinSize, joinResults.getData().size());
        assertEquals(joinResults.getFieldNames(), Arrays.asList("ActorName", "DirectorName", "F.ID"));

        System.out.println("\tTEST ----- Distinct");
        RelRow newRow = new RelRow(123, "Quentin Tarantino", 80000000);
        filmRows.add(newRow);
        api.write().data(newRow).table("Films").build().run();
        RelReadQueryResults distinctResults = api.read().
                select("F.Director", "F.Budget").
                from("Films", "F").
                distinct()
                .build().run(broker);
        for(RelRow readRow: distinctResults.getData()){
            assertEquals(2, (int) readRow.getSize());
            boolean present = false;
            for(RelRow writtenRow: filmRows){
                boolean match = true;
                if(!(writtenRow.getField(1).equals(readRow.getField(0)) &&
                        writtenRow.getField(2).equals(readRow.getField(1)))){
                    match = false;
                }
                if(match){
                    present = true;
                }
            }
            assertTrue(present);
        }
        assertEquals(distinctResults.getData().size(), filmRows.size()-1);
        assertEquals(distinctResults.getFieldNames(), Arrays.asList("F.Director", "F.Budget"));

        System.out.println("\tTEST ----- Write Subqueries results");
        api.createTable("NolanEntries").attributes("ID", "Director", "Budget").keys("ID").build().run();
        api.write().table("NolanEntries").data(
                api.read()
                        .select()
                        .from("Films")
                        .where("Films.Director == \"Christopher Nolan\"")
                        .build()
        ).build().run();
        RelReadQueryResults writeSubqueryResults = api.read().select().from("NolanEntries").build().run(broker);
        assertEquals(writeSubqueryResults.getFieldNames(), Arrays.asList("NolanEntries.ID", "NolanEntries.Director", "NolanEntries.Budget"));
        assertEquals(writeSubqueryResults.getData().size(), 1);
        for(RelRow readRow: writeSubqueryResults.getData()){
            assertEquals(readRow.getField(1), "Christopher Nolan");
        }
        for(RelRow writtenRow: filmRows){
            if(writtenRow.getField(1).equals("Christopher Nolan")){
                boolean present = false;
                for(RelRow readRow: writeSubqueryResults.getData()){
                    assertEquals(readRow.getSize(), 3);
                    boolean equal = true;
                    if(!(readRow.getField(0).equals(writtenRow.getField(0)) && readRow.getField(1).equals(writtenRow.getField(1)) && readRow.getField(2).equals(writtenRow.getField(2)))){
                        equal = false;
                    }
                    if(equal){
                        present = true;
                    }
                }
                assertTrue(present);
            }
        }

        System.out.println("\tTEST ----- Overwrite");
        RelRow updatedRow = new RelRow(3, "Martin Scorsese", 220000000);
        api.write().data(updatedRow).table("Films").build().run();
        RelReadQueryResults newValue = api.read().from("Films").where("Films.ID == 3").build().run(broker);
        assertEquals(newValue.getData().size(), 1);
        assertEquals(newValue.getData().get(0).getField(2), updatedRow.getField(2));

        broker.shutdown();
        coordinator.stopServing();
        for(DataStore dataStore:dataStores) {
            dataStore.shutDown();
        }
        try {
            for(LocalDataStoreCloud ldsc: ldscList) {
                ldsc.clear();
            }
        } catch (Exception e) {
            ;
        }
    }
    @Test
    public void nestedQueriesTests(){
        Coordinator coordinator = new Coordinator(
                null,
                new DefaultLoadBalancer(),
                new DefaultAutoScaler(),
                zkHost, zkPort,
                "127.0.0.1", 7777);
        coordinator.runLoadBalancerDaemon = false;
        coordinator.startServing();

        int numServers = 2;
        List<LocalDataStoreCloud> ldscList = new ArrayList<>();
        List<DataStore> dataStores = new ArrayList<>();
        for(int i = 0; i<numServers; i++) {
            ldscList.add(new LocalDataStoreCloud());
            DataStore<RelRow, RelShard> dataStore = new DataStore<>(ldscList.get(i),
                    new RelShardFactory(),
                    Path.of("/var/tmp/RelUniserve"),
                    zkHost, zkPort,
                    "127.0.0.1", 8000 + i,
                    -1,
                    false
            );
            dataStore.startServing();
            dataStores.add(dataStore);
        }
/*
        LocalDataStoreCloud cloud = new LocalDataStoreCloud();
        DataStore<RelRow, RelShard> dataStore = new DataStore<>(
                cloud,
                new RelShardFactory(),
                Path.of("/var/tmp/RelUniserve"),
                zkHost, zkPort,
                "127.0.0.1", 8000,
                -1,
                false
        );
        dataStore.startServing();
*/
        Broker broker = new Broker(zkHost, zkPort);

        List<RelRow> coursesRows = new ArrayList<>();
        List<RelRow> examsRows = new ArrayList<>();
        List<RelRow> studentsRows = new ArrayList<>();
        List<RelRow> professorsRows = new ArrayList<>();

        String coursesFilePath = "/home/vsz/Scrivania/Uniserve/src/test/java/edu/stanford/futuredata/uniserve/rel/university/courses.txt";
        String examsFilePath = "/home/vsz/Scrivania/Uniserve/src/test/java/edu/stanford/futuredata/uniserve/rel/university/exams.txt";
        String professorsFilePath = "/home/vsz/Scrivania/Uniserve/src/test/java/edu/stanford/futuredata/uniserve/rel/university/professors.txt";
        String studentsFilePath = "/home/vsz/Scrivania/Uniserve/src/test/java/edu/stanford/futuredata/uniserve/rel/university/students.txt";


        int coursesCount = 0, examsCount = 0, professorsCount = 0, studentsCount = 0;
        try (BufferedReader br = new BufferedReader(new FileReader(coursesFilePath))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] parts = line.split(",");
                if (parts.length == 6) {
                    coursesRows.add(
                            new RelRow(
                                    parts[0],
                                    parts[1],
                                    parts[2],
                                    Integer.valueOf(parts[3]),
                                    Integer.valueOf(parts[4]),
                                    Integer.valueOf(parts[5])
                                    ));
                    coursesCount++;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        try (BufferedReader br = new BufferedReader(new FileReader(examsFilePath))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] parts = line.split(",");
                if (parts.length == 3) {
                    examsRows.add(new RelRow(
                            parts[0],
                            Integer.valueOf(parts[1]),
                            Integer.valueOf(parts[2])
                    ));
                    examsCount++;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        try (BufferedReader br = new BufferedReader(new FileReader(professorsFilePath))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] parts = line.split(",");
                if (parts.length == 6) {
                    professorsRows.add(new RelRow(
                        Integer.valueOf(parts[0]),
                        parts[1],
                        parts[2],
                        parts[3],
                        parts[4],
                        Integer.valueOf(parts[5])
                    ));
                    professorsCount++;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        try (BufferedReader br = new BufferedReader(new FileReader(studentsFilePath))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] parts = line.split(",");
                if (parts.length == 5) {
                    studentsRows.add(new RelRow(
                        Integer.valueOf(parts[0]),
                        parts[1],
                        parts[2],
                        parts[3],
                        parts[4]
                    ));
                    studentsCount++;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        API api = new API(broker);
        List<String> studentsSchema = List.of("ID", "Name", "Surname", "Address", "City");
        List<String> professorsSchema = List.of("ID", "Name", "Surname", "City", "PhoneNumber", "Salary");
        List<String> coursesSchema = List.of("Code", "Name", "Faculty", "CFUs", "ProfessorID", "StudentsCount");
        List<String> examsSchema = List.of("CourseCode", "StudentID", "Grade");

        System.out.println("TEST   -----   Creating tables");
        api.createTable("Students")
                .attributes(studentsSchema.toArray(new String[0]))
                .keys("ID")
                .build().run();
        api.createTable("Professors")
                .attributes(professorsSchema.toArray(new String[0]))
                .keys("ID")
                .build().run();
        api.createTable("Courses")
                .attributes(coursesSchema.toArray(new String[0]))
                .keys("Code")
                .build().run();
        api.createTable("Exams")
                .attributes(examsSchema.toArray(new String[0]))
                .keys("CourseCode", "StudentID")
                .build().run();

        System.out.println("TEST   -----   Writing tables");
        System.out.println("\tTEST   -----   Writing students");
        api.write().table("Students").data(studentsRows).build().run();
        System.out.println("\tTEST   -----   Writing professors");
        api.write().table("Professors").data(professorsRows).build().run();
        System.out.println("\tTEST   -----   Writing courses");
        api.write().table("Courses").data(coursesRows).build().run();
        System.out.println("\tTEST   -----   Writing exams");
        api.write().table("Exams").data(examsRows).build().run();

        System.out.println("----- NESTED QUERIES TESTING -----");

        System.out.println("\tTEST ----- max CFUs");
        long tStart = System.currentTimeMillis();
        RelReadQueryResults maxCFUs = api.read()
                .select("C.Code", "C.Name")
                .from("Courses", "C")
                .from(api.read()
                        .select()
                        .max("C1.CFUs", "maxCFU")
                        .from("Courses", "C1")
                        .build(), "M")
                .where("C.CFUs == M.maxCFU")
                .build().run(broker);
        System.out.println("\t\tExecution time: " + (System.currentTimeMillis() - tStart)+"ms.");
        int maxCFUvalue = Integer.MIN_VALUE;
        List<RelRow> writtenMaxCFU = new ArrayList<>();
        for(RelRow writtenRow: coursesRows){
            maxCFUvalue = Integer.max(maxCFUvalue, (int) writtenRow.getField(3));
        }
        for(RelRow writtenRow: coursesRows){
            if((int)writtenRow.getField(3) == maxCFUvalue){
                writtenMaxCFU.add(writtenRow);
            }
        }
        for(RelRow readRow: maxCFUs.getData()){
            boolean contains = false;
            for(RelRow writtenMax: writtenMaxCFU){
                if(writtenMax.getField(0).equals(readRow.getField(0)) &&
                        writtenMax.getField(1).equals(readRow.getField(1))
                ){
                    contains = true;
                    assertEquals((int) writtenMax.getField(3), maxCFUvalue);
                }
            }
            assertTrue(contains);;
        }
        assertEquals(maxCFUs.getFieldNames(), List.of("C.Code", "C.Name"));
        assertEquals(writtenMaxCFU.size(), maxCFUs.getData().size());

        System.out.println("\tTEST ----- not min CFUs");
        tStart = System.currentTimeMillis();
        RelReadQueryResults notMinCFUs = api.read()
                .select("C.Code", "C.Name")
                .from("Courses", "C")
                .from(api.read()
                        .select()
                        .min("C1.CFUs", "minCFU")
                        .from("Courses", "C1")
                        .build(), "M")
                .where("C.CFUs > M.minCFU")
                .build().run(broker);
        System.out.println("\t\tExecution time: " + (System.currentTimeMillis() - tStart)+"ms.");
        int minCFUs = Integer.MAX_VALUE;
        List<RelRow> writtenNotMinCFUs = new ArrayList<>();
        for(RelRow writtenRow: coursesRows){
            minCFUs = Integer.min(minCFUs, (int) writtenRow.getField(3));
        }
        for(RelRow writtenRow: coursesRows){
            if((int)writtenRow.getField(3) != minCFUs){
                writtenNotMinCFUs.add(writtenRow);
            }
        }
        for(RelRow writtenRow: writtenNotMinCFUs){
            boolean present = false;
            for(RelRow readRow: notMinCFUs.getData()){
                if(readRow.getField(0).equals(writtenRow.getField(0)) && readRow.getField(1).equals(writtenRow.getField(1))){
                    present = true;
                }
            }
            assertTrue(present);
        }
        assertEquals(writtenNotMinCFUs.size(), notMinCFUs.getData().size());
        assertEquals(notMinCFUs.getFieldNames(), List.of("C.Code", "C.Name"));

        System.out.println("\tTEST ----- Students who took at least 20");
        tStart = System.currentTimeMillis();
        RelReadQueryResults atLeast20 = api.read()
                .select("E20.CourseCode", "S.ID", "S.Name", "S.Surname")
                .alias("CourseCode", "StudentID", "Name","Surname")
                .from(api.read()
                        .select("E.CourseCode", "E.StudentID", "E.Grade")
                        .alias("CourseCode", "StudentID", "Grade")
                        .from("Exams", "E")
                        .where("Exams.Grade >= 20").build(), "E20")
                .from("Students", "S")
                .where("S.ID == E20.StudentID")
                .build().run(broker);
        System.out.println("\t\tExecution time: " + (System.currentTimeMillis() - tStart)+"ms.");
        int matchesSize = 0;
        for(RelRow writtenExamRow: examsRows){
            if((int) writtenExamRow.getField(2) >= 20){
                matchesSize++;
                for(RelRow writtenStudent: studentsRows){
                    if(writtenExamRow.getField(1).equals(writtenStudent.getField(0))){
                        boolean present = false;
                        for(RelRow readRow: atLeast20.getData()){
                            if(readRow.getField(0).equals(writtenExamRow.getField(0)) &&
                                    readRow.getField(1).equals(writtenStudent.getField(0)) &&
                                    readRow.getField(3).equals(writtenStudent.getField(2)) &&
                                    readRow.getField(2).equals(writtenStudent.getField(1))){
                                present = true;
                            }
                        }
                        assertTrue(present);
                    }
                }
            }
        }
        assertEquals(atLeast20.getData().size(), matchesSize);

        broker.shutdown();
        for(DataStore dataStore: dataStores) {
            dataStore.shutDown();
        }
        coordinator.stopServing();
        try {
            for(LocalDataStoreCloud cloud: ldscList) {
                cloud.clear();
            }
        }catch (Exception e){
            ;
        }
    }
    @Test
    public void aggregateTest() {
        System.out.println("Starting aggregate test components");
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
        String actorFilePath = "/home/vsz/Scrivania/Uniserve/src/test/java/edu/stanford/futuredata/uniserve/rel/actorfilms/ActorTestFile.txt";
        String filmFilePath = "/home/vsz/Scrivania/Uniserve/src/test/java/edu/stanford/futuredata/uniserve/rel/actorfilms/FilmTestFile.txt";
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
                    if (random < 0) {
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
        API api = new API(broker);
        System.out.println("Actors and Films table creation...");
        api.createTable("Actors").attributes("ID", "FullName", "DateOfBirth", "Salary", "FilmID").keys("ID").build().run();
        api.createTable("Films").attributes("ID", "Director", "Budget").keys("ID").build().run();
        System.out.println("Updating Actors and Films tables...");
        api.write().table("Actors").data(actorRows).build().run();
        api.write().table("Films").data(filmRows).build().run();
        System.out.println("Aggregating on Films' Budgets");
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
        System.out.println("Aggregating on multiple sources...");
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
        Map<String, Integer> sums = new HashMap<>();
        Map<String, Integer> counts = new HashMap<>();

        for (int i = 0; i < filmRows.size(); i++) {
            RelRow filmRow = filmRows.get(i);
            for (int j = 0; j < actorRows.size(); j++) {
                RelRow actorRow = actorRows.get(j);
                if (actorRow.getField(4).equals(filmRow.getField(0))) {
                    if (sums.containsKey((String) filmRow.getField(1))) {
                        sums.put((String) filmRow.getField(1), (sums.get((String) filmRow.getField(1)) + (Integer) actorRow.getField(3)));
                    } else {
                        sums.put((String) filmRow.getField(1), (Integer) actorRow.getField(3));
                    }
                    if (counts.containsKey((String) filmRow.getField(1))) {
                        counts.put((String) filmRow.getField(1), (sums.get((String) filmRow.getField(1)) + 1));
                    } else {
                        counts.put((String) filmRow.getField(1), 1);
                    }
                }
            }
        }
        for (RelRow resRow : totalActorEarnings.getData()) {
            String directorName = (String) resRow.getField(0);
            if (counts.containsKey(directorName) && counts.get(directorName) > 1) {
                assertEquals(sums.get(directorName), (Integer) resRow.getField(1));
            }
        }
        System.out.println("Testing group clause...");
        RelReadQueryResults totalActorEarningsGroup = api.read()
                .select("F.Director")
                .alias("DirectorName")
                .sum("A.Salary", "TotalActorsEarnings")
                .count("A.Salary", "NumFilms")
                .from("Actors", "A")
                .from("Films", "F")
                .where("A.FilmID == F.ID")
                .having("NumFilms > 1")
                .group("Films.Director")
                .build().run(broker);
        List<RelRow> groups = totalActorEarningsGroup.getData();
        for (RelRow row : groups) {
            boolean contained = false;
            for (RelRow row1 : totalActorEarnings.getData()) {
                boolean equal = true;
                for (int i = 0; i < row.getSize() && equal; i++) {
                    if (!row.getField(i).equals(row1.getField(i))) {
                        equal = false;
                    }
                }
                if (equal) {
                    contained = true;
                    break;
                }
            }
            assertTrue(contained);
        }
        coordinator.stopServing();
        dataStore.shutDown();
        try {
            ldsc.clear();
        } catch (Exception e) {
            ;
        }
    }
    //@Test
    public void multiServerTest(){
        Coordinator coordinator = new Coordinator(
                null,
                new DefaultLoadBalancer(),
                new DefaultAutoScaler(),
                zkHost, zkPort,
                "127.0.0.1", 7777);
        coordinator.runLoadBalancerDaemon = false;
        coordinator.startServing();

        int numServers = 10;

        List<LocalDataStoreCloud> ldscList = new ArrayList<>();
        List<DataStore> dataStores = new ArrayList<>();
        for(int i = 0; i<numServers; i++) {
            ldscList.add(new LocalDataStoreCloud());
            DataStore<RelRow, RelShard> dataStore = new DataStore<>(ldscList.get(i),
                    new RelShardFactory(),
                    Path.of("/var/tmp/RelUniserve"),
                    zkHost, zkPort,
                    "127.0.0.1", 8000 + i,
                    -1,
                    false
            );
            dataStore.startServing();
            dataStores.add(dataStore);
        }
        Broker broker = new Broker(zkHost, zkPort);
        List<RelRow> actorRows = new ArrayList<>();
        List<RelRow> filmRows = new ArrayList<>();
        String actorFilePath = "/home/vsz/Scrivania/Uniserve/src/test/java/edu/stanford/futuredata/uniserve/rel/actorfilms/ActorTestFile.txt";
        String filmFilePath = "/home/vsz/Scrivania/Uniserve/src/test/java/edu/stanford/futuredata/uniserve/rel/actorfilms/FilmTestFile.txt";
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
                }
            }
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
                    if (random < 0) {
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
        API api = new API(broker);

        api.createTable("Actors").attributes("ID", "FullName", "DateOfBirth", "Salary", "FilmID").shardNumber(20).keys("ID").build().run();
        api.createTable("Films").attributes("ID", "Director", "Budget").keys("ID").shardNumber(20).build().run();
        api.write().table("Actors").data(actorRows).build().run();
        api.write().table("Films").data(filmRows).build().run();

        System.out.println("----- BASIC QUERIES TESTING -----");

        System.out.println("\tTEST ----- Select All Actors, single table query");
        RelReadQueryResults allActors = api.read()
                .select()
                .from("Actors")
                .build().run(broker);
        assertEquals(allActors.getData().size(), count);
        assertEquals(allActors.getFieldNames(), new ArrayList<>(Arrays.asList("Actors.ID", "Actors.FullName", "Actors.DateOfBirth", "Actors.Salary", "Actors.FilmID")));
        for(RelRow writtenRow: actorRows){
            boolean present = false;
            for(RelRow readRow: allActors.getData()){
                boolean match = true;
                for(int i = 0; i<readRow.getSize() && match; i++){
                    match = readRow.getField(i).equals(writtenRow.getField(i));
                }
                if(match){
                    present = true;
                }
            }
            assertTrue(present);
        }

        System.out.println("\tTEST ----- Select All Films, single table query");
        RelReadQueryResults allFilms = api.read()
                .select()
                .from("Films")
                .build().run(broker);
        assertEquals(allFilms.getData().size(), filmCount);
        assertEquals(allFilms.getFieldNames(), new ArrayList<>(Arrays.asList("Films.ID", "Films.Director", "Films.Budget")));
        for(RelRow writtenRow: filmRows){
            boolean present = false;
            for(RelRow readRow: allFilms.getData()){
                boolean match = true;
                for(int i = 0; i<readRow.getSize() && match; i++){
                    match = readRow.getField(i).equals(writtenRow.getField(i));
                }
                if(match){
                    present = true;
                }
            }
            assertTrue(present);
        }

        System.out.println("\tTEST ----- Projection on single table");
        RelReadQueryResults actorSimpleProj = api.read()
                .select("Actors.FullName", "Actors.Salary")
                .from("Actors")
                .build()
                .run(broker);
        assertEquals(actorSimpleProj.getData().size(), count);
        assertEquals(actorSimpleProj.getFieldNames(), new ArrayList<>(Arrays.asList("Actors.FullName", "Actors.Salary")));
        for(RelRow row: actorSimpleProj.getData()){
            assertEquals(row.getSize(), 2);
        }
        for(RelRow readRow: actorSimpleProj.getData()){
            boolean present = false;
            for(RelRow writtenRow: actorRows){
                if(readRow.getField(0).equals(writtenRow.getField(1)) &&
                        readRow.getField(1).equals(writtenRow.getField(3))){
                    present = true;
                }
            }
            assertTrue(present);
        }

        System.out.println("\tTEST ----- Projection on single table with aliases on fields");
        RelReadQueryResults actorSimpleProjAlias = api.read()
                .select("Actors.FullName", "Actors.Salary")
                .alias("FullName", "Salary")
                .from("Actors")
                .build()
                .run(broker);
        assertEquals(actorSimpleProjAlias.getData().size(), count);
        assertEquals(actorSimpleProjAlias.getFieldNames(), new ArrayList<>(Arrays.asList("FullName", "Salary")));
        for(RelRow row: actorSimpleProjAlias.getData()){
            assertEquals(row.getSize(), 2);
        }
        for(RelRow readRow: actorSimpleProjAlias.getData()){
            boolean present = false;
            for(RelRow writtenRow: actorRows){
                if(readRow.getField(0).equals(writtenRow.getField(1)) &&
                        readRow.getField(1).equals(writtenRow.getField(3))){
                    present = true;
                }
            }
            assertTrue(present);
        }

        System.out.println("\tTEST ----- Projection on single table with aliases on fields and table");
        RelReadQueryResults actorSimpleProjAliasTable = api.read()
                .select("Actors.FullName", "A.Salary")
                .alias("FullName", "Salary")
                .from("Actors", "A")
                .build()
                .run(broker);
        assertEquals(actorSimpleProjAliasTable.getData().size(), count);
        assertEquals(actorSimpleProjAliasTable.getFieldNames(), new ArrayList<>(Arrays.asList("FullName", "Salary")));
        for(RelRow row: actorSimpleProjAliasTable.getData()){
            assertEquals(row.getSize(), 2);
        }
        for(RelRow readRow: actorSimpleProjAliasTable.getData()){
            boolean present = false;
            for(RelRow writtenRow: actorRows){
                if(readRow.getField(0).equals(writtenRow.getField(1)) &&
                        readRow.getField(1).equals(writtenRow.getField(3))){
                    present = true;
                }
            }
            assertTrue(present);
        }

        System.out.println("\tTEST ----- Cartesian product");
        RelReadQueryResults cartesianProduct = api.read()
                .select()
                .from("Actors")
                .from("Films")
                .build()
                .run(broker);
        assertEquals(cartesianProduct.getData().size(), count*filmCount);
        assertEquals(cartesianProduct.getFieldNames(), new ArrayList<>(
                Arrays.asList("Films.ID", "Films.Director", "Films.Budget","Actors.ID", "Actors.FullName", "Actors.DateOfBirth", "Actors.Salary", "Actors.FilmID")));
        for(RelRow row: cartesianProduct.getData()){
            assertEquals(row.getSize(), 8);
        }
        for(RelRow readRow: cartesianProduct.getData()){
            boolean presentActor = false;
            for(RelRow writtenRow: actorRows){
                if(readRow.getField(3).equals(writtenRow.getField(0)) &&
                        readRow.getField(4).equals(writtenRow.getField(1)) &&
                        readRow.getField(5).equals(writtenRow.getField(2)) &&
                        readRow.getField(6).equals(writtenRow.getField(3)) &&
                        readRow.getField(7).equals(writtenRow.getField(4))
                ){
                    presentActor = true;
                }
            }
            assertTrue(presentActor);
            boolean presentFilm = false;
            for(RelRow writtenRow: filmRows){
                if(readRow.getField(0).equals(writtenRow.getField(0)) &&
                        readRow.getField(1).equals(writtenRow.getField(1)) &&
                        readRow.getField(2).equals(writtenRow.getField(2))
                ){
                    presentFilm = true;
                }
            }
            assertTrue(presentFilm);
        }

        System.out.println("----- PREDICATE TESTING -----");

        System.out.println("\tTEST ----- Select on Single table, single field");
        RelReadQueryResults selectActors = api.read()
                .select()
                .from("Actors")
                .where("Actors.FullName == \"Johnny Depp\" || Actors.FullName == \"Brad Pitt\"")
                .build().run(broker);
        assertEquals(selectActors.getData().size(), 2);
        assertEquals(selectActors.getFieldNames(), new ArrayList<>(Arrays.asList("Actors.ID", "Actors.FullName", "Actors.DateOfBirth", "Actors.Salary", "Actors.FilmID")));
        for(RelRow readRow: selectActors.getData()){
            boolean present = false;
            for(RelRow writtenRow: actorRows){
                boolean match = true;
                for(int i = 0; i<readRow.getSize() && match; i++){
                    match = readRow.getField(i).equals(writtenRow.getField(i));
                }
                if(match){
                    present = true;
                }
            }
            assertTrue(present);
        }

        System.out.println("\tTEST ----- Select on Single Table, multiple fields");
        RelReadQueryResults selectFilms = api.read()
                .select()
                .from("Films")
                .where("Films.ID > 5 && Films.Budget > 50000000")
                .build().run(broker);
        int selectFilmsRowCount = 0;
        for(RelRow writtenRow: filmRows){
            if((int) writtenRow.getField(0) > 5 && (int) writtenRow.getField(2) > 50000000){
                selectFilmsRowCount++;
                boolean contain = false;
                for(RelRow readRow: selectFilms.getData()){
                    if(readRow.getField(0).equals(writtenRow.getField(0)) &&
                            readRow.getField(1).equals(writtenRow.getField(1)) &&
                            readRow.getField(2).equals(writtenRow.getField(2))
                    ){
                        contain = true;
                    }
                }
                assertTrue(contain);
            }
        }
        assertEquals(selectFilms.getData().size(), selectFilmsRowCount);
        assertEquals(selectFilms.getFieldNames(), new ArrayList<>(Arrays.asList("Films.ID", "Films.Director", "Films.Budget")));


        System.out.println("\tTEST ----- Selection and Projection on single table");
        RelReadQueryResults actorSelProj = api.read()
                .select("Actors.FullName", "Actors.Salary")
                .from("Actors")
                .where("Actors.FullName == \"Julia Roberts\"")
                .build()
                .run(broker);
        assertEquals(actorSelProj.getData().size(), 1);
        assertEquals(actorSelProj.getFieldNames(), new ArrayList<>(Arrays.asList("Actors.FullName", "Actors.Salary")));
        for(RelRow row: actorSimpleProj.getData()){
            assertEquals(row.getSize(), 2);
        }
        for(RelRow readRow: actorSelProj.getData()){
            boolean present = false;
            for(RelRow writtenRow: actorRows){
                if(readRow.getField(0).equals(writtenRow.getField(1)) &&
                        readRow.getField(1).equals(writtenRow.getField(3))){
                    present = true;
                }
            }
            assertTrue(present);
        }



        System.out.println("\tTEST ----- Selection and Projection on single table with aliases on fields");
        RelReadQueryResults actorSelectProjAlias = api.read()
                .select("Actors.FullName", "Actors.Salary")
                .alias("FullName", "Salary")
                .from("Actors")
                .where("Actors.FullName == \"Julia Roberts\" || FullName == \"Vin Diesel\"")
                .build()
                .run(broker);
        assertEquals(actorSelectProjAlias.getData().size(), 2);
        assertEquals(actorSelectProjAlias.getFieldNames(), new ArrayList<>(Arrays.asList("FullName", "Salary")));
        for(RelRow row: actorSelectProjAlias.getData()){
            assertEquals(row.getSize(), 2);
        }
        for(RelRow readRow: actorSelectProjAlias.getData()){
            boolean present = false;
            for(RelRow writtenRow: actorRows){
                if(readRow.getField(0).equals(writtenRow.getField(1)) &&
                        readRow.getField(1).equals(writtenRow.getField(3))){
                    present = true;
                }
            }
            assertTrue(present);
        }

        System.out.println("\tTEST ----- Selection and Projection on single table with aliases on fields and table");
        RelReadQueryResults actorSelectProjAliasTable = api.read()
                .select("Actors.FullName", "A.Salary")
                .alias("FullName", "Salary")
                .from("Actors", "A")
                .where("Actors.FullName == \"Julia Roberts\" || FullName == \"Vin Diesel\" || A.FullName == \"Keanu Reeves\"")
                .build()
                .run(broker);
        assertEquals(actorSelectProjAliasTable.getData().size(), 3);
        assertEquals(actorSelectProjAliasTable.getFieldNames(), new ArrayList<>(Arrays.asList("FullName", "Salary")));
        for(RelRow row: actorSelectProjAliasTable.getData()){
            assertEquals(row.getSize(), 2);
        }
        for(RelRow readRow: actorSelectProjAliasTable.getData()){
            boolean present = false;
            for(RelRow writtenRow: actorRows){
                if(readRow.getField(0).equals(writtenRow.getField(1)) &&
                        readRow.getField(1).equals(writtenRow.getField(3))){
                    present = true;
                }
            }
            assertTrue(present);
        }

        System.out.println("\tTEST ----- Join");
        RelReadQueryResults joinResults = api.read()
                .select("A.FullName", "F.Director", "F.ID")
                .alias("ActorName", "DirectorName", "")
                .from("Actors", "A")
                .from("Films", "F")
                .where("A.FilmID == F.ID && DirectorName == \"Christopher Nolan\"")
                .build()
                .run(broker);
        int joinSize = 0;
        for(RelRow actorWrittenRow: actorRows){
            for(RelRow filmWrittenRow: filmRows){
                if(actorWrittenRow.getField(4).equals(filmWrittenRow.getField(0)) &&
                        filmWrittenRow.getField(1).equals("Christopher Nolan")){
                    joinSize++;
                    boolean isContainedInResults = false;
                    for(RelRow joinRow: joinResults.getData()){
                        if(joinRow.getField(0).equals(actorWrittenRow.getField(1)) &&
                                joinRow.getField(1).equals(filmWrittenRow.getField(1)) &&
                                joinRow.getField(2).equals(filmWrittenRow.getField(0))
                        ){
                            isContainedInResults = true;
                        }
                    }
                    assertTrue(isContainedInResults);
                }
            }
        }
        for(RelRow resRow: joinResults.getData()){
            assertEquals(resRow.getSize(), 3);
        }
        assertEquals(joinSize, joinResults.getData().size());
        assertEquals(joinResults.getFieldNames(), Arrays.asList("ActorName", "DirectorName", "F.ID"));

        System.out.println("\tTEST ----- Distinct");
        RelRow newRow = new RelRow(123, "Quentin Tarantino", 80000000);
        filmRows.add(newRow);
        api.write().data(newRow).table("Films").build().run();
        RelReadQueryResults distinctResults = api.read().
                select("F.Director", "F.Budget").
                from("Films", "F").
                distinct()
                .build().run(broker);
        for(RelRow readRow: distinctResults.getData()){
            assertEquals(2, (int) readRow.getSize());
            boolean present = false;
            for(RelRow writtenRow: filmRows){
                boolean match = true;
                if(!(writtenRow.getField(1).equals(readRow.getField(0)) &&
                        writtenRow.getField(2).equals(readRow.getField(1)))){
                    match = false;
                }
                if(match){
                    present = true;
                }
            }
            assertTrue(present);
        }
        assertEquals(distinctResults.getData().size(), filmRows.size()-1);
        assertEquals(distinctResults.getFieldNames(), Arrays.asList("F.Director", "F.Budget"));

        System.out.println("\tTEST ----- Write Subqueries results");
        api.createTable("NolanEntries").attributes("ID", "Director", "Budget").keys("ID").build().run();
        api.write().table("NolanEntries").data(
                api.read()
                        .select()
                        .from("Films")
                        .where("Films.Director == \"Christopher Nolan\"")
                        .build()
        ).build().run();
        RelReadQueryResults writeSubqueryResults = api.read().select().from("NolanEntries").build().run(broker);
        assertEquals(writeSubqueryResults.getFieldNames(), Arrays.asList("NolanEntries.ID", "NolanEntries.Director", "NolanEntries.Budget"));
        assertEquals(writeSubqueryResults.getData().size(), 1);
        for(RelRow readRow: writeSubqueryResults.getData()){
            assertEquals(readRow.getField(1), "Christopher Nolan");
        }
        for(RelRow writtenRow: filmRows){
            if(writtenRow.getField(1).equals("Christopher Nolan")){
                boolean present = false;
                for(RelRow readRow: writeSubqueryResults.getData()){
                    assertEquals(readRow.getSize(), 3);
                    boolean equal = true;
                    if(!(readRow.getField(0).equals(writtenRow.getField(0)) && readRow.getField(1).equals(writtenRow.getField(1)) && readRow.getField(2).equals(writtenRow.getField(2)))){
                        equal = false;
                    }
                    if(equal){
                        present = true;
                    }
                }
                assertTrue(present);
            }
        }

        System.out.println("\tTEST ----- Overwrite");
        RelRow updatedRow = new RelRow(3, "Martin Scorsese", 220000000);
        api.write().data(updatedRow).table("Films").build().run();
        RelReadQueryResults newValue = api.read().from("Films").where("Films.ID == 3").build().run(broker);
        assertEquals(newValue.getData().size(), 1);
        assertEquals(newValue.getData().get(0).getField(2), updatedRow.getField(2));

        List<RelRow> coursesRows = new ArrayList<>();
        List<RelRow> examsRows = new ArrayList<>();
        List<RelRow> studentsRows = new ArrayList<>();
        List<RelRow> professorsRows = new ArrayList<>();

        String coursesFilePath = "/home/vsz/Scrivania/Uniserve/src/test/java/edu/stanford/futuredata/uniserve/rel/university/courses.txt";
        String examsFilePath = "/home/vsz/Scrivania/Uniserve/src/test/java/edu/stanford/futuredata/uniserve/rel/university/exams.txt";
        String professorsFilePath = "/home/vsz/Scrivania/Uniserve/src/test/java/edu/stanford/futuredata/uniserve/rel/university/professors.txt";
        String studentsFilePath = "/home/vsz/Scrivania/Uniserve/src/test/java/edu/stanford/futuredata/uniserve/rel/university/students.txt";


        int coursesCount = 0, examsCount = 0, professorsCount = 0, studentsCount = 0;
        try (BufferedReader br = new BufferedReader(new FileReader(coursesFilePath))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] parts = line.split(",");
                if (parts.length == 6) {
                    coursesRows.add(
                            new RelRow(
                                    parts[0],
                                    parts[1],
                                    parts[2],
                                    Integer.valueOf(parts[3]),
                                    Integer.valueOf(parts[4]),
                                    Integer.valueOf(parts[5])
                            ));
                    coursesCount++;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        try (BufferedReader br = new BufferedReader(new FileReader(examsFilePath))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] parts = line.split(",");
                if (parts.length == 3) {
                    examsRows.add(new RelRow(
                            parts[0],
                            Integer.valueOf(parts[1]),
                            Integer.valueOf(parts[2])
                    ));
                    examsCount++;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        try (BufferedReader br = new BufferedReader(new FileReader(professorsFilePath))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] parts = line.split(",");
                if (parts.length == 6) {
                    professorsRows.add(new RelRow(
                            Integer.valueOf(parts[0]),
                            parts[1],
                            parts[2],
                            parts[3],
                            parts[4],
                            Integer.valueOf(parts[5])
                    ));
                    professorsCount++;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        try (BufferedReader br = new BufferedReader(new FileReader(studentsFilePath))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] parts = line.split(",");
                if (parts.length == 5) {
                    studentsRows.add(new RelRow(
                            Integer.valueOf(parts[0]),
                            parts[1],
                            parts[2],
                            parts[3],
                            parts[4]
                    ));
                    studentsCount++;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        List<String> studentsSchema = List.of("ID", "Name", "Surname", "Address", "City");
        List<String> professorsSchema = List.of("ID", "Name", "Surname", "City", "PhoneNumber", "Salary");
        List<String> coursesSchema = List.of("Code", "Name", "Faculty", "CFUs", "ProfessorID", "StudentsCount");
        List<String> examsSchema = List.of("CourseCode", "StudentID", "Grade");

        System.out.println("TEST   -----   Creating tables");
        api.createTable("Students")
                .attributes(studentsSchema.toArray(new String[0]))
                .keys("ID")
                .build().run();
        api.createTable("Professors")
                .attributes(professorsSchema.toArray(new String[0]))
                .keys("ID")
                .build().run();
        api.createTable("Courses")
                .attributes(coursesSchema.toArray(new String[0]))
                .keys("Code")
                .build().run();
        api.createTable("Exams")
                .attributes(examsSchema.toArray(new String[0]))
                .keys("CourseCode", "StudentID")
                .build().run();

        System.out.println("TEST   -----   Writing tables");
        System.out.println("\tTEST   -----   Writing students");
        api.write().table("Students").data(studentsRows).build().run();
        System.out.println("\tTEST   -----   Writing professors");
        api.write().table("Professors").data(professorsRows).build().run();
        System.out.println("\tTEST   -----   Writing courses");
        api.write().table("Courses").data(coursesRows).build().run();
        System.out.println("\tTEST   -----   Writing exams");
        api.write().table("Exams").data(examsRows).build().run();

        System.out.println("----- NESTED QUERIES TESTING -----");

        System.out.println("\tTEST ----- max CFUs");
        long tStart = System.currentTimeMillis();
        RelReadQueryResults maxCFUs = api.read()
                .select("C.Code", "C.Name")
                .from("Courses", "C")
                .from(api.read()
                        .select()
                        .max("C1.CFUs", "maxCFU")
                        .from("Courses", "C1")
                        .build(), "M")
                .where("C.CFUs == M.maxCFU")
                .build().run(broker);
        System.out.println("\t\tExecution time: " + (System.currentTimeMillis() - tStart)+"ms.");
        int maxCFUvalue = Integer.MIN_VALUE;
        List<RelRow> writtenMaxCFU = new ArrayList<>();
        for(RelRow writtenRow: coursesRows){
            maxCFUvalue = Integer.max(maxCFUvalue, (int) writtenRow.getField(3));
        }
        for(RelRow writtenRow: coursesRows){
            if((int)writtenRow.getField(3) == maxCFUvalue){
                writtenMaxCFU.add(writtenRow);
            }
        }
        for(RelRow readRow: maxCFUs.getData()){
            boolean contains = false;
            for(RelRow writtenMax: writtenMaxCFU){
                if(writtenMax.getField(0).equals(readRow.getField(0)) &&
                        writtenMax.getField(1).equals(readRow.getField(1))
                ){
                    contains = true;
                    assertEquals((int) writtenMax.getField(3), maxCFUvalue);
                }
            }
            assertTrue(contains);;
        }
        assertEquals(maxCFUs.getFieldNames(), List.of("C.Code", "C.Name"));
        assertEquals(writtenMaxCFU.size(), maxCFUs.getData().size());

        System.out.println("\tTEST ----- not min CFUs");
        tStart = System.currentTimeMillis();
        RelReadQueryResults notMinCFUs = api.read()
                .select("C.Code", "C.Name")
                .from("Courses", "C")
                .from(api.read()
                        .select()
                        .min("C1.CFUs", "minCFU")
                        .from("Courses", "C1")
                        .build(), "M")
                .where("C.CFUs > M.minCFU")
                .build().run(broker);
        System.out.println("\t\tExecution time: " + (System.currentTimeMillis() - tStart)+"ms.");
        int minCFUs = Integer.MAX_VALUE;
        List<RelRow> writtenNotMinCFUs = new ArrayList<>();
        for(RelRow writtenRow: coursesRows){
            minCFUs = Integer.min(minCFUs, (int) writtenRow.getField(3));
        }
        for(RelRow writtenRow: coursesRows){
            if((int)writtenRow.getField(3) != minCFUs){
                writtenNotMinCFUs.add(writtenRow);
            }
        }
        for(RelRow writtenRow: writtenNotMinCFUs){
            boolean present = false;
            for(RelRow readRow: notMinCFUs.getData()){
                if(readRow.getField(0).equals(writtenRow.getField(0)) && readRow.getField(1).equals(writtenRow.getField(1))){
                    present = true;
                }
            }
            assertTrue(present);
        }
        assertEquals(writtenNotMinCFUs.size(), notMinCFUs.getData().size());
        assertEquals(notMinCFUs.getFieldNames(), List.of("C.Code", "C.Name"));

        System.out.println("\tTEST ----- Students who took at least 20");
        tStart = System.currentTimeMillis();
        RelReadQueryResults atLeast20 = api.read()
                .select("E20.CourseCode", "S.ID", "S.Name", "S.Surname")
                .alias("CourseCode", "StudentID", "Name","Surname")
                .from(api.read()
                        .select("E.CourseCode", "E.StudentID", "E.Grade")
                        .alias("CourseCode", "StudentID", "Grade")
                        .from("Exams", "E")
                        .where("Exams.Grade >= 20").build(), "E20")
                .from("Students", "S")
                .where("S.ID == E20.StudentID")
                .build().run(broker);
        System.out.println("\t\tExecution time: " + (System.currentTimeMillis() - tStart)+"ms.");
        int matchesSize = 0;
        for(RelRow writtenExamRow: examsRows){
            if((int) writtenExamRow.getField(2) >= 20){
                matchesSize++;
                for(RelRow writtenStudent: studentsRows){
                    if(writtenExamRow.getField(1).equals(writtenStudent.getField(0))){
                        boolean present = false;
                        for(RelRow readRow: atLeast20.getData()){
                            if(readRow.getField(0).equals(writtenExamRow.getField(0)) &&
                                    readRow.getField(1).equals(writtenStudent.getField(0)) &&
                                    readRow.getField(3).equals(writtenStudent.getField(2)) &&
                                    readRow.getField(2).equals(writtenStudent.getField(1))){
                                present = true;
                            }
                        }
                        assertTrue(present);
                    }
                }
            }
        }
        assertEquals(atLeast20.getData().size(), matchesSize);



        for(DataStore dataStore: dataStores)
            dataStore.shutDown();
        coordinator.stopServing();
        try {
            for(LocalDataStoreCloud ldsc: ldscList)
                ldsc.clear();
        } catch (Exception e) {
            ;
        }
    }

    @Test
    public void storedTest() {
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
        String actorFilePath = "/home/vsz/Scrivania/Uniserve/src/test/java/edu/stanford/futuredata/uniserve/rel/actorfilms/ActorTestFile.txt";
        String filmFilePath = "/home/vsz/Scrivania/Uniserve/src/test/java/edu/stanford/futuredata/uniserve/rel/actorfilms/FilmTestFile.txt";
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
                }
            }
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
                    if (random < 0) {
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
        API api = new API(broker);

        api.createTable("Actors").attributes("ID", "FullName", "DateOfBirth", "Salary", "FilmID").keys("ID").build().run();
        api.createTable("Films").attributes("ID", "Director", "Budget").keys("ID").build().run();

        System.out.println("Initializing table Actors");
        api.write().table("Actors").data(actorRows).build().run();
        System.out.println("Initializing table Films");
        api.write().table("Films").data(filmRows).build().run();

        System.out.println("Defining query to be stored");
        ReadQuery totalActorEarningsQuery = api.read()
                .select("F.Director")
                .alias("DirectorName")
                .sum("A.Salary", "TotalActorsEarnings")
                .count("A.Salary", "NumFilms")
                .from("Actors", "A")
                .from("Films", "F")
                .where("A.FilmID == F.ID")
                .having("NumFilms > 1")
                .group("Films.Director")
                .store()
                .build();
        System.out.println("Running query to be stored");
        RelReadQueryResults totActorEarningsResults = totalActorEarningsQuery.run(broker);
        assertEquals(broker.getTableInfo("Actors").getRegisteredQueries().get(0), totalActorEarningsQuery);
        System.out.println("Adding data to Actors table");
        api.write().table("Actors").data(
                new RelRow(500, "Clint Eastwood", "31/05/1930", 100000, 111),
                new RelRow(501, "Clint Eastwood", "31/05/1930", 100000, 112)
        ).build().run();
        System.out.println("Adding data to Films table");
        api.write().table("Films").data(
                new RelRow(111, "Sergio Leone", 1000000),
                new RelRow(112, "Sergio Leone", 1000000)
        ).build().run();

        try {
            sleep(5000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        RelReadQueryResults updTotActEarn = totalActorEarningsQuery.run(broker);
        assertTrue(totActorEarningsResults.getData().size() < updTotActEarn.getData().size());
        boolean updated = false;
        for(RelRow row: updTotActEarn.getData()){
            updated = row.getField(0).equals("Sergio Leone") || updated;
        }
        assertTrue(updated);

        ReadQuery subqStored = api.read()
                .select()
                .from(totalActorEarningsQuery, "ActorsEarnings")
                .where("ActorsEarnings.NumFilms > 2")
                .store()
                .build();
        RelReadQueryResults atLeastTwoFilms = subqStored.run(broker);
        assertEquals(broker.getTableInfo("Actors").getRegisteredQueries().get(1), subqStored);
        assertEquals(broker.getTableInfo("Films").getRegisteredQueries().get(1), subqStored);
        broker.shutdown();
        coordinator.stopServing();
        dataStore.shutDown();
        try {
            ldsc.clear();
        } catch (Exception e) {
            ;
        }
    }
}
