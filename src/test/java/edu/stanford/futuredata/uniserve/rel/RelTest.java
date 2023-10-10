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
import edu.stanford.futuredata.uniserve.relationalapi.SerializablePredicate;
import edu.stanford.futuredata.uniserve.utilities.TableInfo;
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
import static org.junit.jupiter.api.Assertions.*;

public class RelTest {
    private static final Logger logger = LoggerFactory.getLogger(RelTest.class);

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

    int numServers = 10;
    Coordinator coordinator = null;
    List<LocalDataStoreCloud> ldscList = new ArrayList<>();
    List<DataStore<RelRow, RelShard>> dataStores = new ArrayList<>();


    String filmFilePath = "C:\\Users\\saver\\Desktop\\Uniserve\\src\\test\\java\\edu\\stanford\\futuredata\\uniserve\\rel\\actorfilms\\FilmTestFile.txt";
    String actorFilePath = "C:\\Users\\saver\\Desktop\\Uniserve\\src\\test\\java\\edu\\stanford\\futuredata\\uniserve\\rel\\actorfilms\\ActorTestFile.txt";
    String coursesFilePath = "C:\\Users\\saver\\Desktop\\Uniserve\\src\\test\\java\\edu\\stanford\\futuredata\\uniserve\\rel\\university\\courses.txt";
    String examsFilePath = "C:\\Users\\saver\\Desktop\\Uniserve\\src\\test\\java\\edu\\stanford\\futuredata\\uniserve\\rel\\university\\exams.txt";
    String professorsFilePath = "C:\\Users\\saver\\Desktop\\Uniserve\\src\\test\\java\\edu\\stanford\\futuredata\\uniserve\\rel\\university\\professors.txt";
    String studentsFilePath = "C:\\Users\\saver\\Desktop\\Uniserve\\src\\test\\java\\edu\\stanford\\futuredata\\uniserve\\rel\\university\\students.txt";

    List<String> studentsSchema = List.of("ID", "Name", "Surname", "Address", "City");
    List<String> professorsSchema = List.of("ID", "Name", "Surname", "City", "PhoneNumber", "Salary");
    List<String> coursesSchema = List.of("Code", "Name", "Faculty", "CFUs", "ProfessorID", "StudentsCount");
    List<String> examsSchema = List.of("CourseCode", "StudentID", "Grade");
    List<String> actorsSchema = List.of("ID", "FullName", "DateOfBirth", "Salary", "FilmID");
    List<String> filmsSchema = List.of("ID", "Director", "Budget");

    private void startServers(){
        coordinator = new Coordinator(
                null,
                new DefaultLoadBalancer(),
                new DefaultAutoScaler(),
                zkHost, zkPort,
                "127.0.0.1", 7777);
        coordinator.runLoadBalancerDaemon = false;
        coordinator.startServing();

        ldscList = new ArrayList<>();
        dataStores = new ArrayList<>();
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
    }
    private void stopServers(){
        for(DataStore dataStore:dataStores) {
            dataStore.shutDown();
        }
        coordinator.stopServing();
        try {
            for(LocalDataStoreCloud ldsc: ldscList) {
                ldsc.clear();
            }
        } catch (Exception e) {
            ;
        }
    }

    int filmCount = 0;
    int actorCount = 0;
    int coursesCount = 0;
    int examsCount = 0;
    int professorsCount = 0;
    int studentsCount = 0;

    List<RelRow> filmRows = new ArrayList<>();
    List<RelRow> actorRows = new ArrayList<>();
    List<RelRow> coursesRows = new ArrayList<>();
    List<RelRow> examsRows = new ArrayList<>();
    List<RelRow> studentsRows = new ArrayList<>();
    List<RelRow> professorsRows = new ArrayList<>();

    private void loadActorsAndFilms(){

        if(filmCount == 0 || actorCount == 0) {
            filmCount = 0;
            actorCount = 0;
            filmRows.clear();
            actorRows.clear();
        }else{
            return;
        }


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
        try (BufferedReader br = new BufferedReader(new FileReader(actorFilePath))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] parts = line.split(",");
                if (parts.length == 3) {
                    actorRows.add(new RelRow(actorCount, parts[0], parts[1], Integer.valueOf(parts[2]), new Random().nextInt(filmCount)));
                    actorCount++;
                } else {
                    System.out.println("No match for " + line);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    private void loadUniversity(){

        if(coursesCount == 0 || examsCount == 0 || professorsCount == 0 || studentsCount == 0){
            coursesRows = new ArrayList<>();
            examsRows = new ArrayList<>();
            studentsRows = new ArrayList<>();
            professorsRows = new ArrayList<>();
            coursesCount = 0;
            examsCount = 0;
            professorsCount = 0;
            studentsCount = 0;
        }else{
            return;
        }

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
    }


    @Test
    public void operationTests(){
        startServers();
        Broker broker = new Broker(zkHost, zkPort);

        loadActorsAndFilms();


        API api = new API(broker);
        System.out.println("----- OPERATIONS TESTING -----");
        System.out.println("\tTEST ----- Creating tables");
        api.createTable("Actors").attributes(actorsSchema.toArray(new String[0])).shardNumber(20).keys("ID").build().run();
        api.createTable("Films").attributes(filmsSchema.toArray(new String[0])).keys("ID").shardNumber(20).build().run();
        System.out.println("\tTEST ----- Populating tables");
        api.write().table("Actors").data(actorRows).build().run();
        api.write().table("Films").data(filmRows).build().run();

        System.out.println("\tTEST ----- Doubling film budgets");
        SerializablePredicate doubling = o -> ((Integer) o)*2;
        RelReadQueryResults doubleBudgetResult = api.read().select().from("Films").apply(null, null, doubling).build().run(broker);
        for(RelRow writtenRow: filmRows){
            boolean present = false;
            for(RelRow readRow: doubleBudgetResult.getData()){
                if(readRow.getField(0).equals(readRow.getField(0)) &&
                        readRow.getField(1).equals(readRow.getField(1)) &&
                        ((Integer)readRow.getField(2)).equals(
                                ((Integer) writtenRow.getField(2))*2)
                ){
                    present=true;
                    break;
                }
            }
            assertTrue(present);
        }
        broker.shutdown();
        stopServers();
    }
    @Test
    public void noAggregatesNoNestingTest(){
        startServers();
        Broker broker = new Broker(zkHost, zkPort);

        loadActorsAndFilms();


        API api = new API(broker);
        System.out.println("----- BASIC QUERIES TESTING -----");
        System.out.println("\tTEST ----- Creating tables");
        api.createTable("Actors").attributes(actorsSchema.toArray(new String[0])).shardNumber(20).keys("ID").build().run();
        api.createTable("Films").attributes(filmsSchema.toArray(new String[0])).keys("ID").shardNumber(20).build().run();
        System.out.println("\tTEST ----- Populating tables");
        api.write().table("Actors").data(actorRows).build().run();
        api.write().table("Films").data(filmRows).build().run();


        System.out.println("\tTEST ----- Select All Actors, single table query");
        RelReadQueryResults allActors = api.read()
                .select()
                .from("Actors")
                .build().run(broker);
        assertEquals(allActors.getData().size(), actorCount);
        assertEquals(allActors.getFieldNames(), actorsSchema);
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
        System.out.println("\t\tTEST ----- Read all actors OK");


        System.out.println("\tTEST ----- Select All Films, single table query");
        RelReadQueryResults allFilms = api.read()
                .select()
                .from("Films")
                .build().run(broker);
        assertEquals(allFilms.getData().size(), filmCount);
        assertEquals(allFilms.getFieldNames(), filmsSchema);
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
        System.out.println("\t\tTEST ----- Read all films OK");


        System.out.println("\tTEST ----- Projection on single table");
        RelReadQueryResults actorSimpleProj = api.read()
                .select("FullName", "Salary")
                .from("Actors")
                .build()
                .run(broker);
        assertEquals(actorSimpleProj.getData().size(), actorCount);
        assertEquals(actorSimpleProj.getFieldNames(), new ArrayList<>(Arrays.asList("FullName", "Salary")));
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
        System.out.println("\t\tTEST ----- Projection on all actors OK");


        System.out.println("\tTEST ----- Single table with aliases on fields");
        RelReadQueryResults actorSimpleAlias = api.read()
                .select()
                .alias("A_ID", "A_FullName", "A_DateOfBirth", "A_Salary", "A_FilmID")
                .from("Actors")
                .build()
                .run(broker);
        assertEquals(actorSimpleAlias.getData().size(), actorCount);
        assertEquals(actorSimpleAlias.getFieldNames(), new ArrayList<>(Arrays.asList("A_ID", "A_FullName", "A_DateOfBirth", "A_Salary", "A_FilmID")));
        for(RelRow row: actorSimpleAlias.getData()){
            assertEquals(row.getSize(), 5);
        }
        for(RelRow readRow: actorSimpleAlias.getData()){
            boolean present = false;
            for(RelRow writtenRow: actorRows){
                if(readRow.getField(0).equals(writtenRow.getField(0)) &&
                        readRow.getField(1).equals(writtenRow.getField(1)) &&
                        readRow.getField(2).equals(writtenRow.getField(2)) &&
                        readRow.getField(3).equals(writtenRow.getField(3)) &&
                        readRow.getField(4).equals(writtenRow.getField(4))
                ){
                    present = true;
                }
            }
            assertTrue(present);
        }
        System.out.println("\t\tTEST ----- User defined schema OK");


        System.out.println("\tTEST ----- Single table with aliases on not all fields");
        RelReadQueryResults actorAlias = api.read()
                .select()
                .alias("A_ID", "", null, "A_Salary", "A_FilmID")
                .from("Actors")
                .build()
                .run(broker);
        assertEquals(actorAlias.getData().size(), actorCount);
        assertEquals(actorAlias.getFieldNames(), new ArrayList<>(Arrays.asList("A_ID", "FullName", "DateOfBirth", "A_Salary", "A_FilmID")));
        for(RelRow row: actorAlias.getData()){
            assertEquals(row.getSize(), 5);
        }
        for(RelRow readRow: actorAlias.getData()){
            boolean present = false;
            for(RelRow writtenRow: actorRows){
                if(readRow.getField(0).equals(writtenRow.getField(0)) &&
                        readRow.getField(1).equals(writtenRow.getField(1)) &&
                        readRow.getField(2).equals(writtenRow.getField(2)) &&
                        readRow.getField(3).equals(writtenRow.getField(3)) &&
                        readRow.getField(4).equals(writtenRow.getField(4))
                ){
                    present = true;
                }
            }
            assertTrue(present);
        }
        System.out.println("\t\tTEST ----- User partially defined schema OK");


        System.out.println("\tTEST ----- Projection on single table with aliases on fields");
        RelReadQueryResults actorSimpleProjAlias = api.read()
                .select("FullName", "Salary")
                .alias("A_FullName", "A_Salary")
                .from("Actors")
                .build()
                .run(broker);
        assertEquals(actorSimpleProjAlias.getData().size(), actorCount);
        assertEquals(actorSimpleProjAlias.getFieldNames(), new ArrayList<>(Arrays.asList("A_FullName", "A_Salary")));
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
        System.out.println("\t\tTEST ----- User defined schema on projection OK");


        System.out.println("\tTEST ----- Projection on single table with some aliases on fields");
        RelReadQueryResults actorProjAlias = api.read()
                .select("ID", "FullName", "Salary")
                .alias(null, "A_FullName", "")
                .from("Actors")
                .build()
                .run(broker);
        assertEquals(actorProjAlias.getData().size(), actorCount);
        assertEquals(actorProjAlias.getFieldNames(), new ArrayList<>(Arrays.asList("ID", "A_FullName", "Salary")));
        for(RelRow row: actorProjAlias.getData()){
            assertEquals(row.getSize(), 3);
        }
        for(RelRow readRow: actorProjAlias.getData()){
            boolean present = false;
            for(RelRow writtenRow: actorRows){
                if(readRow.getField(0).equals(writtenRow.getField(0)) &&
                        readRow.getField(1).equals(writtenRow.getField(1)) &&
                        readRow.getField(2).equals(writtenRow.getField(3))){
                    present = true;
                }
            }
            assertTrue(present);
        }
        System.out.println("\t\tTEST ----- User partially defined schema on projection OK");


        System.out.println("----- PREDICATE TESTING -----");

        System.out.println("\tTEST ----- Select on Single table, single field");
        RelReadQueryResults selectActors = api.read()
                .select()
                .fromFilter("Actors", "FullName == \"Johnny Depp\" || FullName == \"Brad Pitt\"")
                .build().run(broker);
        assertEquals(selectActors.getData().size(), 2);
        assertEquals(selectActors.getFieldNames(), actorsSchema);
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
        System.out.println("\t\tTEST ----- Selection on single table and single field OK");

        System.out.println("\tTEST ----- Select on Single Table, multiple fields");
        RelReadQueryResults selectFilms = api.read()
                .select()
                .fromFilter("Films", "ID > 5 && Budget > 50000000")
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
        assertEquals(selectFilms.getFieldNames(), filmsSchema);
        System.out.println("\t\tTEST ----- Selection on multiple fields OK");


        System.out.println("\tTEST ----- Selection and Projection on single table");
        RelReadQueryResults actorSelProj = api.read()
                .select("FullName", "Salary")
                .fromFilter("Actors", "FullName == \"Julia Roberts\"" )
                .build()
                .run(broker);
        assertEquals(actorSelProj.getData().size(), 1);
        assertEquals(actorSelProj.getFieldNames(), new ArrayList<>(Arrays.asList("FullName", "Salary")));
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
        System.out.println("\t\tTEST ----- selection and projection OK");


        System.out.println("\tTEST ----- Selection and Projection on single table with aliases on fields");
        RelReadQueryResults actorSelectProjAlias = api.read()
                .select("FullName", "Salary")
                .alias("A_FullName", "A_Salary")
                .fromFilter("Actors", "A_FullName == \"Julia Roberts\" || FullName == \"Vin Diesel\"")
                .build()
                .run(broker);
        assertEquals(actorSelectProjAlias.getData().size(), 2);
        assertEquals(actorSelectProjAlias.getFieldNames(), new ArrayList<>(Arrays.asList("A_FullName", "A_Salary")));
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
        System.out.println("\t\tTEST ----- Selection and projection on user-defined schema OK");


        System.out.println("\tTEST ----- Selection and Projection on single table with aliases on fields");
        RelReadQueryResults actorSelectProjPartialAlias = api.read()
                .select("FullName", "Salary")
                .alias("", "A_Salary")
                .fromFilter("Actors", "FullName == \"Julia Roberts\" || FullName == \"Vin Diesel\"")
                .build()
                .run(broker);
        assertEquals(actorSelectProjPartialAlias.getData().size(), 2);
        assertEquals(actorSelectProjPartialAlias.getFieldNames(), new ArrayList<>(Arrays.asList("FullName", "A_Salary")));
        for(RelRow row: actorSelectProjPartialAlias.getData()){
            assertEquals(row.getSize(), 2);
        }
        for(RelRow readRow: actorSelectProjPartialAlias.getData()){
            boolean present = false;
            for(RelRow writtenRow: actorRows){
                if(readRow.getField(0).equals(writtenRow.getField(1)) &&
                        readRow.getField(1).equals(writtenRow.getField(3))){
                    present = true;
                }
            }
            assertTrue(present);
        }
        System.out.println("\t\tTEST ----- Selection and projection on user-partially-defined schema OK");

        System.out.println("------ JOIN AND DISTINCT TESTING -----");
        System.out.println("\tTEST ----- Join");
        RelReadQueryResults joinResults = api.join()
                .sources("Actors", "Films",
                        "", "Director == \"Christopher Nolan\"",
                        List.of("FilmID"), List.of("ID")).build()
                .run(broker);
        int joinSize = 0;
        for(RelRow actorWrittenRow: actorRows){
            for(RelRow filmWrittenRow: filmRows){
                if(actorWrittenRow.getField(4).equals(filmWrittenRow.getField(0)) &&
                        filmWrittenRow.getField(1).equals("Christopher Nolan")){
                    joinSize++;
                    boolean isContainedInResults = false;
                    for(RelRow joinRow: joinResults.getData()){
                        if(!isContainedInResults &&
                                joinRow.getField(0).equals(actorWrittenRow.getField(0)) &&
                                joinRow.getField(1).equals(actorWrittenRow.getField(1)) &&
                                joinRow.getField(2).equals(actorWrittenRow.getField(2)) &&
                                joinRow.getField(3).equals(actorWrittenRow.getField(3)) &&
                                joinRow.getField(4).equals(actorWrittenRow.getField(4)) &&
                                joinRow.getField(5).equals(filmWrittenRow.getField(0)) &&
                                joinRow.getField(6).equals(filmWrittenRow.getField(1)) &&
                                joinRow.getField(7).equals(filmWrittenRow.getField(2))
                        ){
                            isContainedInResults = true;
                        }
                    }
                    assertTrue(isContainedInResults);
                }
            }
        }
        for(RelRow resRow: joinResults.getData()){
            assertEquals(resRow.getSize(), 8);
        }
        assertEquals(joinSize, joinResults.getData().size());
        assertEquals(joinResults.getFieldNames(), Arrays.asList("Actors.ID", "Actors.FullName", "Actors.DateOfBirth",
                "Actors.Salary", "Actors.FilmID", "Films.ID", "Films.Director", "Films.Budget"));
        System.out.println("\tTEST ----- Join OK");


        System.out.println("\tTEST ----- Distinct");
        RelRow newRow = new RelRow(123, "Quentin Tarantino", 80000000);
        filmRows.add(newRow);
        api.write().data(newRow).table("Films").build().run();
        RelReadQueryResults distinctResults = api.read().
                select("Director", "Budget").
                from("Films").
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
        printRowList(distinctResults.getData());
        System.out.println();
        printRowList(filmRows);
        System.out.println(filmRows.size());
        assertEquals(distinctResults.getData().size(), filmRows.size()-1);
        assertEquals(distinctResults.getFieldNames(), Arrays.asList("Director", "Budget"));
        System.out.println("\tTEST ----- Distinct OK");


        broker.shutdown();
        stopServers();
    }


    @Test
    public void aggregateTests() {
        System.out.println("TEST ----- AGGREGATES TESTING");
        startServers();
        Broker broker = new Broker(zkHost, zkPort);

        loadActorsAndFilms();

        int totFilmBudget = 0, avgFilmBudget = 0, maxBudget = Integer.MIN_VALUE, minBudget = Integer.MAX_VALUE;
        for(RelRow writtenRow: filmRows){
            int budget = (Integer) writtenRow.getField(2);
            totFilmBudget += budget;
            maxBudget = Integer.max(maxBudget, budget);
            minBudget = Integer.min(minBudget, budget);
        }
        avgFilmBudget = totFilmBudget / filmCount;


        API api = new API(broker);
        System.out.println("\tTEST ----- Table creation");
        api.createTable("Actors").attributes(actorsSchema.toArray(new String[0])).keys("ID").build().run();
        api.createTable("Films").attributes(filmsSchema.toArray(new String[0])).keys("ID").build().run();
        System.out.println("\tTEST ----- Populating tables");
        api.write().table("Actors").data(actorRows).build().run();
        api.write().table("Films").data(filmRows).build().run();


        System.out.println("\tTEST ----- All aggregate operators on single table and single attribute");
        RelReadQueryResults totBudget = api.read()
                .select()
                .from("Films")
                .sum("Budget", "TotBudget")
                .count("Budget", "Count")
                .avg("Budget", "AvgFilmBudget")
                .max("Budget", "MaxBudget")
                .min("Budget", "MinBudget")
                .build().run(broker);
        assertEquals(totFilmBudget, (int) ((Number) totBudget.getData().get(0).getField(0)).doubleValue());
        assertEquals(filmCount, (int) ((Number) totBudget.getData().get(0).getField(1)).doubleValue());
        assertEquals(avgFilmBudget, (int) ((Number) totBudget.getData().get(0).getField(2)).doubleValue());
        assertEquals(maxBudget, (int) ((Number) totBudget.getData().get(0).getField(3)).doubleValue());
        assertEquals(minBudget, (int) ((Number) totBudget.getData().get(0).getField(4)).doubleValue());
        System.out.println("\t\tTEST ----- Simple Aggregate on table OK");

        System.out.println("\tTEST ----- Simple aggregate on filter and projection subquery");
        ReadQuery filterAndProjectionFilms = api.read().select("ID", "Budget").fromFilter("Films", "ID > 10").build();
        RelReadQueryResults budgetFilmsIDMoreThan10 = api.read()
                .select()
                .sum("Budget", "TotBudget")
                .count("Budget", "Count")
                .avg("Budget", "AvgFilmBudget")
                .max("Budget", "MaxBudget")
                .min("Budget", "MinBudget")
                .from(filterAndProjectionFilms, "filProjFilm")
                .build().run(broker);
        int count10 = filmRows.size()-11, sum10 = 0, avg10 = 0, min10 = Integer.MAX_VALUE, max10 = Integer.MIN_VALUE;
        for(RelRow writtenFilmRow: filmRows){
            int id =  (Integer) writtenFilmRow.getField(0);
            int val = (Integer) writtenFilmRow.getField(2);
            if(id>10) {
                sum10 += val;
                min10 = Integer.min(val, min10);
                max10 = Integer.max(val, max10);
            }
        }
        avg10 = sum10 / count10;
        assertEquals(sum10,   (int) ((Number) budgetFilmsIDMoreThan10.getData().get(0).getField(0)).doubleValue());
        assertEquals(count10, (int) ((Number) budgetFilmsIDMoreThan10.getData().get(0).getField(1)).doubleValue());
        assertEquals(avg10, (int) ((Number) budgetFilmsIDMoreThan10.getData().get(0).getField(2)).doubleValue());
        assertEquals(max10, (int) ((Number) budgetFilmsIDMoreThan10.getData().get(0).getField(3)).doubleValue());
        assertEquals(min10, (int) ((Number) budgetFilmsIDMoreThan10.getData().get(0).getField(4)).doubleValue());
        System.out.println("\tTEST ----- Simple aggregate on filter and projection subquery");


        System.out.println("\t\tTEST ----- Groups aggregate operation on subquery results, with predicate on results");
        RelReadQueryResults totalActorEarnings = api.read()
                .select("Films.Director")
                .alias("DirectorName")
                .sum("Actors.Salary", "TotalActorsEarnings")
                .count("Actors.Salary", "NumFilms")
                .from(
                        api.join().sources("Actors", "Films",  null, null,
                                List.of("FilmID"), List.of("ID")).build(), "JoinedFilmsActors"
                )
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
                assertEquals((Number)sums.get(directorName).doubleValue(), ((Number) resRow.getField(1)).doubleValue());
            }
        }
        System.out.println("\t\tTEST ----- Group and predicate OK");

        broker.shutdown();
        stopServers();
    }
    @Test
    public void simpleNestedQueriesTests(){
        System.out.println("TEST ----- NESTING TESTS");
        startServers();
        Broker broker = new Broker(zkHost, zkPort);

        API api = new API(broker);

        loadUniversity();

        System.out.println("\tTEST ----- Creating tables");
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

        System.out.println("\tTEST ----- Populating tables");
        System.out.println("\t\tTEST   -----   Populating students");
        api.write().table("Students").data(studentsRows).build().run();
        System.out.println("\t\tTEST   -----   Populating professors");
        api.write().table("Professors").data(professorsRows).build().run();
        System.out.println("\t\tTEST   -----   Populating courses");
        api.write().table("Courses").data(coursesRows).build().run();
        System.out.println("\t\tTEST   -----   Populating exams");
        api.write().table("Exams").data(examsRows).build().run();

        Random rng = new Random();
        System.out.println("----- NESTED QUERIES TESTING -----");

        System.out.println("\tTEST ----- Nested simple query");
        ReadQuery allStudentsQuery = api.read().from("Students").build();
        RelReadQueryResults allStudentSelect = api.read().select().from(allStudentsQuery, "allStudents").build().run(broker);
        assertEquals(allStudentSelect.getData().size(), studentsCount);
        for(RelRow readRow: allStudentSelect.getData()){
            boolean present = false;
            for(RelRow writeRow: studentsRows){
                if (readRow.equals(writeRow)) {
                    present = true;
                    break;
                }
           }
            assertTrue(present);
        }
        System.out.println("\t\tTEST ----- Simple nesting OK");

        System.out.println("\tTEST ----- Filter in nested query");
        int randomStudentID = rng.nextInt()%studentsCount;
        if(randomStudentID<0){randomStudentID = (randomStudentID * -1);}
        ReadQuery filterOnSubqueryQuery = api.read().fromFilter("Students", "ID < "+randomStudentID).build();
        RelReadQueryResults randomStudent = api.read().select().from(filterOnSubqueryQuery, "randomStudent").build().run(broker);
        for(RelRow readRow: randomStudent.getData()){
            boolean present = false;
            for(RelRow writeRow: studentsRows){
                if (readRow.equals(writeRow)) {
                    present = true;
                    break;
                }
            }
            assertTrue(present);
        }
        System.out.println("\t\tTEST ----- Filter in nested query OK");

        System.out.println("\tTEST ----- Filter on nested query");
        randomStudent = api.read().select().fromFilter(allStudentsQuery, "randomStudent", "ID == " + randomStudentID ).build().run(broker);
        for(RelRow readRow: randomStudent.getData()){
            boolean present = false;
            for(RelRow writeRow: studentsRows){
                if (readRow.equals(writeRow)) {
                    present = true;
                    break;
                }
            }
            assertTrue(present);
        }
        System.out.println("\t\tTEST ----- Filter on nested query OK");

        System.out.println("\tTEST ----- Projection in nested query");
        ReadQuery projectAllStudentsSubQuery = api.read().select("ID").from("Students").build();
        RelReadQueryResults allStudentsIDs = api.read().select().from(projectAllStudentsSubQuery, "allIDs").build().run(broker);
        for(RelRow readRow: allStudentsIDs.getData()){
            boolean present = false;
            for(RelRow writeRow: studentsRows){
                if (readRow.getField(0).equals(writeRow.getField(0))) {
                    present = true;
                    break;
                }
            }
            assertTrue(present);
        }
        System.out.println("\t\tTEST ----- Projection in nested query OK");

        System.out.println("\tTEST ----- Projection on nested query");
        allStudentsIDs = api.read().select("ID").from(allStudentsQuery, "allStudents").build().run(broker);
        for(RelRow readRow: allStudentsIDs.getData()){
            boolean present = false;
            for(RelRow writeRow: studentsRows){
                if (readRow.getField(0).equals(writeRow.getField(0))) {
                    present = true;
                    break;
                }
            }
            assertTrue(present);
        }
        System.out.println("\t\tTEST ----- Projection on nested query OK");

        System.out.println("\tTEST ----- Filter and projection in nested query");
        ReadQuery randomStudentIDQuery = api.read().select("ID").fromFilter("Students", "ID == " + randomStudentID).build();
        RelReadQueryResults randomStudentIDRes = api.read().select().from(randomStudentIDQuery, "randomStudentID").build().run(broker);
        for(RelRow readRow: randomStudentIDRes.getData()){
            boolean present = false;
            for(RelRow writeRow: studentsRows){
                if (readRow.getField(0).equals(writeRow.getField(0))) {
                    present = true;
                    break;
                }
            }
            assertTrue(present);
        }
        System.out.println("\t\tTEST ----- Filter and projection in nested query OK");

        System.out.println("\tTEST ----- Filter and projection on nested query");
        randomStudentIDRes = api.read().select("ID").fromFilter(allStudentsQuery, "randomStudent", "ID == "+randomStudentID).build().run(broker);
        for(RelRow readRow: randomStudentIDRes.getData()){
            boolean present = false;
            for(RelRow writeRow: studentsRows){
                if (readRow.getField(0).equals(writeRow.getField(0))) {
                    present = true;
                    break;
                }
            }
            assertTrue(present);
        }
        System.out.println("\t\tTEST ----- Filter and projection on nested query OK");

        System.out.println("\tTEST ----- Filter on subquery with projection");
        randomStudentIDRes = api.read().select().fromFilter(projectAllStudentsSubQuery, "allStudentsIDs", "ID == " + randomStudentID).build().run(broker);
        for(RelRow readRow: randomStudentIDRes.getData()){
            boolean present = false;
            for(RelRow writeRow: studentsRows){
                if (readRow.getField(0).equals(writeRow.getField(0))) {
                    present = true;
                    break;
                }
            }
            assertTrue(present);
        }
        System.out.println("\t\tTEST ----- Filter on subquery with projection OK");

        System.out.println("\tTEST ----- Projection on subquery with filter");
        randomStudentIDRes = api.read().select("ID").from(randomStudentIDQuery, "aStudentID").build().run(broker);
        for(RelRow readRow: randomStudentIDRes.getData()){
            boolean present = false;
            for(RelRow writeRow: studentsRows){
                if (readRow.getField(0).equals(writeRow.getField(0))) {
                    present = true;
                    break;
                }
            }
            assertTrue(present);
        }
        System.out.println("\t\tTEST ----- Projection on subquery with filter OK");

        int sum = 0;
        int count = examsRows.size();
        int min = Integer.MAX_VALUE;
        int max = Integer.MIN_VALUE;
        int avg = 0;
        for(RelRow writtenRow: examsRows){
            int rowValue = (Integer)writtenRow.getField(examsSchema.indexOf("Grade"));
            sum = sum + rowValue;
            min = Integer.min(min, rowValue);
            max = Integer.max(max, rowValue);
        }
        avg = sum/count;

        System.out.println("\tTEST ----- Simple Aggregate subquery");
        ReadQuery allAggregates = api.read().select().from("Exams")
                .avg("Grade", "avgGrade")
                .max("Grade", "maxGrade")
                .min("Grade", "minGrade")
                .count("Grade", "numEntries")
                .sum("Grade", "sumGrades").build();
        RelReadQueryResults allAggregatesSubq = api.read().select().from(allAggregates, "allAggregates").build().run(broker);
        assertEquals(allAggregatesSubq.getData().size(), 1);
        RelRow allAggregatesResultRow = allAggregatesSubq.getData().get(0);
        assertEquals(avg, ((Number)allAggregatesResultRow.getField(0)).intValue());
        assertEquals(max, ((Number)allAggregatesResultRow.getField(1)).doubleValue());
        assertEquals(min, ((Number)allAggregatesResultRow.getField(2)).doubleValue());
        assertEquals(count, ((Number)allAggregatesResultRow.getField(3)).doubleValue());
        assertEquals(sum,   ((Number)allAggregatesResultRow.getField(4)).doubleValue());
        System.out.println("\t\tTEST ----- Simple Aggregate subquery OK");


        System.out.println("\tTEST ----- Simple aggregate query on simple subquery");
        ReadQuery allExams = api.read().select().from("Exams").build();
        allAggregatesSubq = api.read().select().from(allExams, "allExams")
                .avg("Grade", "avgGrade")
                .max("Grade", "maxGrade")
                .min("Grade", "minGrade")
                .count("Grade", "numEntries")
                .sum("Grade", "sumGrades").build().run(broker);
        assertEquals(allAggregatesSubq.getData().size(), 1);
        allAggregatesResultRow = allAggregatesSubq.getData().get(0);
        assertEquals(avg,   ((Number)allAggregatesResultRow.getField(0)).intValue());
        assertEquals(max,   ((Number)allAggregatesResultRow.getField(1)).intValue());
        assertEquals(min,   ((Number)allAggregatesResultRow.getField(2)).intValue());
        assertEquals(count, ((Number)allAggregatesResultRow.getField(3)).intValue());
        assertEquals(sum,   ((Number)allAggregatesResultRow.getField(4)).intValue());
        System.out.println("\t\tTEST ----- Simple aggregate query on simple subquery OK");


        System.out.println("\tTEST ----- Simple aggregate on projection sub query");
        ReadQuery allGrades = api.read().select("Grade").from("Exams").build();
        allAggregatesSubq = api.read().select().from(allGrades, "allGrades")
                .avg("Grade", "avgGrade")
                .max("Grade", "maxGrade")
                .min("Grade", "minGrade")
                .count("Grade", "numEntries")
                .sum("Grade", "sumGrades").build().run(broker);
        assertEquals(allAggregatesSubq.getData().size(), 1);
        allAggregatesResultRow = allAggregatesSubq.getData().get(0);
        assertEquals(avg,   ((Number)allAggregatesResultRow.getField(0)).intValue());
        assertEquals(max,   ((Number)allAggregatesResultRow.getField(1)).intValue());
        assertEquals(min,   ((Number)allAggregatesResultRow.getField(2)).intValue());
        assertEquals(count, ((Number)allAggregatesResultRow.getField(3)).intValue());
        assertEquals(sum,   ((Number)allAggregatesResultRow.getField(4)).intValue());
        System.out.println("\t\tTEST ----- Simple aggregate on projection sub query OK");

        System.out.println("\tTEST ----- Simple aggregate on filter and projection sub query");
        ReadQuery allGoodGrades = api.read().select("Grade").fromFilter("Exams", "Grade >= 18").build();
        allAggregatesSubq = api.read().select().from(allGoodGrades, "allGoodGrades")
                .avg("Grade", "avgGrade")
                .max("Grade", "maxGrade")
                .min("Grade", "minGrade")
                .count("Grade", "numEntries")
                .sum("Grade", "sumGrades").build().run(broker);
        assertEquals(allAggregatesSubq.getData().size(), 1);
        allAggregatesResultRow = allAggregatesSubq.getData().get(0);

        int avg18 = 0, max18 = Integer.MIN_VALUE, min18 = Integer.MAX_VALUE, count18 = 0, sum18 = 0;
        for(RelRow writtenRow:examsRows){
            int grade = (Integer) writtenRow.getField(2);
            if(grade >= 18){
                sum18 += grade;
                count18++;
                min18 = Integer.min(min18, grade);
                max18 = Integer.max(max18, grade);
            }
        }
        avg18 = sum18 / count18;
        assertEquals(avg18,   ((Number)allAggregatesResultRow.getField(0)).intValue());
        assertEquals(max18,   ((Number)allAggregatesResultRow.getField(1)).intValue());
        assertEquals(min18,   ((Number)allAggregatesResultRow.getField(2)).intValue());
        assertEquals(count18, ((Number)allAggregatesResultRow.getField(3)).intValue());
        assertEquals(sum18,   ((Number)allAggregatesResultRow.getField(4)).intValue());
        System.out.println("\t\tTEST ----- Simple aggregate on filter and projection sub query OK");


        System.out.println("\tTEST ----- Aggregate on aggregate subquery");
        ReadQuery studentAvgGrade = api.read()
                .select("StudentID")
                .avg("Grade", "avgGrade")
                .from("Exams").build();
        RelReadQueryResults maxAvgGrade = api.read().select().max("avgGrade", "maxAVG")
                .from(studentAvgGrade, "avgGrades").build().run(broker);

        Map<Integer, List<RelRow>> studentsIDsToExams = new HashMap<>();
        for(RelRow writtenRow: examsRows){
            int studentID = (int) writtenRow.getField(examsSchema.indexOf("StudentID"));
            studentsIDsToExams.computeIfAbsent(studentID, k->new ArrayList<>()).add(writtenRow);
        }
        Map<Integer, Integer> studentIDToAVG = new HashMap<>();
        for(Map.Entry<Integer, List<RelRow>> entry: studentsIDsToExams.entrySet()){
            int studentID = entry.getKey();
            int avgGrade = 0;
            for(RelRow exam: entry.getValue()){
                avgGrade += (int) exam.getField(examsSchema.indexOf("Grade"));
            }
            avgGrade = avgGrade / entry.getValue().size();
            studentIDToAVG.put(studentID, avgGrade);
        }
        int maxAVG = 0;
        for(Map.Entry<Integer, Integer> entry: studentIDToAVG.entrySet()){
            int currentStudentAVG = entry.getValue();
            if(currentStudentAVG > maxAVG)
                maxAVG = currentStudentAVG;
        }
        for(RelRow readRow: maxAvgGrade.getData()){
            assertEquals(maxAVG, ((Number) readRow.getField(0)).intValue());
        }
        System.out.println("\t\tTEST ----- Aggregate on aggregate subquery OK");


        System.out.println("\tTEST ----- Join on subquery");
        long tStart = System.currentTimeMillis();
        RelReadQueryResults maxCFUs = api.read().select("Courses.Code", "Courses.Name").alias("Code", "Name").from(
                api.join().sources(
                                "Courses",
                                api.read().select().max("CFUs", "maxCFU").from("Courses").build(),
                                "C1", "", "",
                                List.of("CFUs"), List.of("maxCFU")
                        ).build(), "Join")
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
        assertEquals(maxCFUs.getFieldNames(), List.of("Code", "Name"));
        assertEquals(writtenMaxCFU.size(), maxCFUs.getData().size());
        System.out.println("\t\tTEST ----- Nested Join query, with nested simple aggregate OK");


        System.out.println("\tTEST ----- not min CFUs");
        tStart = System.currentTimeMillis();
        ReadQuery minCFU = api.read().select().min("CFUs", "minCFUs").from("Courses").build();
        ReadQuery notMinCFUsQuery = api.read()
                .select("Code", "Name")
                .fromFilter("Courses", "CFUs > min")
                .predicateSubquery("min", minCFU)
                .build();
        RelReadQueryResults notMinCFUs = notMinCFUsQuery.run(broker);
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
        assertEquals(notMinCFUs.getFieldNames(), List.of("Code", "Name"));
        System.out.println("\t\tTEST ----- Nested query on predicate OK");


        System.out.println("\tTEST ----- Students who took at least 20, filter on nested query");
        tStart = System.currentTimeMillis();
        RelReadQueryResults atLeast20 = api.read().select("Exams.CourseCode", "Students.ID", "Students.Name", "Students.Surname")
                .alias("CourseCode", "StudentID", "Name", "Surname")
                .from(
                    api.join().sources("Exams", "Students", "Grade >= 20", "", List.of("StudentID"), List.of("ID")).build(), "J"
                ).build().run(broker);
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
        System.out.println("\t\tTEST ----- Nested Join query OK");


        System.out.println("\tTEST ----- Students who took at least 20, filter on main query");
        tStart = System.currentTimeMillis();
        atLeast20 = api.read().select("Exams.CourseCode", "Students.ID", "Students.Name", "Students.Surname")
                .alias("CourseCode", "StudentID", "Name", "Surname")
                .fromFilter(
                        api.join().sources(
                                "Exams", "Students",
                                "", "",
                                List.of("StudentID"), List.of("ID")).build(),
                        "J", "Exams.Grade >= 20"
                ).build().run(broker);
        System.out.println("\t\tExecution time: " + (System.currentTimeMillis() - tStart)+"ms.");
        matchesSize = 0;
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
        System.out.println("\t\tTEST ----- Nested query with filter OK");

        broker.shutdown();
        stopServers();
    }

    @Test
    public void storedTest() {
        startServers();
        Broker broker = new Broker(zkHost, zkPort);


        loadActorsAndFilms();
        loadUniversity();

        API api = new API(broker);

        System.out.println("TEST   -----   Creating tables");

        api.createTable("Actors")
                .attributes(actorsSchema.toArray(new String[0]))
                .shardNumber(20)
                .keys("ID")
                .build().run();
        api.createTable("Films")
                .attributes(filmsSchema.toArray(new String[0]))
                .keys("ID")
                .shardNumber(20)
                .build().run();
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

        System.out.println("TEST   -----   Populating tables");
        System.out.println("\tTEST   -----   Writing students");
        api.write().table("Students").data(studentsRows).build().run();
        System.out.println("\tTEST   -----   Writing professors");
        api.write().table("Professors").data(professorsRows).build().run();
        System.out.println("\tTEST   -----   Writing courses");
        api.write().table("Courses").data(coursesRows).build().run();
        System.out.println("\tTEST   -----   Writing exams");
        api.write().table("Exams").data(examsRows).build().run();
        System.out.println("\tTEST   -----   Writing actors");
        api.write().table("Actors").data(actorRows).build().run();
        System.out.println("\tTEST   -----   Writing films");
        api.write().table("Films").data(filmRows).build().run();

        System.out.println("TEST   -----   Storing queries");
        System.out.println("\tTEST ----- Select All Actors");
        ReadQuery allActorsQuery = api.read()
                .select()
                .from("Actors")
                .store()
                .build();
        RelReadQueryResults allActors = allActorsQuery.run(broker);

        assertEquals(allActors.getData().size(), actorCount);
        assertEquals(allActors.getFieldNames(), new ArrayList<>(Arrays.asList("ID", "FullName", "DateOfBirth", "Salary", "FilmID")));
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
        System.out.println("\t\tTEST ----- Checking whether the query's stored");
        TableInfo actorTableInfo = broker.getTableInfo("Actors");
        boolean allActorStoredQuery = false;
        for(ReadQuery storedActorQuery: actorTableInfo.getRegisteredQueries()){
            if(storedActorQuery.equals(allActorsQuery)){
                allActorStoredQuery = true;
                break;
            }
        }
        assertTrue(allActorStoredQuery);
        System.out.println("\t\t\tTEST ----- Query is stored");

        System.out.println("\t\tTEST ----- Updating source");
        RelRow newWrittenRow = new RelRow(1111, "Test", "Test", 0, 0);
        api.write().table("Actors").data(List.of(newWrittenRow)).build().run();

        int newActorCount = actorCount+1;
        boolean presentAllActor = false;
        RelReadQueryResults resTableContent = api.read().select().from("1").build().run(broker);
        assertEquals(resTableContent.getData().size(), newActorCount);
        presentAllActor = false;
        for(RelRow readRow : resTableContent.getData()){
            if(readRow.getField(0).equals(1111) &&
                    readRow.getField(1).equals("Test") &&
                    readRow.getField(2).equals("Test") &&
                    readRow.getField(3).equals(0) &&
                    readRow.getField(4).equals(0)
            ){
                presentAllActor = true;
                break;
            }
        }
        assertTrue(presentAllActor);
        System.out.println("\t\t\tTEST ------ Result table is updated");


        System.out.println("\t\tTEST ----- Starting same read query");
        presentAllActor = false;
        allActors = allActorsQuery.run(broker);
        assertEquals(allActors.getData().size(), newActorCount);
        for(RelRow readRow : allActors.getData()){
            if(readRow.getField(0).equals(1111) &&
            readRow.getField(1).equals("Test") &&
                    readRow.getField(2).equals("Test") &&
                    readRow.getField(3).equals(0) &&
                    readRow.getField(4).equals(0)
            ){
                presentAllActor = true;
                break;
            }
        }
        assertTrue(presentAllActor);
        System.out.println("\t\t\tTEST ------ Results are correct");

        System.out.println("\t\tTEST ------ Deleting added row");
        api.delete().from("Actors").data(List.of(newWrittenRow)).build().run();
        System.out.println("\t\t\tTEST ------ Delete completed");

        resTableContent = api.read().select().from("1").build().run(broker);
        assertEquals(resTableContent.getData().size(), newActorCount);
        presentAllActor = false;
        for(RelRow readRow : resTableContent.getData()){
            if(readRow.getField(0).equals(1111) &&
                    readRow.getField(1).equals("Test") &&
                    readRow.getField(2).equals("Test") &&
                    readRow.getField(3).equals(0) &&
                    readRow.getField(4).equals(0)
            ){
                presentAllActor = true;
                break;
            }
        }
        assertTrue(presentAllActor);
        System.out.println("\t\t\tTEST ------ Result table is updated");

        presentAllActor = false;
        allActors = allActorsQuery.run(broker);
        assertEquals(allActors.getData().size(), newActorCount);
        for(RelRow readRow : allActors.getData()){
            if(readRow.getField(0).equals(1111) &&
                    readRow.getField(1).equals("Test") &&
                    readRow.getField(2).equals("Test") &&
                    readRow.getField(3).equals(0) &&
                    readRow.getField(4).equals(0)
            ){
                presentAllActor = true;
                break;
            }
        }
        assertTrue(presentAllActor);
        System.out.println("\t\t\tTEST ------ Results are correct");



        allActorsQuery = api.read().select().fromFilter("Actors", "Salary > 0").build();
        allActors = allActorsQuery.run(broker);
        assertEquals(allActors.getData().size(), actorCount);

        System.out.println("\tTEST ----- Storing aggregate query");
        ReadQuery totalActorEarningsGroupQuery =  api.read()
                .select("Films.Director")
                .alias("DirectorName")
                .sum("Actors.Salary", "TotalActorsEarnings")
                .count("Actors.Salary", "NumFilms")
                .from(
                        api.join()
                                .sources("Actors", "Films",
                                        null, null,
                                        List.of("FilmID"), List.of("ID")
                                ).build(), "Join")
                .having("NumFilms > 1")
                .store()
                .build();

        RelReadQueryResults totalActorEarningsGroup = totalActorEarningsGroupQuery.run(broker);
        actorTableInfo = broker.getTableInfo("Actors");
        TableInfo filmTableInfo = broker.getTableInfo("Films");
        boolean isTotEarnStored = false;
        for(ReadQuery actorStoredQuery: actorTableInfo.getRegisteredQueries()){
            if(actorStoredQuery.equals(totalActorEarningsGroupQuery)){
                isTotEarnStored = true;
                break;
            }
        }
        assertTrue(isTotEarnStored);
        isTotEarnStored = false;
        for(ReadQuery filmsStoredQuery: filmTableInfo.getRegisteredQueries()){
            if(filmsStoredQuery.equals(totalActorEarningsGroupQuery)){
                isTotEarnStored = true;
                break;
            }
        }
        assertTrue(isTotEarnStored);


        System.out.println("\tTEST ----- Stored query used as subquery");
        RelReadQueryResults maxTotEarning = api.read()
                .max("TotalActorsEarnings", "MaxEarnings")
                .from(totalActorEarningsGroupQuery, "S")
                .build().run(broker);

        broker.shutdown();
        stopServers();
    }
}
