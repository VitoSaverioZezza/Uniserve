package edu.stanford.futuredata.uniserve.integration;

import edu.stanford.futuredata.uniserve.broker.Broker;
import edu.stanford.futuredata.uniserve.coordinator.Coordinator;
import edu.stanford.futuredata.uniserve.coordinator.DefaultAutoScaler;
import edu.stanford.futuredata.uniserve.coordinator.DefaultLoadBalancer;
import edu.stanford.futuredata.uniserve.datastore.DataStore;
import edu.stanford.futuredata.uniserve.interfaces.ShuffleOnReadQueryPlan;
import edu.stanford.futuredata.uniserve.localcloud.LocalDataStoreCloud;
import edu.stanford.futuredata.uniserve.tablemockinterface.TableQueryEngine;
import edu.stanford.futuredata.uniserve.tablemockinterface.TableRow;
import edu.stanford.futuredata.uniserve.tablemockinterface.TableShard;
import edu.stanford.futuredata.uniserve.tablemockinterface.TableShardFactory;
import edu.stanford.futuredata.uniserve.tablemockinterface.queryplans.TableReadMostFrequent;
import edu.stanford.futuredata.uniserve.tablemockinterface.queryplans.TableReadPopularState;
import edu.stanford.futuredata.uniserve.tablemockinterface.queryplans.TableWriteInsert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static edu.stanford.futuredata.uniserve.integration.KVStoreTests.cleanUp;
import static org.junit.jupiter.api.Assertions.*;

public class TableTests {
    private static final Logger logger = LoggerFactory.getLogger(TableTests.class);

    private static String zkHost = "127.0.0.1";
    private static Integer zkPort = 2181;

    @BeforeAll
    static void startUpCleanUp() {
        cleanUp(zkHost, zkPort);
    }

    @AfterEach
    private void unitTestCleanUp() {
        cleanUp(zkHost, zkPort);
    }


    @Test
    public void testShuffleMostFrequent() {
        logger.info("testShuffleMostFrequent");
        int numShards = 4;
        Coordinator coordinator = new Coordinator(null, new DefaultLoadBalancer(), new DefaultAutoScaler(), zkHost, zkPort, "127.0.0.1", 7777);
        coordinator.runLoadBalancerDaemon = false;
        assertTrue(coordinator.startServing());
        int numDataStores = 4;
        List<DataStore<TableRow, TableShard>> dataStores = new ArrayList<>();
        List<LocalDataStoreCloud> localDataStoreClouds = new ArrayList<>();
        for (int i = 0; i < numDataStores; i++) {
            localDataStoreClouds.add(i, new LocalDataStoreCloud());
            DataStore<TableRow, TableShard>  dataStore = new DataStore<>(localDataStoreClouds.get(i),
                    new TableShardFactory(), Path.of(String.format("/var/tmp/KVUniserve%d", i)), zkHost, zkPort, "127.0.0.1", 8200 + i, -1, false
            );
            dataStore.runPingDaemon = false;
            assertTrue(dataStore.startServing());
            dataStores.add(dataStore);
        }
        Broker broker = new Broker(zkHost, zkPort);
        assertTrue(broker.createTable("table1", numShards, new ArrayList<>(), null));

        List<TableRow> rows = new ArrayList<>();
        for (int k = 0; k < 5; k++) {
            for (int v = 0; v < k; v++) {
                rows.add(new TableRow(Map.of("k", k, "v", v), k));
            }
        }
        assertTrue(broker.writeQuery(new TableWriteInsert("table1"), rows));

        ShuffleOnReadQueryPlan<TableShard, Integer> r = new TableReadMostFrequent("table1");
        assertEquals(0, broker.shuffleReadQuery(r));

        dataStores.forEach(DataStore::shutDown);
        coordinator.stopServing();
        broker.shutdown();
        try {
            for (LocalDataStoreCloud localDataStoreCloud : localDataStoreClouds) {
                localDataStoreCloud.clear();
            }
        }catch (Exception e ){
            fail();
        }
    }

    @Test
    public void testShufflePopularState() {
        logger.info("testShufflePopularState");
        int numShards = 4;
        Coordinator coordinator = new Coordinator(null, new DefaultLoadBalancer(), new DefaultAutoScaler(), zkHost, zkPort, "127.0.0.1", 7777);
        coordinator.runLoadBalancerDaemon = false;
        assertTrue(coordinator.startServing());
        int numDataStores = 4;
        List<DataStore<TableRow, TableShard>> dataStores = new ArrayList<>();
        List<LocalDataStoreCloud> localDataStoreClouds = new ArrayList<>();

        for (int i = 0; i < numDataStores; i++) {
            localDataStoreClouds.add(i, new LocalDataStoreCloud());
            DataStore<TableRow, TableShard>  dataStore = new DataStore<>(localDataStoreClouds.get(i),
                    new TableShardFactory(), Path.of(String.format("/var/tmp/KVUniserve%d", i)), zkHost, zkPort, "127.0.0.1", 8200 + i, -1, false
            );
            dataStore.runPingDaemon = false;
            assertTrue(dataStore.startServing());
            dataStores.add(dataStore);
        }
        Broker broker = new Broker(zkHost, zkPort);
        assertTrue(broker.createTable("peopleTable", numShards, new ArrayList<>(), null));
        assertTrue(broker.createTable("stateTable", numShards, new ArrayList<>(), null));

        int numStates = 10;
        List<TableRow> rows = new ArrayList<>();
        List<Integer> cities = new ArrayList<>();
        for (int state = 0; state < numStates; state++) {
            for (int city = state * numStates; city <= state * numStates + state; city++) {
                rows.add(new TableRow(Map.of("city", city, "state", state), state));
                assert(!cities.contains(city));
                cities.add(city);
            }
        }
        assertTrue(broker.writeQuery(new TableWriteInsert("stateTable"), rows));
        rows.clear();
        for(int i = 0; i < cities.size(); i++) {
            rows.add(new TableRow(Map.of("person", i, "city", cities.get(i)), i));
        }
        assertTrue(broker.writeQuery(new TableWriteInsert("peopleTable"), rows));

        ShuffleOnReadQueryPlan<TableShard, Integer> r = new TableReadPopularState("peopleTable", "stateTable");
        assertEquals(9, broker.shuffleReadQuery(r));

        dataStores.forEach(DataStore::shutDown);
        coordinator.stopServing();
        broker.shutdown();
        try {
            for (LocalDataStoreCloud localDataStoreCloud : localDataStoreClouds) {
                localDataStoreCloud.clear();
            }
        }catch (Exception e ){
            fail();
        }
    }
}
