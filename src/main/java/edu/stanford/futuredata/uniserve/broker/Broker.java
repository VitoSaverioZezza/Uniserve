package edu.stanford.futuredata.uniserve.broker;

import com.google.protobuf.ByteString;
import edu.stanford.futuredata.uniserve.*;
import edu.stanford.futuredata.uniserve.datastore.DataStore;
import edu.stanford.futuredata.uniserve.interfaces.*;
import edu.stanford.futuredata.uniserve.utilities.ConsistentHash;
import edu.stanford.futuredata.uniserve.utilities.DataStoreDescription;
import edu.stanford.futuredata.uniserve.utilities.TableInfo;
import edu.stanford.futuredata.uniserve.utilities.Utilities;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;


/**
 * TODO: put txIDs and lastCommittedVersion attributes in ZooKeeper.
 * TODO: replace the ExecutorService readQueryThreadPool with async calls.
 * TODO: Figure out what to do with the query engine.
 * TODO: Retry routine and error handling when the Broker cannot find the Coordinator.
 * TODO: Error handling in getTableInfo should be not be carried out via assert statements.
 * TODO: Error handling in getStubForShard should not be carried out via assert statements.
 * TODO: Generic error handling across the whole Broker specification, this should NOT be done via assert statements.
 * */
public class Broker {

    private final QueryEngine queryEngine;
    private final BrokerCurator zkCurator;

    private static final Logger logger = LoggerFactory.getLogger(Broker.class);
    // Consistent hash assigning shards to servers.
    private ConsistentHash consistentHash;
    // Map from dsIDs to channels.
    private Map<Integer, ManagedChannel> dsIDToChannelMap = new ConcurrentHashMap<>();
    // Stub for communication with the coordinator.
    private BrokerCoordinatorGrpc.BrokerCoordinatorBlockingStub coordinatorBlockingStub;
    // Map from table names to IDs.
    private final Map<String, TableInfo> tableInfoMap = new ConcurrentHashMap<>();

    private final ShardMapUpdateDaemon shardMapUpdateDaemon;
    public boolean runShardMapUpdateDaemon = true;
    public static int shardMapDaemonSleepDurationMillis = 1000;

    private final QueryStatisticsDaemon queryStatisticsDaemon;
    public boolean runQueryStatisticsDaemon = true;
    public static int queryStatisticsDaemonSleepDurationMillis = 10000;

    public final Collection<Long> remoteExecutionTimes = new ConcurrentLinkedQueue<>();
    public final Collection<Long> aggregationTimes = new ConcurrentLinkedQueue<>();

    public static final int QUERY_SUCCESS = 0;
    public static final int QUERY_FAILURE = 1;
    public static final int QUERY_RETRY = 2;

    public static final int SHARDS_PER_TABLE = 1000000;

    public ConcurrentHashMap<Set<Integer>, Integer> queryStatistics = new ConcurrentHashMap<>();

    ExecutorService readQueryThreadPool = Executors.newFixedThreadPool(256);  //TODO:  Replace with async calls.

    AtomicLong txIDs = new AtomicLong(0); // TODO:  Put in ZooKeeper.
    long lastCommittedVersion = 0; // TODO:  Put in ZooKeeper.


    /**Instantiates a Broker curator acting as a client for the underlying ZooKeeper service.
     * Retrieves the Coordinator's location from ZK and opens a channel with it.
     * Creates and starts a Shard Map Update Daemon thread and a Query Statistic Daemon Thread.
     * TODO: Retry routine and error handling when the Broker cannot find the Coordinator
     * TODO: Figure out what to do with the query engine
     *
     * @param queryEngine not used
     * @param zkHost IP address of the ZooKeeper service
     * @param zkPort port assigned to the ZooKeeper service
     * */
    public Broker(String zkHost, int zkPort, QueryEngine queryEngine) {
        this.queryEngine = queryEngine;
        this.zkCurator = new BrokerCurator(zkHost, zkPort);
        Optional<Pair<String, Integer>> masterHostPort = zkCurator.getMasterLocation();
        String masterHost = null;
        Integer masterPort = null;
        if (masterHostPort.isPresent()) {
            masterHost = masterHostPort.get().getValue0();
            masterPort = masterHostPort.get().getValue1();
        } else {
            logger.error("Broker could not find master");
        }
        ManagedChannel channel = ManagedChannelBuilder.forAddress(masterHost, masterPort).usePlaintext().build();
        coordinatorBlockingStub = BrokerCoordinatorGrpc.newBlockingStub(channel);
        consistentHash = zkCurator.getConsistentHashFunction();
        shardMapUpdateDaemon = new ShardMapUpdateDaemon();
        shardMapUpdateDaemon.start();
        queryStatisticsDaemon = new QueryStatisticsDaemon();
        queryStatisticsDaemon.start();
    }
    /**Stops the ShardMapUpdate and QueryStatistic daemon threads, shuts down all channels with datastores and coordinator
     * then closes the Curator client with ZooKeeper and shuts down the thread pool*/
    public void shutdown() {
        runShardMapUpdateDaemon = false;
        runQueryStatisticsDaemon = false;
        try {
            shardMapUpdateDaemon.join();
            queryStatisticsDaemon.interrupt();
            queryStatisticsDaemon.join();
        } catch (InterruptedException ignored) {}
        // TODO:  Synchronize with outstanding queries?
        ((ManagedChannel) this.coordinatorBlockingStub.getChannel()).shutdownNow();
        for (ManagedChannel c: dsIDToChannelMap.values()) {
            c.shutdownNow();
        }
        int numQueries = remoteExecutionTimes.size();
        if (numQueries > 0) {
            long p50RE = remoteExecutionTimes.stream().mapToLong(i -> i).sorted().toArray()[remoteExecutionTimes.size() / 2];
            long p99RE = remoteExecutionTimes.stream().mapToLong(i -> i).sorted().toArray()[remoteExecutionTimes.size() * 99 / 100];
            long p50agg = aggregationTimes.stream().mapToLong(i -> i).sorted().toArray()[aggregationTimes.size() / 2];
            long p99agg = aggregationTimes.stream().mapToLong(i -> i).sorted().toArray()[aggregationTimes.size() * 99 / 100];
            logger.info("Queries: {} p50 Remote: {}μs p99 Remote: {}μs  p50 Aggregation: {}μs p99 Aggregation: {}μs", numQueries, p50RE, p99RE, p50agg, p99agg);
        }
        zkCurator.close();
        readQueryThreadPool.shutdown();
    }

    /*
     * PUBLIC FUNCTIONS
     *
     * Table creation and query execution. Each query function takes the query plan.
     *
     * bool createTable (tableName, numShards)
     * <Row, Shard> boolean writeQuery(WriteQueryPlan<R,S>, rows)
     * <Row, Shard> boolean simpleWriteQuery(SimpleWriteQueryPlan<R,S>, rows)
     * <Shard, V> V anchoredReadQuery(AnchoredReadQueryPlan<S,V>)
     * <Shard, V> V shuffleReadQuery(ShuffleReadQueryPlan<S,V>)
     */

    /**Creates a table with the specified name and number of shards by invoking the Coordinator's method.
     *
     * @param tableName The name of the table to be created
     * @param numShards The number of shards the table will have
     * @return true if and only if the specified table name has not been associated with any previously created table, false otherwise
     * */
    public boolean createTable(String tableName, int numShards) {
        CreateTableMessage m = CreateTableMessage.newBuilder().setTableName(tableName).setNumShards(numShards).build();
        CreateTableResponse r = coordinatorBlockingStub.createTable(m);
        return r.getReturnCode() == QUERY_SUCCESS;
    }
    /**Executes the write query plan on the given rows in a 2PC fashion. It starts a write query thread for each shard contaning any of
     * the given rows. A primary datastore will be randomly selected between those hosting a replica of a shard involved
     * in the query.
     * @param rows the list of rows to be written
     * @param writeQueryPlan the plan specifying how the operations have to be carried out
     * @return true if and only if the query has been executed successfully*/
    public <R extends Row, S extends Shard> boolean writeQuery(WriteQueryPlan<R, S> writeQueryPlan, List<R> rows) {
        zkCurator.acquireWriteLock(); // TODO: Maybe acquire later?
        long tStart = System.currentTimeMillis();
        Map<Integer, List<R>> shardRowListMap = new HashMap<>();
        TableInfo tableInfo = getTableInfo(writeQueryPlan.getQueriedTable());
        for (R row: rows) {
            int partitionKey = row.getPartitionKey();
            assert(partitionKey >= 0);
            int shard = keyToShard(tableInfo.id, tableInfo.numShards, partitionKey);
            shardRowListMap.computeIfAbsent(shard, (k -> new ArrayList<>())).add(row);
        }
        Map<Integer, R[]> shardRowArrayMap =
                shardRowListMap
                        .entrySet()
                        .stream()
                        .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().toArray((R[]) new Row[0])));
        List<WriteQueryThread<R, S>> writeQueryThreads = new ArrayList<>();
        long txID = txIDs.getAndIncrement();
        CountDownLatch queryLatch = new CountDownLatch(shardRowArrayMap.size());
        AtomicInteger queryStatus = new AtomicInteger(QUERY_SUCCESS);
        AtomicBoolean statusWritten = new AtomicBoolean(false);
        for (Integer shardNum: shardRowArrayMap.keySet()) {
            R[] rowArray = shardRowArrayMap.get(shardNum);
            WriteQueryThread<R, S> t = new WriteQueryThread<>(
                    shardNum,
                    writeQueryPlan,
                    rowArray,
                    txID,
                    queryLatch,
                    queryStatus,
                    statusWritten);
            t.start();
            writeQueryThreads.add(t);
        }
        for (WriteQueryThread<R, S> t: writeQueryThreads) {
            try {
                t.join();
            } catch (InterruptedException e) {
                logger.error("Write query interrupted: {}", e.getMessage());
                assert(false);
            }
        }
        lastCommittedVersion = txID;
        logger.info("Write completed. Rows: {}. Version: {} Time: {}ms", rows.size(), lastCommittedVersion,
                System.currentTimeMillis() - tStart);
        assert (queryStatus.get() != QUERY_RETRY);
        zkCurator.releaseWriteLock();
        return queryStatus.get() == QUERY_SUCCESS;
    }
    /**Executes an eventually consistent write query of the given rows, following the procedure specified in the given
     * query plan. It starts a simple write query thread for all shards involved in the query that will randomly select
     * a primary datastore hosting the shard which is also responsible for the write replication.
     * @param writeQueryPlan the write query plan implementing the single-node write logic
     * @param rows the list of rows to be written
     * @return true if the primary datastore has successfully executed the query. No guarantees on how many replicas
     * have executed the query are given.
     * */
    public <R extends Row, S extends Shard> boolean simpleWriteQuery(SimpleWriteQueryPlan<R, S> writeQueryPlan, List<R> rows) {
        long tStart = System.currentTimeMillis();
        Map<Integer, List<R>> shardRowListMap = new HashMap<>();
        TableInfo tableInfo = getTableInfo(writeQueryPlan.getQueriedTable());
        for (R row: rows) {
            int partitionKey = row.getPartitionKey();
            assert(partitionKey >= 0);
            int shard = keyToShard(tableInfo.id, tableInfo.numShards, partitionKey);
            shardRowListMap.computeIfAbsent(shard, (k -> new ArrayList<>())).add(row);
        }
        Map<Integer, R[]> shardRowArrayMap = shardRowListMap.entrySet().stream().
                collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().toArray((R[]) new Row[0])));
        List<SimpleWriteQueryThread<R, S>> writeQueryThreads = new ArrayList<>();
        long txID = txIDs.getAndIncrement();
        AtomicInteger queryStatus = new AtomicInteger(QUERY_SUCCESS);
        AtomicBoolean statusWritten = new AtomicBoolean(false);
        for (Integer shardNum: shardRowArrayMap.keySet()) {
            R[] rowArray = shardRowArrayMap.get(shardNum);
            SimpleWriteQueryThread<R, S> t = new SimpleWriteQueryThread<>(shardNum, writeQueryPlan, rowArray, txID, queryStatus, statusWritten);
            t.start();
            writeQueryThreads.add(t);
        }
        for (SimpleWriteQueryThread<R, S> t: writeQueryThreads) {
            try {
                t.join();
            } catch (InterruptedException e) {
                logger.error("SimpleWrite query interrupted: {}", e.getMessage());
                assert(false);
            }
        }
        lastCommittedVersion = txID;
        logger.info("SimpleWritne completed. Rows: {}. Version: {} Time: {}ms", rows.size(), lastCommittedVersion,
                System.currentTimeMillis() - tStart);
        assert (queryStatus.get() != QUERY_RETRY);
        return queryStatus.get() == QUERY_SUCCESS;
    }

    /**Executes an AnchoredReadQueryPlan
     *
     * Calls the remote BrokerDataStore service anchoredReadQuery for each shard of the anchor table.
     * <p></p>
     * Each call will trigger the execution of a scatter-gather routine, whose results are returned
     * either as a ByteString or as a location of an intermediateShard.
     * - if the query plan being executed returns an aggregate value, the user-defined combine method is executed
     *      passing the previously obtained result of the scatter-gather routine
     * - if the query returns a shard (i.e. it is a sub query), then, each the objects returned by the S-G routine
     *      represent intermediate shards locations to be returned to the caller, since those are results of
     *      sub-queries.
     * */
    public <S extends Shard, V> V anchoredReadQuery(AnchoredReadQueryPlan<S, V> plan) {
        long txID = txIDs.getAndIncrement();
        Map<String, List<Integer>> partitionKeys = plan.keysForQuery();
        HashMap<String, List<Integer>> targetShards = new HashMap<>();

        /* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
        * Build a Map<TableName, List<ShardsIDs to be queried relative to the table>>                                  *
        * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

        for(Map.Entry<String, List<Integer>> entry: partitionKeys.entrySet()) {
            String tableName = entry.getKey();
            List<Integer> tablePartitionKeys = entry.getValue();
            TableInfo tableInfo = getTableInfo(tableName);
            int tableID = tableInfo.id;
            int numShards = tableInfo.numShards;
            List<Integer> shardNums;
            if (tablePartitionKeys.contains(-1)) {
                // -1 is a wildcard--run on all shards.
                shardNums = IntStream.range(tableID * SHARDS_PER_TABLE, tableID * SHARDS_PER_TABLE + numShards)
                        .boxed().collect(Collectors.toList());
            } else {
                shardNums = tablePartitionKeys.stream().map(i -> keyToShard(tableID, numShards, i))
                        .distinct().collect(Collectors.toList());
            }
            targetShards.put(tableName, shardNums);
        }

        /* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
        * Recursively executes all sub queries, placing their results in a structure                                     *
        * Map<TableName, Map<ShardID to be queried on the table, datastoreID storing the shard>>. (one table, one shard) *
        * The targetShards mapping is updated with these intermediate results                                            *
        * Map<TableName, List<ShardIDs to be queried on the table>>                                                      *
        * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

        HashMap<String, Map<Integer, Integer>> intermediateShards = new HashMap<>();
        for(AnchoredReadQueryPlan<S, Map<String, Map<Integer, Integer>>> p: plan.getSubQueries()) {
            Map<String, Map<Integer, Integer>> subQueryShards = anchoredReadQuery(p);
            intermediateShards.putAll(subQueryShards);
            subQueryShards.forEach((k, v) -> targetShards.put(k, new ArrayList<>(v.keySet())));
        }


        ByteString serializedTargetShards = Utilities.objectToByteString(targetShards);
        ByteString serializedIntermediateShards = Utilities.objectToByteString(intermediateShards);
        ByteString serializedQuery = Utilities.objectToByteString(plan);
        String anchorTable = plan.getAnchorTable();
        List<Integer> anchorTableShards = targetShards.get(anchorTable);
        int numRepartitions = anchorTableShards.size();
        List<ByteString> intermediates = new CopyOnWriteArrayList<>();

        /* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
        * The anchoredReadQuery service of the Broker-Datastore is called for each shard to be queried of the          *
        * anchor table. The datastore is randomly selected among those hosting the shard.                              *
        * The results of the call are stored in the "intermediates" list of ByteStrings.                               *
        *                                                                                                              *
        * This is a blocking phase, it ends once an answer has been received from all calls related to each shard of   *
        * the anchor table.                                                                                            *
        * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

        CountDownLatch latch = new CountDownLatch(numRepartitions);
        long lcv = lastCommittedVersion;
        for (int anchorShardNum: anchorTableShards) {
            int dsID = consistentHash.getRandomBucket(anchorShardNum);
            ManagedChannel channel = dsIDToChannelMap.get(dsID);
            BrokerDataStoreGrpc.BrokerDataStoreStub stub = BrokerDataStoreGrpc.newStub(channel);
            AnchoredReadQueryMessage m = AnchoredReadQueryMessage.newBuilder().
                    setTargetShard(anchorShardNum).
                    setSerializedQuery(serializedQuery).
                    setNumRepartitions(numRepartitions).
                    setTxID(txID).
                    setLastCommittedVersion(lcv).
                    setTargetShards(serializedTargetShards).
                    setIntermediateShards(serializedIntermediateShards).
                    build();
            StreamObserver<AnchoredReadQueryResponse> responseObserver = new StreamObserver<>() {

                private void retry() {
                    shardMapUpdateDaemon.updateMap();
                    int newDSID = consistentHash.getRandomBucket(anchorShardNum);
                    ManagedChannel channel = dsIDToChannelMap.get(newDSID);
                    BrokerDataStoreGrpc.BrokerDataStoreStub stub = BrokerDataStoreGrpc.newStub(channel);
                    stub.anchoredReadQuery(m, this);
                }

                @Override
                public void onNext(AnchoredReadQueryResponse r) {
                    if (r.getReturnCode() == QUERY_RETRY) {
                        logger.warn("Got QUERY_RETRY from DS{}", dsID);
                        retry();
                    } else {
                        assert (r.getReturnCode() == QUERY_SUCCESS);
                        intermediates.add(r.getResponse());
                        latch.countDown();
                    }
                }

                @Override
                public void onError(Throwable throwable) {
                    logger.warn("Read Query Error on DS{}: {}", dsID, throwable.getMessage());
                    retry();
                }

                @Override
                public void onCompleted() {
                }
            };
            stub.anchoredReadQuery(m, responseObserver);
        }
        try {
            latch.await();
        } catch (InterruptedException ignored) { }

        long aggStart = System.nanoTime();
        V ret;

        /* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
         * The "returnTableName" method returns an empty object if the query must return an aggregate value. The same  *
         * method will return a table name if the query must return a shard (like the subqueries that store their      *
         * results in shards across the whole system's datastores and tables). This is here checked and:               *
         *                                                                                                             *
         * - if the query returns an aggregate value, then the method simply combines all retrieved "intermediates"    *
         *      from all the previous calls related to each anchor shard.                                              *
         * - if the query returns a shard (i.e. is a subquery), then, each intermediate ByteString object represents   *
         *      a Map<Integer, Integer>, which is extracted and placed in a list called shardLocations.                *
         *      Each map present in this list (i.e. each result of each anchor-shard query) is iterated on, and        *
         *      each integer pair is placed into the combinedShardLocations mapping. In other words, all mappings are  *
         *      merged. TODO: figure out what these pairs actually represent, they are returned by the remote call.    *
         *      A Map<TableNames, Map<Integer, Integer>> containing only the entry related to the return table is      *
         *      built and has value equal to the combinedShardLocation mapping.                                        *
         *      This single-value-map is casted to the type returned by the query.                                     *
         * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

        if (plan.returnTableName().isEmpty()) {
            ret = plan.combine(intermediates);
        } else {
            List<Map<Integer, Integer>> shardLocations = intermediates.stream()
                    .map(i -> (Map<Integer, Integer>) Utilities.byteStringToObject(i))
                    .collect(Collectors.toList());
            Map<Integer, Integer> combinedShardLocations = new HashMap<>();
            shardLocations.forEach(i -> i.forEach(combinedShardLocations::put));
            Map<String, Map<Integer, Integer>> s = Map.of(plan.returnTableName().get(), combinedShardLocations);
            intermediateShards.putAll(s);
            ret = (V) intermediateShards;
        }

        /* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
        * Updates aggregationTimes mapping with the time interval related to the execution of this read query before   *
        * returning the result (shard or value) built in the previous phase.                                           *
        * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

        long aggEnd = System.nanoTime();
        aggregationTimes.add((aggEnd - aggStart) / 1000L);
        return ret;
    }

    public <S extends Shard, V> V shuffleReadQuery(ShuffleReadQueryPlan<S, V> plan) {
        long txID = txIDs.getAndIncrement();
        Map<String, List<Integer>> partitionKeys = plan.keysForQuery();
        HashMap<String, List<Integer>> targetShards = new HashMap<>();
        for (Map.Entry<String, List<Integer>> entry : partitionKeys.entrySet()) {
            String tableName = entry.getKey();
            List<Integer> tablePartitionKeys = entry.getValue();
            TableInfo tableInfo = getTableInfo(tableName);
            int tableID = tableInfo.id;
            int numShards = tableInfo.numShards;
            List<Integer> shardNums;
            if (tablePartitionKeys.contains(-1)) {
                // -1 is a wildcard--run on all shards.
                shardNums = IntStream.range(tableID * SHARDS_PER_TABLE, tableID * SHARDS_PER_TABLE + numShards)
                        .boxed().collect(Collectors.toList());
            } else {
                shardNums = tablePartitionKeys.stream().map(i -> keyToShard(tableID, numShards, i))
                        .distinct().collect(Collectors.toList());
            }
            targetShards.put(tableName, shardNums);
        }
        ByteString serializedTargetShards = Utilities.objectToByteString(targetShards);
        ByteString serializedQuery = Utilities.objectToByteString(plan);
        Set<Integer> dsIDs = dsIDToChannelMap.keySet();
        int numReducers = dsIDs.size();
        List<ByteString> intermediates = new CopyOnWriteArrayList<>();
        CountDownLatch latch = new CountDownLatch(numReducers);
        int reducerNum = 0;
        for (int dsID : dsIDs) {
            ManagedChannel channel = dsIDToChannelMap.get(dsID);
            BrokerDataStoreGrpc.BrokerDataStoreStub stub = BrokerDataStoreGrpc.newStub(channel);
            ShuffleReadQueryMessage m = ShuffleReadQueryMessage.newBuilder().
                    setRepartitionNum(reducerNum).
                    setSerializedQuery(serializedQuery).
                    setNumRepartitions(numReducers).
                    setTxID(txID).
                    setTargetShards(serializedTargetShards).
                    build();
            reducerNum++;
            StreamObserver<ShuffleReadQueryResponse> responseObserver = new StreamObserver<>() {
                @Override
                public void onNext(ShuffleReadQueryResponse r) {
                    assert (r.getReturnCode() == Broker.QUERY_SUCCESS);
                    intermediates.add(r.getResponse());
                }

                @Override
                public void onError(Throwable throwable) {
                    logger.warn("Read Query Error on DS{}: {}", dsID, throwable.getMessage());
                    int numDSIDs = dsIDToChannelMap.keySet().size();
                    Integer newDSID = dsIDToChannelMap.keySet().stream().skip(new Random().nextInt(numDSIDs)).findFirst().orElse(null);
                    ManagedChannel channel = dsIDToChannelMap.get(newDSID);
                    BrokerDataStoreGrpc.BrokerDataStoreStub stub = BrokerDataStoreGrpc.newStub(channel);
                    stub.shuffleReadQuery(m, this);
                }

                @Override
                public void onCompleted() {
                    latch.countDown();
                }
            };
            stub.shuffleReadQuery(m, responseObserver);
        }
        try {
            latch.await();
        } catch (InterruptedException ignored) {
        }
        long aggStart = System.nanoTime();
        V ret = plan.combine(intermediates);
        long aggEnd = System.nanoTime();
        aggregationTimes.add((aggEnd - aggStart) / 1000L);
        return ret;
    }

    /**Extracts all shards involved in the query and for each of them starts a thread that will select a random datastore hosting the
     * shard. The datastore will apply the transformation to the rows contained in the shard it hosts and will then write the results
     * before replicating them with eventually consistent guarantees.
     * The transformation is carried out by the randomly selected primary datastore, ensuring that (1) the load is split
     * across different data stores and (2) no transformation is performed twice since it is carried out before replication.
     * TODO: test this.
     * @param transformQueryPlan the query plan specifying the write and transform operations
     * @param rows the rows to be written across all shards
     * @return true if the query has been successfully executed
     * */
    public <R extends Row, S extends Shard> boolean simpleTransformSimpleWriteQuery(SimpleWriteSimpleTransform<R,S> transformQueryPlan, List<R> rows){
        long tStart = System.currentTimeMillis();
        Map<Integer, List<R>> shardRowListMap = new HashMap<>();
        TableInfo tableInfo = getTableInfo(transformQueryPlan.getQueriedTable());
        for (R row: rows) {
            int partitionKey = row.getPartitionKey();
            assert(partitionKey >= 0);
            int shard = keyToShard(tableInfo.id, tableInfo.numShards, partitionKey);
            shardRowListMap.computeIfAbsent(shard, (k -> new ArrayList<>())).add(row);
        }
        Map<Integer, R[]> shardRowArrayMap = shardRowListMap.entrySet().stream().
                collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().toArray((R[]) new Row[0])));
        List<SimpleWriteSimpleTransformThread<R, S>> writeQueryThreads = new ArrayList<>();
        long txID = txIDs.getAndIncrement();
        AtomicInteger queryStatus = new AtomicInteger(QUERY_SUCCESS);
        AtomicBoolean statusWritten = new AtomicBoolean(false);
        for (Integer shardNum: shardRowArrayMap.keySet()) {
            R[] rowArray = shardRowArrayMap.get(shardNum);
            SimpleWriteSimpleTransformThread<R, S> t = new SimpleWriteSimpleTransformThread<>(shardNum, transformQueryPlan, rowArray, txID, queryStatus, statusWritten);
            t.start();
            writeQueryThreads.add(t);
        }
        for (SimpleWriteSimpleTransformThread<R, S> t: writeQueryThreads) {
            try {
                t.join();
            } catch (InterruptedException e) {
                logger.error("SimpleWrite query interrupted: {}", e.getMessage());
                assert(false);
            }
        }
        lastCommittedVersion = txID;
        logger.info("SimpleWritne completed. Rows: {}. Version: {} Time: {}ms", rows.size(), lastCommittedVersion,
                System.currentTimeMillis() - tStart);
        assert (queryStatus.get() != QUERY_RETRY);
        return queryStatus.get() == QUERY_SUCCESS;
    }
    /*
     * PRIVATE FUNCTIONS
     */

    /**Retrieves a TableInfo object associated with the given table name.
     * TODO: Error handling should be not be carried out via assert statements
     * @param tableName The name of the queried table
     * @return The table info object associated with the given name
     * */
    private TableInfo getTableInfo(String tableName) {
        if (tableInfoMap.containsKey(tableName)) {
            return tableInfoMap.get(tableName);
        } else {
            TableInfoResponse r = coordinatorBlockingStub.
                    tableInfo(TableInfoMessage.newBuilder().setTableName(tableName).build());
            assert(r.getReturnCode() == QUERY_SUCCESS);
            TableInfo t = new TableInfo(tableName, r.getId(), r.getNumShards());
            tableInfoMap.put(tableName, t);
            return t;
        }
    }
    /**Given a Table identifier, its number of shards and a row's partition key, returns the shard identifier
     * associated with the table's shard containing the row having given partition key
     * @param tableID The queried table identifier
     * @param numShards the number of shards the table has
     * @param partitionKey the partition key of the row to be retrieved
     * @return the shard identifier hosting the given row in the given table*/
    private static int keyToShard(int tableID, int numShards, int partitionKey) {
        return tableID * SHARDS_PER_TABLE + (partitionKey % numShards);
    }
    /**Retrieve a blocking stub for communication between the Broker object calling the method and a randomly selected
     * datastore hosting the given shard identifier
     * @param shard the shard identifier for which a stub is needed
     * @return a blocking stub for the broker-datastore service of a random datastore storing the given shard*/
    private BrokerDataStoreGrpc.BrokerDataStoreBlockingStub getStubForShard(int shard) {
        int dsID = consistentHash.getRandomBucket(shard);
        ManagedChannel channel = dsIDToChannelMap.get(dsID);
        assert(channel != null);
        return BrokerDataStoreGrpc.newBlockingStub(channel);
    }


    /**Sends the statistics to the Coordinator via the appropriate Broker-Coordinator service*/
    public void sendStatisticsToCoordinator() {
        ByteString queryStatisticsSer = Utilities.objectToByteString(queryStatistics);
        QueryStatisticsMessage m = QueryStatisticsMessage.newBuilder().setQueryStatistics(queryStatisticsSer).build();
        QueryStatisticsResponse r = coordinatorBlockingStub.queryStatistics(m);
    }

    /**Periodically sends statistics to the Coordinator*/
    private class QueryStatisticsDaemon extends Thread {
        @Override
        public void run() {
            while (runQueryStatisticsDaemon) {
                sendStatisticsToCoordinator();
                queryStatistics = new ConcurrentHashMap<>();
                try {
                    Thread.sleep(queryStatisticsDaemonSleepDurationMillis);
                } catch (InterruptedException e) {
                    break;
                }
            }
        }
    }
    /**Periodically updates the dsIDToChannelMap and the consistentHash attributes.
     * The first is updated by virtue of the datastores identifiers being monotonically increasing, the latter is
     * updated by retrieving the value from the ZooKeeper service*/
    private class ShardMapUpdateDaemon extends Thread {
        private void updateMap() {
            ConsistentHash consistentHash = zkCurator.getConsistentHashFunction();
            Map<Integer, ManagedChannel> dsIDToChannelMap = new HashMap<>();
            int dsID = 0;
            while (true) {
                DataStoreDescription d = zkCurator.getDSDescriptionFromDSID(dsID);
                if (d == null) {
                    break;
                } else if (d.status.get() == DataStoreDescription.ALIVE) {
                    ManagedChannel channel = Broker.this.dsIDToChannelMap.containsKey(dsID) ?
                            Broker.this.dsIDToChannelMap.get(dsID) :
                            ManagedChannelBuilder.forAddress(d.host, d.port).usePlaintext().build();
                    dsIDToChannelMap.put(dsID, channel);
                } else if (d.status.get() == DataStoreDescription.DEAD) {
                    if (Broker.this.dsIDToChannelMap.containsKey(dsID)) {
                        Broker.this.dsIDToChannelMap.get(dsID).shutdown();
                    }
                }
                dsID++;
            }
            Broker.this.dsIDToChannelMap = dsIDToChannelMap;
            Broker.this.consistentHash = consistentHash;
        }

        @Override
        public void run() {
            while (runShardMapUpdateDaemon) {
                updateMap();
                try {
                    Thread.sleep(shardMapDaemonSleepDurationMillis);
                } catch (InterruptedException e) {
                    break;
                }
            }
        }

        @Override
        public synchronized void start() {
            updateMap();
            super.start();
        }
    }
    /**An object of this class is created for each shard involved in a write query thread. This thread is responsible
     * for randomly selecting a random datastore that will act as primary datastore for the shard being queried among
     * those datastores hosting the shard and executing the query itself while also monitoring the various phases
     * */
    private class WriteQueryThread<R extends Row, S extends Shard> extends Thread {
        private final int shardNum;
        private final WriteQueryPlan<R, S> writeQueryPlan;
        private final R[] rowArray;
        private final long txID;
        private CountDownLatch queryLatch;
        private AtomicInteger queryStatus;
        private AtomicBoolean statusWritten;

        WriteQueryThread(int shardNum, WriteQueryPlan<R, S> writeQueryPlan, R[] rowArray, long txID,
                         CountDownLatch queryLatch, AtomicInteger queryStatus, AtomicBoolean statusWritten) {
            this.shardNum = shardNum;
            this.writeQueryPlan = writeQueryPlan;
            this.rowArray = rowArray;
            this.txID = txID;
            this.queryLatch = queryLatch;
            this.queryStatus = queryStatus;
            this.statusWritten = statusWritten;
        }

        @Override
        public void run() { writeQuery(); }

        private void writeQuery() {
            AtomicInteger subQueryStatus = new AtomicInteger(QUERY_RETRY);
            while (subQueryStatus.get() == QUERY_RETRY) {
                BrokerDataStoreGrpc.BrokerDataStoreBlockingStub blockingStub = getStubForShard(shardNum);
                BrokerDataStoreGrpc.BrokerDataStoreStub stub = BrokerDataStoreGrpc.newStub(blockingStub.getChannel());
                final CountDownLatch prepareLatch = new CountDownLatch(1);
                final CountDownLatch finishLatch = new CountDownLatch(1);
                StreamObserver<WriteQueryMessage> observer =
                        stub.writeQuery(new StreamObserver<>() {
                            @Override
                            public void onNext(WriteQueryResponse writeQueryResponse) {
                                subQueryStatus.set(writeQueryResponse.getReturnCode());
                                prepareLatch.countDown();
                            }

                            @Override
                            public void onError(Throwable th) {
                                logger.warn("Write query RPC failed for shard {}", shardNum);
                                subQueryStatus.set(QUERY_FAILURE);
                                prepareLatch.countDown();
                                finishLatch.countDown();
                            }

                            @Override
                            public void onCompleted() {
                                finishLatch.countDown();
                            }
                        });
                final int STEPSIZE = 1000;
                for (int i = 0; i < rowArray.length; i += STEPSIZE) {
                    ByteString serializedQuery = Utilities.objectToByteString(writeQueryPlan);
                    R[] rowSlice = Arrays.copyOfRange(rowArray, i, Math.min(rowArray.length, i + STEPSIZE));
                    ByteString rowData = Utilities.objectToByteString(rowSlice);
                    WriteQueryMessage rowMessage = WriteQueryMessage.newBuilder()
                            .setShard(shardNum)
                            .setSerializedQuery(serializedQuery)
                            .setRowData(rowData)
                            .setTxID(txID)
                            .setWriteState(DataStore.COLLECT)
                            .build();
                    observer.onNext(rowMessage);
                }
                WriteQueryMessage prepare = WriteQueryMessage.newBuilder()
                        .setWriteState(DataStore.PREPARE)
                        .build();
                observer.onNext(prepare);
                try {
                    prepareLatch.await();
                } catch (InterruptedException e) {
                    logger.error("Write Interrupted: {}", e.getMessage());
                    assert (false);
                }
                if (subQueryStatus.get() == QUERY_RETRY) {
                    try {
                        observer.onCompleted();
                        Thread.sleep(shardMapDaemonSleepDurationMillis);
                        continue;
                    } catch (InterruptedException e) {
                        logger.error("Write Interrupted: {}", e.getMessage());
                        assert (false);
                    }
                }
                assert(subQueryStatus.get() != QUERY_RETRY);
                if (subQueryStatus.get() == QUERY_FAILURE) {
                    queryStatus.set(QUERY_FAILURE);
                }
                queryLatch.countDown();
                try {
                    queryLatch.await();
                } catch (InterruptedException e) {
                    logger.error("Write Interrupted: {}", e.getMessage());
                    assert (false);
                }
                assert(queryStatus.get() != QUERY_RETRY);
                if (queryStatus.get() == QUERY_SUCCESS) {
                    // TODO:  This must finish before any commit message is sent.
                    if (statusWritten.compareAndSet(false, true)) {
                        zkCurator.writeTransactionStatus(txID, DataStore.COMMIT);
                    }
                    WriteQueryMessage commit = WriteQueryMessage.newBuilder()
                            .setWriteState(DataStore.COMMIT)
                            .build();
                    observer.onNext(commit);
                } else if (queryStatus.get() == QUERY_FAILURE) {
                    WriteQueryMessage abort = WriteQueryMessage.newBuilder()
                            .setWriteState(DataStore.ABORT)
                            .build();
                    observer.onNext(abort);
                }
                observer.onCompleted();
                try {
                    finishLatch.await();
                } catch (InterruptedException e) {
                    logger.error("Write Interrupted: {}", e.getMessage());
                    assert (false);
                }
            }
        }
    }
    /**Manages an eventually consistent write query of a single shard by communicating with a randomly selected primary
     * datastore for the particular shard
     * */
    private class SimpleWriteQueryThread<R extends Row, S extends Shard> extends Thread {
        private final int shardNum;
        private final SimpleWriteQueryPlan<R, S> writeQueryPlan;
        private final R[] rowArray;
        private final long txID;
        private AtomicInteger queryStatus;
        private AtomicBoolean statusWritten;

        SimpleWriteQueryThread(int shardNum, SimpleWriteQueryPlan<R, S> writeQueryPlan, R[] rowArray, long txID,
                         AtomicInteger queryStatus, AtomicBoolean statusWritten) {
            this.shardNum = shardNum;
            this.writeQueryPlan = writeQueryPlan;
            this.rowArray = rowArray;
            this.txID = txID;
            this.queryStatus = queryStatus;
            this.statusWritten = statusWritten;
        }

        @Override
        public void run() { writeQuery(); }

        private void writeQuery() {
            AtomicInteger subQueryStatus = new AtomicInteger(QUERY_RETRY);
            while (subQueryStatus.get() == QUERY_RETRY) {
                BrokerDataStoreGrpc.BrokerDataStoreBlockingStub blockingStub = getStubForShard(shardNum);
                BrokerDataStoreGrpc.BrokerDataStoreStub stub = BrokerDataStoreGrpc.newStub(blockingStub.getChannel());
                final CountDownLatch prepareLatch = new CountDownLatch(1);
                final CountDownLatch finishLatch = new CountDownLatch(1);
                StreamObserver<WriteQueryMessage> observer =
                        stub.simpleWriteQuery(new StreamObserver<>() {
                            @Override
                            public void onNext(WriteQueryResponse writeQueryResponse) {
                                subQueryStatus.set(writeQueryResponse.getReturnCode());
                                prepareLatch.countDown();
                            }

                            @Override
                            public void onError(Throwable th) {
                                logger.warn("SimpleWrite query RPC failed for shard {}", shardNum);
                                subQueryStatus.set(QUERY_FAILURE);
                                prepareLatch.countDown();
                                finishLatch.countDown();
                            }

                            @Override
                            public void onCompleted() {
                                finishLatch.countDown();
                            }
                        });
                final int STEPSIZE = 1000;
                for (int i = 0; i < rowArray.length; i += STEPSIZE) {
                    ByteString serializedQuery = Utilities.objectToByteString(writeQueryPlan);
                    R[] rowSlice = Arrays.copyOfRange(rowArray, i, Math.min(rowArray.length, i + STEPSIZE));
                    ByteString rowData = Utilities.objectToByteString(rowSlice);
                    WriteQueryMessage rowMessage = WriteQueryMessage.newBuilder()
                            .setShard(shardNum)
                            .setSerializedQuery(serializedQuery)
                            .setRowData(rowData)
                            .setTxID(txID)
                            .setWriteState(DataStore.COLLECT)
                            .build();
                    observer.onNext(rowMessage);
                }
                WriteQueryMessage prepare = WriteQueryMessage.newBuilder()
                        .setWriteState(DataStore.PREPARE)
                        .build();
                observer.onNext(prepare);
                try {
                    prepareLatch.await();
                } catch (InterruptedException e) {
                    logger.error("SimpleWrite Interrupted: {}", e.getMessage());
                    assert (false);
                }
                if (subQueryStatus.get() == QUERY_RETRY) {
                    try {
                        observer.onCompleted();
                        Thread.sleep(shardMapDaemonSleepDurationMillis);
                        continue;
                    } catch (InterruptedException e) {
                        logger.error("SimpleWrite Interrupted: {}", e.getMessage());
                        assert (false);
                    }
                }
                assert(subQueryStatus.get() != QUERY_RETRY);
                if (subQueryStatus.get() == QUERY_FAILURE) {
                    queryStatus.set(QUERY_FAILURE);
                }
                assert(queryStatus.get() != QUERY_RETRY);
                observer.onCompleted();
                try {
                    finishLatch.await();
                } catch (InterruptedException e) {
                    logger.error("SimpleWrite Interrupted: {}", e.getMessage());
                    assert (false);
                }
            }
        }
    }

    /**Manages an eventually consistent write with row transformation query of a single shard by communicating with a
     * randomly selected primary datastore.
     * */
    private class SimpleWriteSimpleTransformThread<R extends Row, S extends Shard> extends Thread{
        private final int shardNum;
        private final SimpleWriteSimpleTransform<R, S> writeQueryPlan;
        private final R[] rowArray;
        private final long txID;
        private AtomicInteger queryStatus;
        private AtomicBoolean statusWritten;

        SimpleWriteSimpleTransformThread(int shardNum, SimpleWriteSimpleTransform<R, S> writeQueryPlan, R[] rowArray, long txID,
                               AtomicInteger queryStatus, AtomicBoolean statusWritten) {
            this.shardNum = shardNum;
            this.writeQueryPlan = writeQueryPlan;
            this.rowArray = rowArray;
            this.txID = txID;
            this.queryStatus = queryStatus;
            this.statusWritten = statusWritten;
        }

        @Override
        public void run() { simpleWriteSimpleTransform(); }

        private void simpleWriteSimpleTransform() {
            AtomicInteger subQueryStatus = new AtomicInteger(QUERY_RETRY);
            while (subQueryStatus.get() == QUERY_RETRY) {
                BrokerDataStoreGrpc.BrokerDataStoreBlockingStub blockingStub = getStubForShard(shardNum);
                BrokerDataStoreGrpc.BrokerDataStoreStub stub = BrokerDataStoreGrpc.newStub(blockingStub.getChannel());
                final CountDownLatch prepareLatch = new CountDownLatch(1);
                final CountDownLatch finishLatch = new CountDownLatch(1);
                StreamObserver<WriteSimpleTransformMessage> observer =
                        stub.simpleWriteSimpleTransform(new StreamObserver<>() {
                            @Override
                            public void onNext(WriteSimpleTransformResponse writeQueryResponse) {
                                subQueryStatus.set(writeQueryResponse.getReturnCode());
                                prepareLatch.countDown();
                            }

                            @Override
                            public void onError(Throwable th) {
                                logger.warn("SimpleWrite query RPC failed for shard {}", shardNum);
                                subQueryStatus.set(QUERY_FAILURE);
                                prepareLatch.countDown();
                                finishLatch.countDown();
                            }

                            @Override
                            public void onCompleted() {
                                finishLatch.countDown();
                            }
                        });
                final int STEPSIZE = 1000;
                for (int i = 0; i < rowArray.length; i += STEPSIZE) {
                    ByteString serializedQuery = Utilities.objectToByteString(writeQueryPlan);
                    R[] rowSlice = Arrays.copyOfRange(rowArray, i, Math.min(rowArray.length, i + STEPSIZE));
                    ByteString rowData = Utilities.objectToByteString(rowSlice);
                    WriteSimpleTransformMessage rowMessage = WriteSimpleTransformMessage.newBuilder()
                            .setShard(shardNum)
                            .setSerializedQuery(serializedQuery)
                            .setRowData(rowData)
                            .setTxID(txID)
                            .setWriteState(DataStore.COLLECT)
                            .build();
                    observer.onNext(rowMessage);
                }
                WriteSimpleTransformMessage prepare = WriteSimpleTransformMessage.newBuilder()
                        .setWriteState(DataStore.PREPARE)
                        .build();
                observer.onNext(prepare);
                try {
                    prepareLatch.await();
                } catch (InterruptedException e) {
                    logger.error("SimpleWriteSimpleTransform Interrupted: {}", e.getMessage());
                    assert (false);
                }
                if (subQueryStatus.get() == QUERY_RETRY) {
                    try {
                        observer.onCompleted();
                        Thread.sleep(shardMapDaemonSleepDurationMillis);
                        continue;
                    } catch (InterruptedException e) {
                        logger.error("SimpleWriteSimpleTransform Interrupted: {}", e.getMessage());
                        assert (false);
                    }
                }
                assert(subQueryStatus.get() != QUERY_RETRY);
                if (subQueryStatus.get() == QUERY_FAILURE) {
                    queryStatus.set(QUERY_FAILURE);
                }
                assert(queryStatus.get() != QUERY_RETRY);
                observer.onCompleted();
                try {
                    finishLatch.await();
                } catch (InterruptedException e) {
                    logger.error("SimpleWriteSimpleTransform Interrupted: {}", e.getMessage());
                    assert (false);
                }
            }
        }
    }
}