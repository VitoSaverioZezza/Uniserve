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
    public static final int READ_NON_EXISTING_SHARD = 7;

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

    /*WRITE QUERIES*/
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
            SimpleWriteQueryThread<R, S> t = new SimpleWriteQueryThread<>(
                    shardNum,
                    writeQueryPlan,
                    rowArray,
                    txID,
                    queryStatus,
                    statusWritten
            );
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
        logger.info("SimpleWrite completed. Rows: {}. Version: {} Time: {}ms", rows.size(), lastCommittedVersion,
                System.currentTimeMillis() - tStart);
        assert (queryStatus.get() != QUERY_RETRY);
        return queryStatus.get() == QUERY_SUCCESS;
    }

    /*VOLATILE OPERATIONS*/
    public<V> V volatileShuffleQuery(VolatileShuffleQueryPlan<V> plan, List<Row> rows){
        long txID = txIDs.getAndIncrement();

        /* Map shardID to rows, this mapping will be used to determine which actor will store the volatile
         * raw data to be later shuffled */

        Map<Integer, List<Row>> shardRowListMap = new HashMap<>();
        TableInfo tableInfo = getTableInfo(plan.getQueriedTables());
        for (Row row: rows) {
            int partitionKey = row.getPartitionKey();
            assert(partitionKey >= 0);
            int shard = keyToShard(tableInfo.id, tableInfo.numShards, partitionKey);
            shardRowListMap.computeIfAbsent(shard, (k -> new ArrayList<>())).add(row);
        }
        Map<Integer, Row[]> shardRowArrayMap =
                shardRowListMap
                        .entrySet()
                        .stream()
                        .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().toArray((Row[]) new Row[0])));

        /* Send the raw data to the actors that will keep them in memory for later shuffling operation. The
         * ids of the actors storing the raw data are added to the dsIDsScatter list. Proceed once all data has
         * been forwarded. If one send operation failed, signal to all other actors to delete the volatile data
         * associated to this transaction and return. The raw data will be stored by the receiving actor as a List
         * of serialized arrays of Rows, the same ones prepared in the thread (List<Serialized(R[])>)
         * */

        List<StoreVolatileDataThread<Row>> storeVolatileDataThreads = new ArrayList<>();
        AtomicInteger queryStatus = new AtomicInteger(QUERY_SUCCESS);
        List<Integer> dsIDsScatter = new ArrayList<>();
        for (Integer shardNum: shardRowArrayMap.keySet()) {
            Row[] rowArray = shardRowArrayMap.get(shardNum);
            Integer dsID = consistentHash.getRandomBucket(shardNum);
            if(!dsIDsScatter.contains(dsID)){dsIDsScatter.add(dsID);}
            StoreVolatileDataThread<Row> t = new StoreVolatileDataThread<>(
                    dsID,
                    txID,
                    rowArray,
                    queryStatus
            );
            t.start();
            storeVolatileDataThreads.add(t);
        }
        for (StoreVolatileDataThread<Row> t: storeVolatileDataThreads) {
            try {
                t.join();
            } catch (InterruptedException e) {
                logger.error("Volatile shuffle query interrupted: {}", e.getMessage());
                assert(false);
            }
        }
        if(queryStatus.get() == Broker.QUERY_FAILURE){
            boolean successfulCleanup = volatileShuffleCleanup(dsIDsScatter, txID, null);
            if(!successfulCleanup){
                logger.error("Unsuccessful cleanup of shuffled data for transaction {}", txID);
                assert (false);
                queryStatus.set(Broker.QUERY_FAILURE);
            }
            return null;
        }

        /* Start the scatter operations by sending a signal and the query plan to all actors holding raw data.
         * Each scatter execution returns a Map<dsID, List<ByteString>>, and the data gets forwarded to the appropriate
         * actors. The ids of the actors storing shuffled data are returned to the broker and stored in the
         * gatherDSids list. Resume once all scatter operations have been carried out. If one fails, both the
         * shuffled and raw data associated to this transaction is deleted from the memory of the actors.
         * */

        List<ScatterVolatileDataThread<Row,V>> scatterVolatileDataThreads = new ArrayList<>();
        List<Integer> gatherDSids = Collections.synchronizedList(new ArrayList<>());
        for(Integer dsID: dsIDsScatter){
            ScatterVolatileDataThread<Row,V> t = new ScatterVolatileDataThread<Row,V>(dsID, txID, gatherDSids, queryStatus, plan);
            t.start();
            scatterVolatileDataThreads.add(t);
        }
        for(ScatterVolatileDataThread<Row,V> t: scatterVolatileDataThreads){
            try {
                t.join();
            }catch (InterruptedException e ){
                logger.error("Broker: Volatile scatter query interrupted: {}", e.getMessage());
                assert(false);
                queryStatus.set(Broker.QUERY_FAILURE);
            }
        }
        if(queryStatus.get()==Broker.QUERY_FAILURE){
            boolean successfulCleanup = volatileShuffleCleanup(dsIDsScatter, txID, gatherDSids);
            if(!successfulCleanup){
                logger.error("Broker: Unsuccessful cleanup of volatile data for unsuccessful transaction {} (scatter failed)", txID);
                assert (false);
            }
            return null;
        }

        /*All scatter operations have been successfully carried out and the shuffled data is stored in memory
         * by the actors that will execute the gather operation. The ids of the actors storing this data are stored in
         * the gatherDSids list. The Broker now triggers the gather operations and retrieves all results from
         * the actors. All actors must return before the eecution resumes.
         * If one gather fails, all volatile data is deleted.
         * */

        List<GatherVolatileDataThread<Row,V>> gatherVolatileDataThreads = new ArrayList<>();
        List<ByteString> gatherResults = Collections.synchronizedList(new ArrayList<>());
        for(Integer dsID: gatherDSids){
            GatherVolatileDataThread<Row,V> t = new GatherVolatileDataThread<Row,V>(dsID, txID, gatherResults, queryStatus, plan);
            t.start();
            gatherVolatileDataThreads.add(t);
        }
        for(GatherVolatileDataThread<Row,V> t: gatherVolatileDataThreads){
            try {
                t.join();
            }catch (InterruptedException e){
                logger.error("Broker: Volatile gather query interrupted: {}", e.getMessage());
                assert(false);
            }
        }
        if(queryStatus.get()==Broker.QUERY_FAILURE){
            boolean successfulCleanup = volatileShuffleCleanup(dsIDsScatter, txID, gatherDSids);
            if(!successfulCleanup){
                logger.error("Unsuccessful cleanup of volatile data for unsuccessful transaction {}", txID);
                assert (false);
            }
            return null;
        }

        /* The values returned by the gather operations are given as parameter to the combine operator and the returned
         * value is given to the caller as the query result after all volatile data is deleted.
         */

        long aggStart = System.nanoTime();
        V result = plan.combine(gatherResults);
        long aggEnd = System.nanoTime();
        aggregationTimes.add((aggEnd - aggStart) / 1000L);
        boolean successfulCleanup = volatileShuffleCleanup(dsIDsScatter, txID, gatherDSids);
        if(!successfulCleanup){
            logger.error("Broker: Unsuccessful cleanup of volatile data for successful transaction {}", txID);
        }
        return result;
    }
    public<R extends Row> List<R> mapQuery(MapQueryPlan<R> mapQueryPlan, List<R> rows){
        long tStart = System.currentTimeMillis();

        List<R> results = Collections.synchronizedList(new ArrayList<>());
        Map<Integer, List<R>> shardRowListMap = new HashMap<>();
        TableInfo tableInfo = getTableInfo(mapQueryPlan.getQueriedTable());
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

        List<MapQueryThread<R>> mapQueryThreads = new ArrayList<>();
        CountDownLatch queryLatch = new CountDownLatch(shardRowArrayMap.size());
        AtomicInteger queryStatus = new AtomicInteger(QUERY_SUCCESS);

        for (Integer shardNum: shardRowArrayMap.keySet()) {
            R[] rowArray = shardRowArrayMap.get(shardNum);
            MapQueryThread<R> t = new MapQueryThread<>(
                    results,
                    shardNum,
                    mapQueryPlan,
                    rowArray,
                    queryLatch,
                    queryStatus
            );
            t.start();
            mapQueryThreads.add(t);
        }

        for (MapQueryThread<R> t: mapQueryThreads) {
            try {
                t.join();
            } catch (InterruptedException e) {
                logger.error("Map query interrupted: {}", e.getMessage());
                assert(false);
            }
        }
        logger.info("Map completed. Rows: {}. Time: {}ms", rows.size(), System.currentTimeMillis() - tStart);
        assert (queryStatus.get() == QUERY_SUCCESS);
        return results;
    }

    /*READ QUERIES*/
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
    public <S extends Shard, R extends Row, V> V anchoredReadQuery(AnchoredReadQueryPlan<S, V> plan) {
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
    public <S extends Shard, R extends Row, V> V shuffleReadQuery(ShuffleOnReadQueryPlan<S, V> plan) {
        long txID = txIDs.getAndIncrement();
        Map<String, List<Integer>> partitionKeys = plan.keysForQuery();
        HashMap<String, List<Integer>> targetShards = new HashMap<>();

        /*Builds the targetShard mapping, consisting of:
        * Map < tableName, List < Shard identifiers to be queried on the table > >*/

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
            if(shardNums.contains(-1)){
                shardNums.remove(Integer.valueOf(-1));
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
            /*Each datastore receives a call to the shuffleReadQuery method, with message containing:
            * - repartitionNum: the dsID
            * - serializedQuery
            * - numRepartitions: the total number of datastores
            * - txID
            * - targetShards: the Map<tableName, List<Shards ids queried on the table>>
            *
            * each call returns a single ByteString object that is the result of the server's execution of a
            * scatter-gather routine. In particular:
            * - each datastore queries all shards
            * - a single scatter operation is performed for each shard, returning a map object that binds server
            *       identifiers to a list of results
            * - each server performs a combine operation
            * - the combine operation takes as input all the results of all shards' scatter operation for the
            *       datastore executing the gather method
            *
            * The result of all the scatter-gather are stored in the intermediate structure
            * */
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
        /*Once all datastores have executed the gather operation, their results are passed to the user-defined
        * combine operator and the result is returned to the caller.*/
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
    public <S extends Shard, R extends Row, V> V retrieveAndCombineReadQuery(RetrieveAndCombineQueryPlan<S,V> plan){
        long txID = txIDs.getAndIncrement();
        Map<String, List<Integer>> tablesToKeysMap = plan.keysForQuery();
        Map<String, List<Integer>> tablesToShardsMap = new HashMap<>();
        Map<String, List<ByteString>> retrievedData = new HashMap<>();

        long lcv = lastCommittedVersion;
        for(String tableName: tablesToKeysMap.keySet()){
            retrievedData.put(tableName, new CopyOnWriteArrayList<>());
            List<Integer> keyList = tablesToKeysMap.get(tableName);
            TableInfo tableInfo = getTableInfo(tableName);
            List<Integer> shardNums;
            if(keyList.contains(-1)){
                shardNums = IntStream.range(
                        tableInfo.id * SHARDS_PER_TABLE, tableInfo.id * SHARDS_PER_TABLE + tableInfo.numShards
                        )
                        .boxed().collect(Collectors.toList());
            }else{
                shardNums = keyList.stream().map(i -> keyToShard(tableInfo.id, tableInfo.numShards, i))
                        .distinct().collect(Collectors.toList());

            }

            shardNums.remove(Integer.valueOf(-1));
            tablesToShardsMap.put(tableName, shardNums);
            CountDownLatch tableLatch = new CountDownLatch(tablesToShardsMap.get(tableName).size());
            for(Integer shardID:tablesToShardsMap.get(tableName)){
                BrokerDataStoreGrpc.BrokerDataStoreStub stub = BrokerDataStoreGrpc.newStub(getStubForShard(shardID).getChannel());
                ByteString serializedQueryPlan = Utilities.objectToByteString(plan);
                RetrieveAndCombineQueryMessage requestMessage = RetrieveAndCombineQueryMessage.newBuilder()
                        .setShardID(shardID)
                        .setSerializedQueryPlan(serializedQueryPlan)
                        .setLastCommittedVersion(lcv)
                        .build();
                StreamObserver<RetrieveAndCombineQueryResponse> responseStreamObserver = new StreamObserver<RetrieveAndCombineQueryResponse>() {
                    @Override
                    public void onNext(RetrieveAndCombineQueryResponse response) {
                        if(response.getState() == QUERY_SUCCESS){
                            retrievedData.get(tableName).add(response.getData());
                        }
                    }

                    @Override
                    public void onError(Throwable throwable) {}

                    @Override
                    public void onCompleted() {
                        tableLatch.countDown();
                    }
                };
                stub.retrieveAndCombineQuery(requestMessage, responseStreamObserver);
            }
            try {
                tableLatch.await();
            }catch (Exception e){
                logger.error("Retrieve and combine query failed");
            }
        }
        for(Map.Entry<String, List<ByteString>> e: retrievedData.entrySet()){
            if(e.getValue() == null){
                retrievedData.remove(e.getKey());
            }
        }
        return plan.combine(retrievedData);
    }



    /*DATA DISTRIBUTION THREADS*/
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
    private class MapQueryThread<R extends Row> extends Thread{
        List<R> results;
        int shardID;
        MapQueryPlan<R> mapQueryPlan;
        R[] rowArray;
        CountDownLatch queryLatch;
        AtomicInteger queryStatus;

        MapQueryThread(List<R> results, int shardID, MapQueryPlan<R> mapQueryPlan, R[] rowArray,CountDownLatch queryLatch, AtomicInteger queryStatus){
            this.mapQueryPlan = mapQueryPlan;
            this.results = results;
            this.shardID = shardID;
            this.rowArray = rowArray;
            this.queryLatch = queryLatch;
            this.queryStatus = queryStatus;
        }

        @Override
        public void run(){
            R[] originalData = rowArray.clone();
            AtomicInteger mapStatus = new AtomicInteger(QUERY_RETRY);
            while(mapStatus.get() == QUERY_RETRY){
                final List<R[]> partialResults = new ArrayList<>();
                CountDownLatch finishLatch = new CountDownLatch(1);
                BrokerDataStoreGrpc.BrokerDataStoreBlockingStub blockingStub = getStubForShard(shardID);
                BrokerDataStoreGrpc.BrokerDataStoreStub stub = BrokerDataStoreGrpc.newStub(blockingStub.getChannel());

                StreamObserver<MapQueryMessage> observer = stub.mapQuery(new StreamObserver<MapQueryResponse>() {
                    @Override
                    public void onNext(MapQueryResponse mapQueryResponse) {
                        mapStatus.set(mapQueryResponse.getState());
                        if(mapStatus.get() == DataStore.COLLECT){
                            partialResults.add((R[]) Utilities.byteStringToObject(mapQueryResponse.getTransformedData()));
                        }
                    }

                    @Override
                    public void onError(Throwable throwable) {
                        mapStatus.set(QUERY_FAILURE);
                        finishLatch.countDown();
                    }

                    @Override
                    public void onCompleted(){
                        finishLatch.countDown();
                    }
                });
                int STEPSIZE = 10000;
                for (int i = 0; i < originalData.length; i += STEPSIZE) {
                    ByteString serializedQuery = Utilities.objectToByteString(mapQueryPlan);
                    R[] rowSlice = Arrays.copyOfRange(originalData, i, Math.min(originalData.length, i + STEPSIZE));
                    ByteString rowData = Utilities.objectToByteString(rowSlice);
                    MapQueryMessage rowMessage = MapQueryMessage.newBuilder()
                            .setSerializedQuery(serializedQuery)
                            .setRowData(rowData)
                            .setState(DataStore.COLLECT)
                            .build();
                    observer.onNext(rowMessage);
                }
                observer.onCompleted();
                try{
                    finishLatch.await();
                }catch (Exception e){
                    logger.error("Map interrupted {}", e.getMessage());
                }
                if (mapStatus.get() == QUERY_RETRY) {
                    try {
                        originalData = rowArray.clone();
                        Thread.sleep(shardMapDaemonSleepDurationMillis);
                        continue;
                    } catch (InterruptedException e) {
                        logger.error("Map Interrupted: {}", e.getMessage());
                        assert (false);
                    }
                }
                if (mapStatus.get() == QUERY_FAILURE) {
                    queryStatus.set(QUERY_FAILURE);
                }
                assert (mapStatus.get() == DataStore.COLLECT);
                for(R[] rowArray: partialResults){
                    for(R row: rowArray){
                        results.add(row);
                    }
                }
                return;
            }
        }
    }
    private class StoreVolatileDataThread<R extends Row> extends Thread{
        private final Integer dsID;
        private final long txID;
        private final R[] rowArray;
        private final AtomicInteger queryStatus;

        StoreVolatileDataThread(Integer dsID,
                                   long txID,
                                   R[] rowArray,
                                   AtomicInteger queryStatus){
            this.dsID = dsID;
            this.txID = txID;
            this.rowArray = rowArray;
            this.queryStatus = queryStatus;
        }

        @Override
        public void run(){
            storeVolatileData();
        }

        private void storeVolatileData(){
            AtomicInteger subQueryStatus = new AtomicInteger(QUERY_RETRY);
            while (subQueryStatus.get() == QUERY_RETRY) {
                ManagedChannel channel = dsIDToChannelMap.get(dsID);
                assert(channel != null);
                BrokerDataStoreGrpc.BrokerDataStoreBlockingStub blockingStub = BrokerDataStoreGrpc.newBlockingStub(channel);
                BrokerDataStoreGrpc.BrokerDataStoreStub stub = BrokerDataStoreGrpc.newStub(blockingStub.getChannel());

                final CountDownLatch prepareLatch = new CountDownLatch(1);

                StreamObserver<StoreVolatileDataMessage> observer =
                        stub.storeVolatileData(new StreamObserver<StoreVolatileDataResponse>() {
                            @Override
                            public void onNext(StoreVolatileDataResponse storeVolatileDataResponse) {
                                subQueryStatus.set(storeVolatileDataResponse.getState());
                                prepareLatch.countDown();
                            }

                            @Override
                            public void onError(Throwable th) {
                                logger.warn("StoreVolatileDataThread: Volatile shuffle query RPC failed for datastore {}", dsID);
                                subQueryStatus.set(QUERY_FAILURE);
                                prepareLatch.countDown();
                            }

                            @Override
                            public void onCompleted() {
                                prepareLatch.countDown();
                            }
                        });
                final int STEPSIZE = 1000;
                for (int i = 0; i < rowArray.length; i += STEPSIZE) {
                    R[] rowSlice = Arrays.copyOfRange(rowArray, i, Math.min(rowArray.length, i + STEPSIZE));
                    ByteString rowData = Utilities.objectToByteString(rowSlice);
                    StoreVolatileDataMessage rowMessage = StoreVolatileDataMessage.newBuilder()
                            .setData(rowData)
                            .setTransactionID(txID)
                            .setState(DataStore.COLLECT)
                            .build();
                    observer.onNext(rowMessage);
                }
                StoreVolatileDataMessage confirmMessage = StoreVolatileDataMessage.newBuilder()
                        .setState(DataStore.COMMIT).build();
                observer.onNext(confirmMessage);
                try {
                    prepareLatch.await();
                } catch (InterruptedException e) {
                    logger.error("StoreVolatileDataThread: Volatile Shuffle Interrupted: {}", e.getMessage());
                    assert (false);
                }

                assert(subQueryStatus.get() != QUERY_RETRY);
                if (subQueryStatus.get() == QUERY_FAILURE) {
                    queryStatus.set(QUERY_FAILURE);
                }
                //Communication closed by the client after the response message has been received
                observer.onCompleted();
            }
        }
    }
    private class ScatterVolatileDataThread<R extends Row, V> extends Thread{
        private final long txID;
        private final List<Integer> gatherDSlist;
        private final AtomicInteger queryStatus;
        private final Integer dsID;
        private final VolatileShuffleQueryPlan<V> plan;

        public ScatterVolatileDataThread(
                Integer dsID,
                long txID,
                List<Integer> gatherDSlist,
                AtomicInteger queryStatus,
                VolatileShuffleQueryPlan<V> plan){
            this.gatherDSlist = gatherDSlist;
            this.queryStatus = queryStatus;
            this.txID = txID;
            this.dsID = dsID;
            this.plan = plan;
        }

        @Override
        public void run(){
            scatterVolatileData();
        }

        private void scatterVolatileData(){
            ByteString serializedPlan = Utilities.objectToByteString(plan);
            ManagedChannel channel = dsIDToChannelMap.get(dsID);
            BrokerDataStoreGrpc.BrokerDataStoreBlockingStub stub = BrokerDataStoreGrpc.newBlockingStub(channel);
            ScatterVolatileDataMessage startScatterMessage = ScatterVolatileDataMessage.newBuilder()
                    .setTransactionID(txID)
                    .setPlan(serializedPlan)
                    .setActorCount(dsIDToChannelMap.keySet().size())
                    .build();
            ScatterVolatileDataResponse scatterVolatileDataResponse = stub.scatterVolatileData(startScatterMessage);
            if(scatterVolatileDataResponse.getState() == Broker.QUERY_FAILURE){
                queryStatus.set(Broker.QUERY_FAILURE);
            }else{
                ByteString serializedGatherDSids = scatterVolatileDataResponse.getIdsDsGather();
                List<Integer> receivedGatherDSids = (ArrayList<Integer>) Utilities.byteStringToObject(serializedGatherDSids);
                for(Integer gDSid: receivedGatherDSids){
                    if(!gatherDSlist.contains(gDSid)) {
                        gatherDSlist.add(gDSid);
                    }
                }
            }
        }
    }
    private class GatherVolatileDataThread<R extends Row, V> extends Thread{
        private final Integer dsID;
        private final long txID;
        private final List<ByteString> gatherResults;
        private final AtomicInteger queryStatus;
        private final VolatileShuffleQueryPlan<V> plan;

        public GatherVolatileDataThread(Integer dsID,
                                 long txID,
                                 List<ByteString> gatherResults,
                                 AtomicInteger queryStatus,
                                 VolatileShuffleQueryPlan<V> plan){
            this.dsID=dsID;
            this.txID=txID;
            this.gatherResults=gatherResults;
            this.queryStatus=queryStatus;
            this.plan=plan;
        }

        @Override
        public void run(){
            gatherVolatileData();
        }

        private void gatherVolatileData(){
            CountDownLatch gatherLatch = new CountDownLatch(1);
            ByteString serializedPlan = Utilities.objectToByteString(plan);
            ManagedChannel channel = dsIDToChannelMap.get(dsID);
            BrokerDataStoreGrpc.BrokerDataStoreBlockingStub stub = BrokerDataStoreGrpc.newBlockingStub(channel);
            GatherVolatileDataMessage startGatherMessage = GatherVolatileDataMessage.newBuilder()
                    .setTransactionID(txID)
                    .setPlan(serializedPlan)
                    .build();
            GatherVolatileDataResponse gatherVolatileDataResponse = stub.gatherVolatileData(startGatherMessage);
            if(gatherVolatileDataResponse.getState() == Broker.QUERY_FAILURE){
                queryStatus.set(Broker.QUERY_FAILURE);
            }else{
                ByteString result = gatherVolatileDataResponse.getGatherResult();
                gatherResults.add(result);
            }
        }
    }


    /*UTILITIES*/

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
    private boolean volatileShuffleCleanup(List<Integer> dsIDsScatter, long txID, List<Integer> gatherDSids){
        CountDownLatch failureLatch = new CountDownLatch(dsIDsScatter.size());
        AtomicInteger outcome = new AtomicInteger(0);
        for(Integer dsID: dsIDsScatter){
            ManagedChannel channel = dsIDToChannelMap.get(dsID);
            BrokerDataStoreGrpc.BrokerDataStoreStub stub = BrokerDataStoreGrpc.newStub(channel);
            RemoveVolatileDataMessage message = RemoveVolatileDataMessage.newBuilder().setTransactionID(txID).build();
            StreamObserver<RemoveVolatileDataResponse> observer = new StreamObserver<>() {
                @Override
                public void onNext(RemoveVolatileDataResponse removeVolatileDataResponse) {
                    ;
                }

                @Override
                public void onError(Throwable throwable) {
                    logger.error("Broker: removeVolatileData Failed for transaction id {}", txID);
                    outcome.set(1);
                    failureLatch.countDown();
                }

                @Override
                public void onCompleted() {
                    failureLatch.countDown();
                }
            };
            stub.removeVolatileData(message, observer);
        }
        try {
            failureLatch.await();
        }catch (InterruptedException e){
            logger.error("Broker: RemoveVolatileData Failed for transaction id {}", txID);
            return false;
        }
        if(gatherDSids == null || outcome.get() != 0) {
            return outcome.get() == 0;
        }
        final CountDownLatch secondFailureLatch = new CountDownLatch(gatherDSids.size());
        for(Integer dsID: gatherDSids){
            ManagedChannel channel = dsIDToChannelMap.get(dsID);
            BrokerDataStoreGrpc.BrokerDataStoreStub stub = BrokerDataStoreGrpc.newStub(channel);
            RemoveVolatileDataMessage message = RemoveVolatileDataMessage.newBuilder().setTransactionID(txID).build();
            StreamObserver<RemoveVolatileDataResponse> observer = new StreamObserver<RemoveVolatileDataResponse>() {
                @Override
                public void onNext(RemoveVolatileDataResponse removeVolatileDataResponse) {
                    ;
                }

                @Override
                public void onError(Throwable throwable) {
                    logger.error("Broker: removeVolatileScatteredData Failed for transaction id {}", txID);
                    outcome.set(1);
                }

                @Override
                public void onCompleted() {
                    secondFailureLatch.countDown();
                }
            };
            stub.removeVolatileScatteredData(message, observer);
        }
        try{
            secondFailureLatch.await();
        }catch (InterruptedException e){
            logger.error("Broker: removeVolatileScatteredData Failed for transaction id {}", txID);
            return false;
        }
        return outcome.get() == 0;
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
}