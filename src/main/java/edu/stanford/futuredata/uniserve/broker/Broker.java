package edu.stanford.futuredata.uniserve.broker;

import com.google.protobuf.ByteString;
import edu.stanford.futuredata.uniserve.*;
import edu.stanford.futuredata.uniserve.datastore.DataStore;
import edu.stanford.futuredata.uniserve.interfaces.*;
import edu.stanford.futuredata.uniserve.relational.RelReadQueryResults;
import edu.stanford.futuredata.uniserve.relational.RelRow;
import edu.stanford.futuredata.uniserve.relationalapi.ReadQuery;
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
import java.util.stream.Collectors;
import java.util.stream.IntStream;


/**
 * TODO: replace the ExecutorService readQueryThreadPool with async calls.
 * TODO: Retry routine and error handling when the Broker cannot find the Coordinator.
 * TODO: Error handling in getTableInfo should be not be carried out via assert statements.
 * TODO: Error handling in getStubForShard should not be carried out via assert statements.
 * TODO: Generic error handling across the whole Broker specification, this should NOT be done via assert statements.
 * */
public class Broker {
    public static final TableInfo NIL_TABLE_INFO = new TableInfo("", -1, 0);

    /**Curator Framework interface */
    private final BrokerCurator zkCurator;
    private static final Logger logger = LoggerFactory.getLogger(Broker.class);
    /**Consistent hash assigning actors to servers.*/
    private ConsistentHash consistentHash;
    /**Map servers to channels.*/
    private Map<Integer, ManagedChannel> dsIDToChannelMap = new ConcurrentHashMap<>();
    /**Blocking Stub for communication with the coordinator's services specified in the broker_coordinator.proto.*/
    private final BrokerCoordinatorGrpc.BrokerCoordinatorBlockingStub coordinatorBlockingStub;

    /**Thread updating stored actors' state*/
    private final ShardMapUpdateDaemon shardMapUpdateDaemon;
    /**If true, enables automatic actor state update*/
    public boolean runShardMapUpdateDaemon = true;
    /**Time period between actors updates in milliseconds*/
    public static int shardMapDaemonSleepDurationMillis = 1000;

    /**Thread sending query statistics to the coordinator*/
    private final QueryStatisticsDaemon queryStatisticsDaemon;
    /**If true, enables statistic forwarding to the coordinator*/
    public boolean runQueryStatisticsDaemon = true;
    /**Time period between statistic forwards in milliseconds*/
    public static int queryStatisticsDaemonSleepDurationMillis = 10000;

    /**Execution times of queries ran by this broker instance*/
    public final Collection<Long> remoteExecutionTimes = new ConcurrentLinkedQueue<>();
    public final Collection<Long> aggregationTimes = new ConcurrentLinkedQueue<>();

    public static final int QUERY_SUCCESS = 0;
    public static final int QUERY_FAILURE = 1;
    public static final int QUERY_RETRY = 2;
    public static final int READ_NON_EXISTING_SHARD = 7;

    public static final int SHARDS_PER_TABLE = 10000;

    /**Statistics collected by the broker since last forward operation*/
    public ConcurrentHashMap<Set<Integer>, Integer> queryStatistics = new ConcurrentHashMap<>();
    ExecutorService readQueryThreadPool = Executors.newFixedThreadPool(256);  //TODO:  Replace with async calls.

    /**Instantiates a Broker curator acting as a client for the underlying ZooKeeper service.
     * Retrieves the Coordinator's location from ZK and opens a channel with it.
     * Creates and starts a Shard Map Update Daemon thread and a Query Statistic Daemon Thread.
     * TODO: Retry routine and error handling when the Broker cannot find the Coordinator
     * @param zkHost IP address of the ZooKeeper service
     * @param zkPort port assigned to the ZooKeeper service
     * */
    public Broker(String zkHost, int zkPort) {
        this.zkCurator = new BrokerCurator(zkHost, zkPort);
        Optional<Pair<String, Integer>> masterHostPort = zkCurator.getMasterLocation();
        String masterHost = null;
        Integer masterPort = null;
        if (masterHostPort.isPresent()) {
            masterHost = masterHostPort.get().getValue0();
            masterPort = masterHostPort.get().getValue1();
        } else {
            if(Utilities.logger_flag)
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

    /**Stops the ShardMapUpdate and QueryStatistic daemon threads, shuts down all channels with servers and coordinator
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
            if(Utilities.logger_flag)
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
    public boolean createTable(String tableName, int numShards, List<String> attributeNames, Boolean[] keyStructure) {
        String[] attrNamesArray = attributeNames.toArray(new String[0]);
        ByteString serAttrNamesArray = Utilities.objectToByteString(attrNamesArray);
        ByteString serKeyStr = Utilities.objectToByteString(keyStructure);
        CreateTableMessage m = CreateTableMessage.newBuilder()
                .setTableName(tableName)
                .setNumShards(numShards)
                .setAttributeNames(serAttrNamesArray)
                .setKeyStructure(serKeyStr)
                .build();
        CreateTableResponse r = coordinatorBlockingStub.createTable(m);
        return r.getReturnCode() == QUERY_SUCCESS;
    }

    /*WRITE QUERIES*/
    /**Executes the write query plan on the given rows in a 2PC fashion. It starts a write query thread for each shard containing any of
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
            int partitionKey = row.getPartitionKey(tableInfo.getKeyStructure());
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

        long txID = zkCurator.getTxID();

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
                if(Utilities.logger_flag)
                    logger.error("Write query interrupted: {}", e.getMessage());
                assert(false);
            }
        }
        zkCurator.writeLastCommittedVersion(txID);
        zkCurator.releaseWriteLock();
        /*After the lock has been released since all stored queries are performed via a write operation that uses its own lock*/
        List<ReadQuery> triggeredQueries = tableInfo.getRegisteredQueries();
        for(ReadQuery triggeredQuery: triggeredQueries){
            triggeredQuery.updateStoredResults(this);
        }

        if(Utilities.logger_flag)
            logger.info("Write completed. Rows: {}. Version: {} Time: {}ms", rows.size(), txID,
                System.currentTimeMillis() - tStart);
        assert (queryStatus.get() != QUERY_RETRY);

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
            int partitionKey = row.getPartitionKey(tableInfo.getKeyStructure());
            assert(partitionKey >= 0);
            int shard = keyToShard(tableInfo.id, tableInfo.numShards, partitionKey);
            shardRowListMap.computeIfAbsent(shard, (k -> new ArrayList<>())).add(row);
        }
        Map<Integer, R[]> shardRowArrayMap = shardRowListMap.entrySet().stream().
                collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().toArray((R[]) new Row[0])));
        List<SimpleWriteQueryThread<R, S>> writeQueryThreads = new ArrayList<>();
        long txID = zkCurator.getTxID();
        AtomicInteger queryStatus = new AtomicInteger(QUERY_SUCCESS);
        for (Integer shardNum: shardRowArrayMap.keySet()) {
            R[] rowArray = shardRowArrayMap.get(shardNum);
            SimpleWriteQueryThread<R, S> t = new SimpleWriteQueryThread<>(
                    shardNum,
                    writeQueryPlan,
                    rowArray,
                    txID,
                    queryStatus
            );
            t.start();
            writeQueryThreads.add(t);
        }
        for (SimpleWriteQueryThread<R, S> t: writeQueryThreads) {
            try {
                t.join();
            } catch (InterruptedException e) {
                if(Utilities.logger_flag)
                    logger.error("SimpleWrite query interrupted: {}", e.getMessage());
                assert(false);
            }
        }
        zkCurator.writeLastCommittedVersion(txID);
        List<ReadQuery> triggeredQueries = tableInfo.getRegisteredQueries();
        for(ReadQuery triggeredQuery: triggeredQueries){
            triggeredQuery.updateStoredResults(this);
        }
        if(Utilities.logger_flag)
            logger.info("SimpleWrite completed. Rows: {}. Table: {} Version: {} Time: {}ms.",
                rows.size(), writeQueryPlan.getQueriedTable(), txID,
                System.currentTimeMillis() - tStart);
        assert (queryStatus.get() != QUERY_RETRY);
        return queryStatus.get() == QUERY_SUCCESS;
    }


    public <R extends Row, S extends Shard> boolean writeCachedData(
            SimpleWriteQueryPlan<R, S> writeQueryPlan, long txID, Map<Integer, Integer> shardToDs) {
        long tStart = System.currentTimeMillis();
        List<WriteCachedDataThread<R, S>> writeQueryThreads = new ArrayList<>();
        AtomicInteger queryStatus = new AtomicInteger(QUERY_SUCCESS);
        for (Integer shardNum: shardToDs.keySet()) {
            WriteCachedDataThread<R, S> t = new WriteCachedDataThread<>(
                    shardNum,
                    writeQueryPlan,
                    txID,
                    queryStatus,
                    shardToDs.get(shardNum)
            );
            t.start();
            writeQueryThreads.add(t);
        }
        for (WriteCachedDataThread<R,S> t: writeQueryThreads) {
            try {
                t.join();
            } catch (InterruptedException e) {
                if(Utilities.logger_flag)
                    logger.error("SimpleWrite query interrupted: {}", e.getMessage());
                assert(false);
            }
        }
        zkCurator.writeLastCommittedVersion(txID);
        if(Utilities.logger_flag)
            logger.info("Write completed. Table: {} Version: {} Time: {}ms.",
                writeQueryPlan.getQueriedTable(), txID,
                System.currentTimeMillis() - tStart);
        assert (queryStatus.get() != QUERY_RETRY);
        return queryStatus.get() == QUERY_SUCCESS;
    }


    /*VOLATILE OPERATIONS*/
    public<V> V volatileShuffleQuery(VolatileShuffleQueryPlan plan, List<Row> rows){
        long txID = zkCurator.getTxID();
        Map<Integer, List<Row>> dsToRowListMap = new HashMap<>();
        int dsCount = dsIDToChannelMap.size();
        for(Row row: rows){
            int dsID = row.hashCode() % dsCount;
            dsToRowListMap.computeIfAbsent(dsID, k->new ArrayList<>()).add(row);
        }
        Map<Integer, Row[]> dsToRowArrayMap =
                dsToRowListMap
                        .entrySet()
                        .stream()
                        .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().toArray(new Row[0])));

        /* Send the raw data to the actors that will keep them in memory for later shuffling operation. The
         * ids of the actors storing the raw data are added to the dsIDsScatter list. Proceed once all data has
         * been forwarded. If one send operation failed, signal to all other actors to delete the volatile data
         * associated to this transaction and return. The raw data will be stored by the receiving actor as a List
         * of serialized arrays of Rows, the same ones prepared in the thread (List<Serialized(R[])>)
         * */
        List<StoreVolatileDataThread> storeVolatileDataThreads = new ArrayList<>();
        AtomicInteger queryStatus = new AtomicInteger(QUERY_SUCCESS);
        List<Integer> dsIDsScatter = new ArrayList<>();
        for (Integer dsID: dsToRowArrayMap.keySet()) {
            Row[] rowArray = dsToRowArrayMap.get(dsID);
            if(!dsIDsScatter.contains(dsID)){
                dsIDsScatter.add(dsID);
            }
            StoreVolatileDataThread t = new StoreVolatileDataThread<>(
                    dsID,
                    txID,
                    rowArray,
                    queryStatus,
                    plan
            );
            t.start();
            storeVolatileDataThreads.add(t);
        }
        for (StoreVolatileDataThread t: storeVolatileDataThreads) {
            try {
                t.join();
            } catch (InterruptedException e) {
                if(Utilities.logger_flag)
                    logger.error("Volatile shuffle query interrupted: {}", e.getMessage());
                assert(false);
            }
        }
        if(queryStatus.get() == Broker.QUERY_FAILURE){
            boolean successfulCleanup = volatileShuffleCleanup(dsIDsScatter, txID, null);
            if(!successfulCleanup){
                if(Utilities.logger_flag)
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

        List<ScatterVolatileDataThread<V>> scatterVolatileDataThreads = new ArrayList<>();
        List<Integer> gatherDSids = Collections.synchronizedList(new ArrayList<>());

        for(Integer dsID: dsIDsScatter){
            ScatterVolatileDataThread<V> t = new ScatterVolatileDataThread<V>(dsID, txID, gatherDSids, queryStatus, plan);
            t.start();
            scatterVolatileDataThreads.add(t);
        }
        for(ScatterVolatileDataThread<V> t: scatterVolatileDataThreads){
            try {
                t.join();
            }catch (InterruptedException e ){
                if(Utilities.logger_flag)
                    logger.error("Broker: Volatile scatter query interrupted: {}", e.getMessage());
                assert(false);
                queryStatus.set(Broker.QUERY_FAILURE);
            }
        }
        if(queryStatus.get()==Broker.QUERY_FAILURE){
            boolean successfulCleanup = volatileShuffleCleanup(dsIDsScatter, txID, gatherDSids);
            if(!successfulCleanup){
                if(Utilities.logger_flag)
                    logger.error("Broker: Unsuccessful cleanup of volatile data for unsuccessful transaction {} (scatter failed)", txID);
                assert (false);
            }
            return null;
        }

        /*All scatter operations have been successfully carried out and the shuffled data is stored in memory
         * by the actors that will execute the gather operation. The ids of the actors storing this data are stored in
         * the gatherDSids list. The Broker now triggers the gather operations and retrieves all results from
         * the actors. All actors must return before the execution resumes.
         * If one gather fails, all volatile data is deleted.
         * */


        List<GatherVolatileDataThread<V>> gatherVolatileDataThreads = new ArrayList<>();
        List<ByteString> gatherResults = Collections.synchronizedList(new ArrayList<>());
        for(Integer dsID: gatherDSids){
            GatherVolatileDataThread<V> t = new GatherVolatileDataThread<V>(dsID, txID, gatherResults, queryStatus, plan);
            t.start();
            gatherVolatileDataThreads.add(t);
        }
        for(GatherVolatileDataThread<V> t: gatherVolatileDataThreads){
            try {
                t.join();
            }catch (InterruptedException e){
                if(Utilities.logger_flag)
                    logger.error("Broker: Volatile gather query interrupted: {}", e.getMessage());
                assert(false);
            }
        }
        if(queryStatus.get()==Broker.QUERY_FAILURE){
            boolean successfulCleanup = volatileShuffleCleanup(dsIDsScatter, txID, gatherDSids);
            if(!successfulCleanup){
                if(Utilities.logger_flag)
                    logger.error("Unsuccessful cleanup of volatile data for unsuccessful transaction {}", txID);
                assert (false);
            }
            return null;
        }

        /* The values returned by the gather operations are given as parameter to the combine operator and the returned
         * value is given to the caller as the query result after all volatile data is deleted.
         */
        long aggStart = System.nanoTime();
        V result = (V) plan.combine(gatherResults);
        long aggEnd = System.nanoTime();
        aggregationTimes.add((aggEnd - aggStart) / 1000L);
        boolean successfulCleanup = volatileShuffleCleanup(dsIDsScatter, txID, gatherDSids);
        if(!successfulCleanup){
            if(Utilities.logger_flag)
                logger.error("Broker: Unsuccessful cleanup of volatile data for successful transaction {}", txID);
        }
        return result;
    }

    /*READ QUERIES*/
    /**Executes an AnchoredReadQueryPlan
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
        long txID = zkCurator.getTxID();

        Map<String, List<Integer>> tableNameToPartitionKeys = plan.keysForQuery();
        HashMap<String, List<Integer>> tableNameToShardIDs = new HashMap<>();

        /* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
        * Build a Map<TableName, List<ShardsIDs to be queried relative to the table>>                                  *
        * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

        for(Map.Entry<String, List<Integer>> entry: tableNameToPartitionKeys.entrySet()) {
            String tableName = entry.getKey();
            List<Integer> tablePartitionKeys = entry.getValue();
            TableInfo tableInfo = getTableInfo(tableName);
            int tableID = tableInfo.id;
            int numShards = tableInfo.numShards;
            List<Integer> shardNumbs;
            if (tablePartitionKeys.contains(-1)) {
                // -1 is a wildcard--run on all shards.
                shardNumbs = tableInfo.getTableShardsIDs();
            } else {
                shardNumbs = tablePartitionKeys.stream().map(i -> keyToShard(tableID, numShards, i))
                        .distinct().collect(Collectors.toList());
            }
            tableNameToShardIDs.put(tableName, shardNumbs);
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
            subQueryShards.forEach((k, v) -> tableNameToShardIDs.put(k, new ArrayList<>(v.keySet())));
        }

        ByteString serializedTableNameToShardIDs = Utilities.objectToByteString(tableNameToShardIDs);
        ByteString serializedIntermediateShards = Utilities.objectToByteString(intermediateShards);
        ByteString serializedQuery = Utilities.objectToByteString(plan);
        String anchorTable = plan.getAnchorTable();
        List<Integer> anchorTableShards = tableNameToShardIDs.get(anchorTable);
        int numberOfAnchorShards = anchorTableShards.size();
        List<ByteString> intermediates = new CopyOnWriteArrayList<>();

        /* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
        * The anchoredReadQuery service of the Broker-Datastore is called for each shard to be queried of the          *
        * anchor table. The datastore is randomly selected among those hosting the shard.                              *
        * The results of the call are stored in the "intermediates" list of ByteStrings.                               *
        *                                                                                                              *
        * This is a blocking phase, it ends once an answer has been received from all calls related to each shard of   *
        * the anchor table.                                                                                            *
        * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

        CountDownLatch latch = new CountDownLatch(numberOfAnchorShards);
        long lcv = zkCurator.getLastCommittedVersion();
        for (int anchorShardNum: anchorTableShards) {
            int dsID = consistentHash.getRandomBucket(anchorShardNum);
            ManagedChannel channel = dsIDToChannelMap.get(dsID);
            BrokerDataStoreGrpc.BrokerDataStoreStub stub = BrokerDataStoreGrpc.newStub(channel);
            AnchoredReadQueryMessage m = AnchoredReadQueryMessage.newBuilder().
                    setAnchorShardNum(anchorShardNum).
                    setSerializedQuery(serializedQuery).
                    setAnchorShardsCount(numberOfAnchorShards).
                    setTxID(txID).
                    setLastCommittedVersion(lcv).
                    setTableNameToShardIDs(serializedTableNameToShardIDs).
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
                        retry();
                    } else {
                        assert (r.getReturnCode() == QUERY_SUCCESS);
                        intermediates.add(r.getResponse());
                        latch.countDown();
                    }
                }

                @Override
                public void onError(Throwable throwable) {
                    if(Utilities.logger_flag)
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
         *      merged.                                                                                                *
         *      A Map<TableNames, Map<Integer, Integer>> containing only the entry related to the return table is      *
         *      built and has value equal to the combinedShardLocation mapping.                                        *
         *      This single-value-map is cast to the type returned by the query.                                     *
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
    public <S extends Shard, V> V shuffleReadQuery(ShuffleOnReadQueryPlan<S, V> plan) {
        long txID = zkCurator.getTxID();
        //run subqueries

        Map<String, ReadQuery> volatileSubqueries = plan.getVolatileSubqueries();
        Map<String, List<Pair<Integer, Integer>>> volatileSubqueriesResults = new HashMap<>();
        for(Map.Entry<String, ReadQuery> entry: volatileSubqueries.entrySet()){
            volatileSubqueriesResults.put(entry.getKey(), entry.getValue().run(this).getIntermediateLocations());
        }
        Map<String, List<Integer>> partitionKeys = plan.keysForQuery();
        HashMap<String, List<Integer>> targetShards = new HashMap<>();


        Map<String, ReadQuery> concreteSubqueries = plan.getConcreteSubqueries();
        Map<String, ReadQueryResults> concreteSubqueriesResults = new HashMap<>();
        for(Map.Entry<String, ReadQuery> entry: concreteSubqueries.entrySet()){
            concreteSubqueriesResults.put(entry.getKey(), entry.getValue().run(this));
        }
        //Builds the targetShard mapping, consisting of:
        //Map < tableName, List < Shard identifiers to be queried on the table > >
        for (Map.Entry<String, List<Integer>> entry : partitionKeys.entrySet()) {
            String tableName = entry.getKey();
            List<Integer> tablePartitionKeys = entry.getValue();
            TableInfo tableInfo = getTableInfo(tableName);
            int tableID = tableInfo.id;
            int numShards = tableInfo.numShards;
            List<Integer> shardNums = new ArrayList<>();
            shardNums = tableInfo.getTableShardsIDs();
            try {
                if((shardNums != null) && !shardNums.isEmpty() && shardNums.contains(-1)){
                    shardNums.remove(Integer.valueOf(-1));
                }
            }catch (NullPointerException e){
                System.err.println(e.getMessage());
                System.err.println("Table: " + entry.getKey());
                shardNums = new ArrayList<>();
            }
            targetShards.put(tableName, shardNums);
        }


        ByteString serializedQuery = Utilities.objectToByteString(plan);
        Set<Integer> dsIDs = dsIDToChannelMap.keySet();
        int numReducers = dsIDs.size();
        List<ByteString> intermediates = new CopyOnWriteArrayList<>();
        CountDownLatch latch = new CountDownLatch(numReducers);
        int reducerNum = 0;
        List<ByteString> destinationShardIDs = new CopyOnWriteArrayList<>();
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
            HashMap<String, List<Pair<Integer, Integer>>> dsSubqueriesResults = new HashMap<>(volatileSubqueriesResults);
            HashMap<String, List<Integer>> dsTargetShards = new HashMap<>(targetShards);
            ByteString serializedTargetShards = Utilities.objectToByteString(dsTargetShards);
            ByteString serializedSubqueriesResults = Utilities.objectToByteString(dsSubqueriesResults);
            ByteString serializedConcreteSubqueriesResults = Utilities.objectToByteString(new HashMap<>(concreteSubqueriesResults));
            ManagedChannel channel = dsIDToChannelMap.get(dsID);
            BrokerDataStoreGrpc.BrokerDataStoreStub stub = BrokerDataStoreGrpc.newStub(channel);
            ShuffleReadQueryMessage m = ShuffleReadQueryMessage.newBuilder().
                    setRepartitionNum(reducerNum).
                    setSerializedQuery(serializedQuery).
                    setNumRepartitions(numReducers).
                    setTxID(txID).
                    setTargetShards(serializedTargetShards).
                    setSubqueriesResults(serializedSubqueriesResults).
                    setConcreteSubqueriesResults(serializedConcreteSubqueriesResults).
                    build();
            reducerNum++;
            StreamObserver<ShuffleReadQueryResponse> responseObserver = new StreamObserver<>() {
                @Override
                public void onNext(ShuffleReadQueryResponse r) {
                    assert (r.getReturnCode() == Broker.QUERY_SUCCESS);
                    intermediates.add(r.getResponse());
                    if(plan.isStored()){
                        destinationShardIDs.add(r.getDestinationShards());
                    }
                }

                @Override
                public void onError(Throwable throwable) {
                    if(Utilities.logger_flag)
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

        if(plan.isStored()){
            Set<Integer> destinationShardsSet = new HashSet<>();
            for(ByteString bs: destinationShardIDs){
                destinationShardsSet.addAll((List<Integer>)Utilities.byteStringToObject(bs));
            }
            HashMap<Integer, Integer> shardIDtoDSId = new HashMap<>();
            for(Integer shardID: destinationShardsSet){
                shardIDtoDSId.put(shardID, consistentHash.getRandomBucket(shardID));
            }
            CountDownLatch latch1 = new CountDownLatch(dsIDs.size());
            ByteString serShardIDToDSId = Utilities.objectToByteString(shardIDtoDSId);
            for(Integer dsID: dsIDs){
                ManagedChannel channel = dsIDToChannelMap.get(dsID);
                BrokerDataStoreGrpc.BrokerDataStoreStub stub = BrokerDataStoreGrpc.newStub(channel);
                ForwardDataToStoreMessage m = ForwardDataToStoreMessage.newBuilder()
                        .setTxID(txID)
                        .setShardIDToDSIDMap(serShardIDToDSId)
                        .build();
                StreamObserver<ForwardDataToStoreResponse> responseObserver = new StreamObserver<ForwardDataToStoreResponse>() {
                    @Override
                    public void onNext(ForwardDataToStoreResponse forwardDataToStoreResponse) {
                        assert(forwardDataToStoreResponse.getStatus() == Broker.QUERY_SUCCESS);
                    }

                    @Override
                    public void onError(Throwable throwable) {

                    }

                    @Override
                    public void onCompleted() {
                        latch1.countDown();
                    }
                };
                stub.forwardDataToStore(m, responseObserver);
            }
            try{
                latch1.await();
            }catch (Exception e ){
                assert (false);
            }

            this.writeCachedData(plan.getWriteResultPlan(), txID, shardIDtoDSId);
            removeCachedResults(txID);
        }

        //TODO: parallelize
        for(Map.Entry<String, List<Pair<Integer, Integer>>> subqueryRes: volatileSubqueriesResults.entrySet()){
            for(Pair<Integer, Integer> shardIDDSiD: subqueryRes.getValue()){
                removeIntermediateShard(shardIDDSiD.getValue0(), shardIDDSiD.getValue1());
            }
        }
        return ret;
    }
    public <S extends Shard, V> V retrieveAndCombineReadQuery(RetrieveAndCombineQueryPlan<S,V> plan){
        long txID = zkCurator.getTxID();
        ByteString serializedQueryPlan = Utilities.objectToByteString(plan);
        Map<String, List<Integer>> tablesToKeysMap = plan.keysForQuery();
        Map<String, List<Integer>> tablesToShardsMap = new HashMap<>();
        Map<String, List<ByteString>> retrievedData = new HashMap<>();
        //run subqueries
        Map<String, ReadQuery> volatileSubqueries = plan.getVolatileSubqueries();

        Map<String, List<Pair<Integer,Integer>>> volatileSubqueriesResults = new HashMap<>();
        for(Map.Entry<String, ReadQuery> entry: volatileSubqueries.entrySet()){
            volatileSubqueriesResults.put(entry.getKey(), entry.getValue().run(this).getIntermediateLocations());
        }

        Map<String, ReadQuery> concreteSubqueries = plan.getConcreteSubqueries();
        HashMap<String, ReadQueryResults> concreteSubqueriesResults = new HashMap<>();
        for(Map.Entry<String, ReadQuery> entry: concreteSubqueries.entrySet()){
            concreteSubqueriesResults.put(entry.getKey(), entry.getValue().run(this));
        }
        ByteString serializedConcreteSubqueriesResults = Utilities.objectToByteString(concreteSubqueriesResults);
        List<ByteString> destinationShardIDs = new CopyOnWriteArrayList<>();
        long lcv = zkCurator.getLastCommittedVersion();
        for(String tableName: tablesToKeysMap.keySet()){
            retrievedData.put(tableName, new CopyOnWriteArrayList<>());
            List<Integer> keyList = tablesToKeysMap.get(tableName);
            TableInfo tableInfo = getTableInfo(tableName);
            assert (tableInfo != null && tableInfo != Broker.NIL_TABLE_INFO);
            assert (tableInfo.name.equals(tableName));
            List<Integer> shardNums;
            if(keyList.contains(-1)){
                shardNums = tableInfo.getTableShardsIDs();
            }else{
                shardNums = keyList.stream().map(i -> keyToShard(tableInfo.id, tableInfo.numShards, i))
                        .distinct().collect(Collectors.toList());

            }
            if(shardNums == null){
                if(Utilities.logger_flag)
                    logger.error("TableInfo does not store list of shards already allocated for table " + tableName);
            }
            shardNums.remove(Integer.valueOf(-1));
            tablesToShardsMap.put(tableName, shardNums);
            CountDownLatch tableLatch = new CountDownLatch(tablesToShardsMap.get(tableName).size());
            for(Integer shardID:tablesToShardsMap.get(tableName)){
                BrokerDataStoreGrpc.BrokerDataStoreStub stub = BrokerDataStoreGrpc.newStub(getStubForShard(shardID).getChannel());
                RetrieveAndCombineQueryMessage requestMessage = RetrieveAndCombineQueryMessage.newBuilder()
                        .setTxID(txID)
                        .setShardID(shardID)
                        .setSerializedQueryPlan(serializedQueryPlan)
                        .setLastCommittedVersion(lcv)
                        .setTableName(tableName)
                        .setSubquery(false)
                        .setConcreteSubqueriesResults(serializedConcreteSubqueriesResults)
                        .build();
                StreamObserver<RetrieveAndCombineQueryResponse> responseStreamObserver = new StreamObserver<>() {
                    @Override
                    public void onNext(RetrieveAndCombineQueryResponse response) {
                        if(response.getState() == QUERY_SUCCESS){
                            retrievedData.get(tableName).add(response.getData());
                            if(plan.isStored()){
                                destinationShardIDs.add(response.getDestinationShards());
                            }
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
                if(Utilities.logger_flag)
                    logger.error("Retrieve and combine query failed");
            }
        }
        for(String subqueryID: volatileSubqueriesResults.keySet()){
            retrievedData.put(subqueryID, new CopyOnWriteArrayList<>());
            CountDownLatch tableLatch = new CountDownLatch(volatileSubqueriesResults.get(subqueryID).size());
            for(Pair<Integer, Integer> shardIDDSIDPair: volatileSubqueriesResults.get(subqueryID)){
                Integer shardID = shardIDDSIDPair.getValue0();
                Integer dsID = shardIDDSIDPair.getValue1();
                BrokerDataStoreGrpc.BrokerDataStoreStub stub = BrokerDataStoreGrpc.newStub(dsIDToChannelMap.get(dsID));
                RetrieveAndCombineQueryMessage requestMessage = RetrieveAndCombineQueryMessage.newBuilder()
                        .setTxID(txID)
                        .setShardID(shardID)
                        .setSerializedQueryPlan(serializedQueryPlan)
                        .setLastCommittedVersion(lcv)
                        .setTableName(subqueryID)
                        .setConcreteSubqueriesResults(serializedConcreteSubqueriesResults)
                        .setSubquery(true)
                        .build();
                StreamObserver<RetrieveAndCombineQueryResponse> responseStreamObserver = new StreamObserver<>() {
                    @Override
                    public void onNext(RetrieveAndCombineQueryResponse response) {
                        if(response.getState() == QUERY_SUCCESS){
                            retrievedData.get(subqueryID).add(response.getData());
                            if(plan.isStored()){
                                destinationShardIDs.add(response.getDestinationShards());
                            }
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
                if(Utilities.logger_flag)
                    logger.error("Retrieve and combine query failed");
            }
        }
        long aggStart = System.nanoTime();

        for(Map.Entry<String, List<ByteString>> e: retrievedData.entrySet()){
            if(e.getValue() == null){
                retrievedData.remove(e.getKey());
            }
        }
        V ret = plan.combine(retrievedData);

        long aggEnd = System.nanoTime();
        aggregationTimes.add((aggEnd - aggStart) / 1000L);

        Set<Integer> dsIDs = dsIDToChannelMap.keySet();

        if(plan.isStored()){
            Set<Integer> destinationShardsSet = new HashSet<>();
            for(ByteString bs: destinationShardIDs){
                List<Integer> destinationShards = (List<Integer>) Utilities.byteStringToObject(bs);
                destinationShardsSet.addAll(destinationShards);
            }
            HashMap<Integer, Integer> shardIDtoDSId = new HashMap<>();
            for(Integer shardID: destinationShardsSet){
                shardIDtoDSId.put(shardID, consistentHash.getRandomBucket(shardID));
            }
            CountDownLatch latch1 = new CountDownLatch(dsIDs.size());
            ByteString serShardIDToDSId = Utilities.objectToByteString(shardIDtoDSId);
            for(Integer dsID: dsIDs){
                ManagedChannel channel = dsIDToChannelMap.get(dsID);
                BrokerDataStoreGrpc.BrokerDataStoreStub stub = BrokerDataStoreGrpc.newStub(channel);
                ForwardDataToStoreMessage m = ForwardDataToStoreMessage.newBuilder()
                        .setTxID(txID)
                        .setShardIDToDSIDMap(serShardIDToDSId)
                        .build();
                StreamObserver<ForwardDataToStoreResponse> responseObserver = new StreamObserver<ForwardDataToStoreResponse>() {
                    @Override
                    public void onNext(ForwardDataToStoreResponse forwardDataToStoreResponse) {
                        assert(forwardDataToStoreResponse.getStatus() == Broker.QUERY_SUCCESS);
                    }

                    @Override
                    public void onError(Throwable throwable) {

                    }

                    @Override
                    public void onCompleted() {
                        latch1.countDown();
                    }
                };
                stub.forwardDataToStore(m, responseObserver);
            }
            try{
                latch1.await();
            }catch (Exception e ){
                assert (false);
            }

            this.writeCachedData(plan.getWriteResultPlan(), txID, shardIDtoDSId);
            removeCachedResults(txID);
        }

        //TODO: parallelize
        for(Map.Entry<String, List<Pair<Integer, Integer>>> subqueryRes: volatileSubqueriesResults.entrySet()){
            for(Pair<Integer, Integer> shardIDDSiD: subqueryRes.getValue()){
                removeIntermediateShard(shardIDDSiD.getValue0(), shardIDDSiD.getValue1());
            }
        }

        return ret;
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
        private final CountDownLatch queryLatch;
        private final AtomicInteger queryStatus;
        private final AtomicBoolean statusWritten;

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
                                if(Utilities.logger_flag)
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
                    if(Utilities.logger_flag)
                        logger.error("Write Interrupted: {}", e.getMessage());
                    assert (false);
                }
                if (subQueryStatus.get() == QUERY_RETRY) {
                    try {
                        observer.onCompleted();
                        Thread.sleep(shardMapDaemonSleepDurationMillis);
                        continue;
                    } catch (InterruptedException e) {
                        if(Utilities.logger_flag)
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
                    if(Utilities.logger_flag)
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
                    if(Utilities.logger_flag)
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
        private final AtomicInteger queryStatus;

        SimpleWriteQueryThread(int shardNum, SimpleWriteQueryPlan<R, S> writeQueryPlan, R[] rowArray, long txID,
                         AtomicInteger queryStatus) {
            this.shardNum = shardNum;
            this.writeQueryPlan = writeQueryPlan;
            this.rowArray = rowArray;
            this.txID = txID;
            this.queryStatus = queryStatus;
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
                                if(Utilities.logger_flag)
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
                    if(Utilities.logger_flag)
                        logger.error("SimpleWrite Interrupted: {}", e.getMessage());
                    assert (false);
                }
                if (subQueryStatus.get() == QUERY_RETRY) {
                    try {
                        observer.onCompleted();
                        Thread.sleep(shardMapDaemonSleepDurationMillis);
                        continue;
                    } catch (InterruptedException e) {
                        if(Utilities.logger_flag)
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
                    if(Utilities.logger_flag)
                        logger.error("SimpleWrite Interrupted: {}", e.getMessage());
                    assert (false);
                }
            }
        }
    }
    private class WriteCachedDataThread<R extends Row, S extends Shard> extends Thread{
        private final int shardNum;
        private final SimpleWriteQueryPlan<R, S> writeQueryPlan;
        private final long txID;
        private final AtomicInteger queryStatus;
        private final int dsID;

        WriteCachedDataThread(int shardNum, SimpleWriteQueryPlan<R, S> writeQueryPlan, long txID,
                               AtomicInteger queryStatus, int dsID) {
            this.shardNum = shardNum;
            this.writeQueryPlan = writeQueryPlan;
            this.txID = txID;
            this.queryStatus = queryStatus;
            this.dsID = dsID;
        }

        @Override
        public void run() { writeQuery(); }

        private void writeQuery() {
            AtomicInteger subQueryStatus = new AtomicInteger(QUERY_RETRY);
            while (subQueryStatus.get() == QUERY_RETRY) {
                BrokerDataStoreGrpc.BrokerDataStoreStub stub = BrokerDataStoreGrpc.newStub(dsIDToChannelMap.get(dsID));
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
                                if(Utilities.logger_flag)
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
                WriteQueryMessage prepare = WriteQueryMessage.newBuilder()
                        .setSerializedQuery(Utilities.objectToByteString(writeQueryPlan))
                        .setTxID(txID)
                        .setShard(shardNum)
                        .setIsDataCached(true)
                        .setWriteState(DataStore.PREPARE)
                        .build();
                observer.onNext(prepare);
                try {
                    prepareLatch.await();
                } catch (InterruptedException e) {
                    if(Utilities.logger_flag)
                        logger.error("SimpleWrite Interrupted: {}", e.getMessage());
                    assert (false);
                }
                if (subQueryStatus.get() == QUERY_RETRY) {
                    try {
                        observer.onCompleted();
                        Thread.sleep(shardMapDaemonSleepDurationMillis);
                        continue;
                    } catch (InterruptedException e) {
                        if(Utilities.logger_flag)
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
                    if(Utilities.logger_flag)
                        logger.error("SimpleWrite Interrupted: {}", e.getMessage());
                    assert (false);
                }
            }
        }
    }


    /**Forwards raw data to servers*/
    private class StoreVolatileDataThread<R extends Row> extends Thread{
        private final Integer dsID;
        private final long txID;
        private final R[] rowArray;
        private final AtomicInteger queryStatus;
        private final VolatileShuffleQueryPlan plan;

        StoreVolatileDataThread(Integer dsID,
                                long txID,
                                R[] rowArray,
                                AtomicInteger queryStatus,
                                VolatileShuffleQueryPlan plan
        ){
            this.dsID = dsID;
            this.txID = txID;
            this.rowArray = rowArray;
            this.queryStatus = queryStatus;
            this.plan = plan;
        }

        @Override
        public void run(){
            storeVolatileData();
        }

        private void storeVolatileData(){
            AtomicInteger subQueryStatus = new AtomicInteger(QUERY_RETRY);
            ByteString serializedPlan = Utilities.objectToByteString(plan);
            while (subQueryStatus.get() == QUERY_RETRY) {
                ManagedChannel channel = dsIDToChannelMap.get(dsID);
                assert(channel != null);
                BrokerDataStoreGrpc.BrokerDataStoreBlockingStub blockingStub = BrokerDataStoreGrpc.newBlockingStub(channel);
                BrokerDataStoreGrpc.BrokerDataStoreStub stub = BrokerDataStoreGrpc.newStub(blockingStub.getChannel());

                final CountDownLatch prepareLatch = new CountDownLatch(1);

                StreamObserver<StoreVolatileDataMessage> observer =
                        stub.storeVolatileData(new StreamObserver<>() {
                            @Override
                            public void onNext(StoreVolatileDataResponse storeVolatileDataResponse) {
                                subQueryStatus.set(storeVolatileDataResponse.getState());
                                prepareLatch.countDown();
                            }

                            @Override
                            public void onError(Throwable th) {
                                if(Utilities.logger_flag)
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
                        .setState(DataStore.COMMIT).setPlan(serializedPlan).build();
                observer.onNext(confirmMessage);
                try {
                    prepareLatch.await();
                } catch (InterruptedException e) {
                    if(Utilities.logger_flag)
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
    /**Triggers execution of the scatter operation between servers storing raw data assocciated with the transaction
     * identifier*/
    private class ScatterVolatileDataThread<V> extends Thread{
        private final long txID;
        private final List<Integer> gatherDSlist;
        private final AtomicInteger queryStatus;
        private final Integer dsID;
        private final VolatileShuffleQueryPlan<V, Shard> plan;

        public ScatterVolatileDataThread(
                Integer dsID,
                long txID,
                List<Integer> gatherDSlist,
                AtomicInteger queryStatus,
                VolatileShuffleQueryPlan<V, Shard> plan){
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
    /**Triggers the execution of the gather operation in those servers that store scattered data*/
    private class GatherVolatileDataThread<V> extends Thread{
        private final Integer dsID;
        private final long txID;
        private final List<ByteString> gatherResults;
        private final AtomicInteger queryStatus;
        private final VolatileShuffleQueryPlan<V, Shard> plan;

        public GatherVolatileDataThread(Integer dsID,
                                 long txID,
                                 List<ByteString> gatherResults,
                                 AtomicInteger queryStatus,
                                 VolatileShuffleQueryPlan<V, Shard> plan){
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
     * @param tableName The name of the queried table
     * @return The table info object associated with the given name
     * */
    public TableInfo getTableInfo(String tableName) {
        TableInfoResponse r = coordinatorBlockingStub.tableInfo(TableInfoMessage.newBuilder().setTableName(tableName).build());
        assert(r.getReturnCode() == QUERY_SUCCESS);
        TableInfo t = new TableInfo(tableName, r.getId(), r.getNumShards());
        ByteString serializedAttributeNames = r.getAttributeNames();
        Object[] attributeNamesArray = new Object[0];
        if(serializedAttributeNames != null){
            attributeNamesArray = (Object[]) Utilities.byteStringToObject(r.getAttributeNames());
        }
        List<String> attributeNames = new ArrayList<>();
        for (Object o:attributeNamesArray){
            attributeNames.add((String) o);
        }
        t.setAttributeNames(attributeNames);
        t.setKeyStructure((Boolean[]) Utilities.byteStringToObject(r.getKeyStructure()));
        t.setRegisteredQueries((ArrayList<ReadQuery>) Utilities.byteStringToObject(r.getTriggeredQueries()));
        t.setTableShardsIDs((ArrayList<Integer>) Utilities.byteStringToObject(r.getShardIDs()));
        return t;
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
    /**Cleans the in memory structures holding both the raw data and the scattered data for the execution of a
     * volatile shuffle query
     * @param dsIDsScatter the server identifiers of those servers storing raw data
     * @param txID the volatile shuffle transaction's id
     * @param gatherDSids the server identifiers of those servers storing scattered data
     * @return true if and only if all data associated with the given transaction id has been removed by the
     * servers involved in the transaction
     * */
    private boolean volatileShuffleCleanup(List<Integer> dsIDsScatter, long txID, List<Integer> gatherDSids){
        CountDownLatch latch = new CountDownLatch(dsIDsScatter.size());
        AtomicInteger outcome = new AtomicInteger(0);
        for(Integer dsID: dsIDsScatter){
            ManagedChannel channel = dsIDToChannelMap.get(dsID);
            BrokerDataStoreGrpc.BrokerDataStoreStub stub = BrokerDataStoreGrpc.newStub(channel);
            RemoveVolatileDataMessage message = RemoveVolatileDataMessage.newBuilder().setTransactionID(txID).build();
            StreamObserver<RemoveVolatileDataResponse> observer = new StreamObserver<>() {
                @Override
                public void onNext(RemoveVolatileDataResponse removeVolatileDataResponse) {

                }

                @Override
                public void onError(Throwable throwable) {
                    if(Utilities.logger_flag)
                        logger.error("Broker: removeVolatileData Failed for transaction id {}", txID);
                    outcome.set(1);
                    latch.countDown();
                }

                @Override
                public void onCompleted() {
                    latch.countDown();
                }
            };
            stub.removeVolatileData(message, observer);
        }
        try {
            latch.await();
        }catch (InterruptedException e){
            if(Utilities.logger_flag)
                logger.error("Broker: RemoveVolatileData Failed for transaction id {}", txID);
            return false;
        }
        if(gatherDSids == null || outcome.get() != 0) {
            return outcome.get() == 0;
        }
        final CountDownLatch secondLatch = new CountDownLatch(gatherDSids.size());
        for(Integer dsID: gatherDSids){
            ManagedChannel channel = dsIDToChannelMap.get(dsID);
            BrokerDataStoreGrpc.BrokerDataStoreStub stub = BrokerDataStoreGrpc.newStub(channel);
            RemoveVolatileDataMessage message = RemoveVolatileDataMessage.newBuilder().setTransactionID(txID).build();
            StreamObserver<RemoveVolatileDataResponse> observer = new StreamObserver<>() {
                @Override
                public void onNext(RemoveVolatileDataResponse removeVolatileDataResponse) {

                }

                @Override
                public void onError(Throwable throwable) {
                    if(Utilities.logger_flag)
                        logger.error("Broker: removeVolatileScatteredData Failed for transaction id {}", txID);
                    outcome.set(1);
                }

                @Override
                public void onCompleted() {
                    secondLatch.countDown();
                }
            };
            stub.removeVolatileScatteredData(message, observer);
        }
        try{
            secondLatch.await();
        }catch (InterruptedException e){
            if(Utilities.logger_flag)
                logger.error("Broker: removeVolatileScatteredData Failed for transaction id {}", txID);
            return false;
        }
        return outcome.get() == 0;
    }
    public String registerQuery(ReadQuery query){
        Integer resultTableID = zkCurator.getResultTableID();
        query.setResultTableName(Integer.toString(resultTableID));
        boolean isTableCreated = createTable(Integer.toString(resultTableID), SHARDS_PER_TABLE, query.getResultSchema(), query.getKeyStructure());
        if(!isTableCreated){
            throw new RuntimeException("Query cannot be registered");
        }
        StoreQueryResponse queryResponse = coordinatorBlockingStub.storeQuery(
                StoreQueryMessage.newBuilder().setQuery(Utilities.objectToByteString(query)).build());
        if(Utilities.logger_flag)
            logger.info("Query associated with table {} registered", resultTableID);
        assert (queryResponse.getStatus() == 0);
        return Integer.toString(resultTableID);
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

    private void removeIntermediateShard(Integer shardID, Integer dataStoreID){
        BrokerDataStoreGrpc.BrokerDataStoreStub stub = BrokerDataStoreGrpc.newStub(dsIDToChannelMap.get(dataStoreID));
        RemoveIntermediateShardMessage message = RemoveIntermediateShardMessage.newBuilder().setShardID(shardID).build();
        StreamObserver<RemoveIntermediateShardResponse> responseObserver = new StreamObserver<>() {
            @Override
            public void onNext(RemoveIntermediateShardResponse r) {}

            @Override
            public void onError(Throwable throwable) {
                if(Utilities.logger_flag)
                    logger.error("Error deleting intermediate shard {} from server {}", shardID, dataStoreID);
            }

            @Override
            public void onCompleted() {}
        };
        stub.removeIntermediateShards(message, responseObserver);
    }
    private boolean removeCachedResults(Long txID){
        CountDownLatch latch = new CountDownLatch(dsIDToChannelMap.size());
        AtomicInteger outcome = new AtomicInteger(0);
        for(Integer dsID: dsIDToChannelMap.keySet()){
            ManagedChannel channel = dsIDToChannelMap.get(dsID);
            BrokerDataStoreGrpc.BrokerDataStoreStub stub = BrokerDataStoreGrpc.newStub(channel);
            RemoveVolatileDataMessage message = RemoveVolatileDataMessage.newBuilder().setTransactionID(txID).build();
            StreamObserver<RemoveVolatileDataResponse> observer = new StreamObserver<>() {
                @Override
                public void onNext(RemoveVolatileDataResponse removeVolatileDataResponse) {}

                @Override
                public void onError(Throwable throwable) {
                    if(Utilities.logger_flag)
                        logger.error("Broker: removeVolatileData Failed for transaction id {}", txID);
                    outcome.set(1);
                    latch.countDown();
                }

                @Override
                public void onCompleted() {
                    latch.countDown();
                }
            };
            stub.removeCachedData(message, observer);
        }
        try {
            latch.await();
        }catch (InterruptedException e){
            if(Utilities.logger_flag)
                logger.error("Broker: RemoveVolatileData Failed for transaction id {}", txID);
            return false;
        }
        return outcome.get() == 0;
    }
}