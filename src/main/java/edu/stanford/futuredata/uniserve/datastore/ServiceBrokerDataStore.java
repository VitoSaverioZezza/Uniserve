package edu.stanford.futuredata.uniserve.datastore;

import com.google.protobuf.ByteString;
import edu.stanford.futuredata.uniserve.*;
import edu.stanford.futuredata.uniserve.broker.Broker;
import edu.stanford.futuredata.uniserve.interfaces.*;
import edu.stanford.futuredata.uniserve.utilities.Utilities;
import edu.stanford.futuredata.uniserve.utilities.ZKShardDescription;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

class ServiceBrokerDataStore<R extends Row, S extends Shard> extends BrokerDataStoreGrpc.BrokerDataStoreImplBase {

    private static final Logger logger = LoggerFactory.getLogger(ServiceBrokerDataStore.class);
    private final DataStore<R, S> dataStore;

    ServiceBrokerDataStore(DataStore<R, S> dataStore) {
        this.dataStore = dataStore;
    }

    /*WRITE QUERIES*/
    @Override
    public StreamObserver<WriteQueryMessage> writeQuery(StreamObserver<WriteQueryResponse> responseObserver) {
        return new StreamObserver<>() {
            int shardNum;
            long txID;
            WriteQueryPlan<R, S> writeQueryPlan;
            final List<R[]> rowArrayList = new ArrayList<>();
            int lastState = DataStore.COLLECT;
            List<R> rows;
            final List<StreamObserver<ReplicaWriteMessage>> replicaObservers = new ArrayList<>();
            final Semaphore commitSemaphore = new Semaphore(0);
            WriteLockerThread t;

            @Override
            public void onNext(WriteQueryMessage writeQueryMessage) {
                int writeState = writeQueryMessage.getWriteState();
                if (writeState == DataStore.COLLECT) {
                    assert (lastState == DataStore.COLLECT);
                    shardNum = writeQueryMessage.getShard();
                    dataStore.createShardMetadata(shardNum);
                    txID = writeQueryMessage.getTxID();
                    writeQueryPlan = (WriteQueryPlan<R, S>) Utilities.byteStringToObject(writeQueryMessage.getSerializedQuery()); // TODO:  Only send this once.
                    R[] rowChunk = (R[]) Utilities.byteStringToObject(writeQueryMessage.getRowData());
                    rowArrayList.add(rowChunk);
                } else if (writeState == DataStore.PREPARE) {
                    assert (lastState == DataStore.COLLECT);
                    rows = rowArrayList.stream().flatMap(Arrays::stream).collect(Collectors.toList());
                    if (dataStore.shardLockMap.containsKey(shardNum)) {
                        t = new WriteLockerThread(dataStore.shardLockMap.get(shardNum));
                        t.acquireLock();
                        long tStart = System.currentTimeMillis();
                        responseObserver.onNext(prepareWriteQuery(shardNum, txID, writeQueryPlan));
                        logger.info("DS{} Write {} Execution Time: {}", dataStore.dsID, txID, System.currentTimeMillis() - tStart);
                    } else {
                        responseObserver.onNext(WriteQueryResponse.newBuilder().setReturnCode(Broker.QUERY_RETRY).build());
                    }
                } else if (writeState == DataStore.COMMIT) {
                    assert (lastState == DataStore.PREPARE);
                    commitWriteQuery(shardNum, txID, writeQueryPlan);
                    lastState = writeState;
                    t.releaseLock();
                } else if (writeState == DataStore.ABORT) {
                    assert (lastState == DataStore.PREPARE);
                    abortWriteQuery(shardNum, txID, writeQueryPlan);
                    lastState = writeState;
                    t.releaseLock();
                }
                lastState = writeState;
            }

            @Override
            public void onError(Throwable throwable) {
                logger.warn("DS{} Primary Write RPC Error Shard {} {}", dataStore.dsID, shardNum, throwable.getMessage());
                if (lastState == DataStore.PREPARE) {
                    if (dataStore.zkCurator.getTransactionStatus(txID) == DataStore.COMMIT) {
                        commitWriteQuery(shardNum, txID, writeQueryPlan);
                    } else {
                        abortWriteQuery(shardNum, txID, writeQueryPlan);
                    }
                    t.releaseLock();
                }
            }

            @Override
            public void onCompleted() {
                responseObserver.onCompleted();
            }

            private WriteQueryResponse prepareWriteQuery(int shardNum, long txID, WriteQueryPlan<R, S> writeQueryPlan) {
                if (dataStore.consistentHash.getBuckets(shardNum).contains(dataStore.dsID)) {
                    dataStore.ensureShardCached(shardNum);
                    S shard;
                    if (dataStore.readWriteAtomicity) {
                        ZKShardDescription z = dataStore.zkCurator.getZKShardDescription(shardNum);
                        if (z == null) {
                            shard = dataStore.shardMap.get(shardNum); // This is the first commit.
                        } else {
                            Optional<S> shardOpt =
                                    dataStore.useReflink ?
                                            dataStore.copyShardToDir(shardNum, z.cloudName, z.versionNumber) :
                                            dataStore.downloadShardFromCloud(shardNum, z.cloudName, z.versionNumber);
                            assert (shardOpt.isPresent());
                            shard = shardOpt.get();
                        }
                        dataStore.multiVersionShardMap.get(shardNum).put(txID, shard);
                    } else {
                        shard = dataStore.shardMap.get(shardNum);
                    }
                    assert(shard != null);
                    List<DataStoreDataStoreGrpc.DataStoreDataStoreStub> replicaStubs =
                            dataStore.replicaDescriptionsMap.get(shardNum).stream().map(i -> i.stub).collect(Collectors.toList());
                    int numReplicas = replicaStubs.size();
                    R[] rowArray;
                    rowArray = (R[]) rows.toArray(new Row[0]);
                    AtomicBoolean success = new AtomicBoolean(true);
                    Semaphore prepareSemaphore = new Semaphore(0);
                    for (DataStoreDataStoreGrpc.DataStoreDataStoreStub stub : replicaStubs) {
                        StreamObserver<ReplicaWriteMessage> observer = stub.replicaWrite(new StreamObserver<>() {
                            @Override
                            public void onNext(ReplicaWriteResponse replicaResponse) {
                                if (replicaResponse.getReturnCode() != 0) {
                                    logger.warn("DS{} Replica Prepare Failed Shard {}", dataStore.dsID, shardNum);
                                    success.set(false);
                                }
                                prepareSemaphore.release();
                            }

                            @Override
                            public void onError(Throwable throwable) {
                                logger.warn("DS{} Replica Prepare RPC Failed Shard {} {}", dataStore.dsID, shardNum, throwable.getMessage());
                                success.set(false);
                                prepareSemaphore.release();
                                commitSemaphore.release();
                            }

                            @Override
                            public void onCompleted() {
                                commitSemaphore.release();
                            }
                        });
                        final int stepSize = 10000;
                        for (int i = 0; i < rowArray.length; i += stepSize) {
                            ByteString serializedQuery = Utilities.objectToByteString(writeQueryPlan);
                            R[] rowSlice = Arrays.copyOfRange(rowArray, i, Math.min(rowArray.length, i + stepSize));
                            ByteString rowData = Utilities.objectToByteString(rowSlice);
                            ReplicaWriteMessage rm = ReplicaWriteMessage.newBuilder()
                                    .setShard(shardNum)
                                    .setSerializedQuery(serializedQuery)
                                    .setRowData(rowData)
                                    .setVersionNumber(dataStore.shardVersionMap.get(shardNum))
                                    .setWriteState(DataStore.COLLECT)
                                    .setTxID(txID)
                                    .build();
                            observer.onNext(rm);
                        }
                        ReplicaWriteMessage rm = ReplicaWriteMessage.newBuilder()
                                .setWriteState(DataStore.PREPARE)
                                .build();
                        observer.onNext(rm);
                        replicaObservers.add(observer);
                    }
                    boolean primaryWriteSuccess = writeQueryPlan.preCommit(shard, rows);
                    lastState = DataStore.PREPARE;
                    try {
                        prepareSemaphore.acquire(numReplicas);
                    } catch (InterruptedException e) {
                        logger.error("DS{} Write Query Interrupted Shard {}: {}", dataStore.dsID, shardNum, e.getMessage());
                        assert (false);
                    }
                    int returnCode;
                    if (primaryWriteSuccess && success.get()) {
                        returnCode = Broker.QUERY_SUCCESS;
                    } else {
                        returnCode = Broker.QUERY_FAILURE;
                    }
                    return WriteQueryResponse.newBuilder().setReturnCode(returnCode).build();
                } else {
                    logger.warn("DS{} Primary got write request for unassigned shard {}", dataStore.dsID, shardNum);
                    t.releaseLock();
                    return WriteQueryResponse.newBuilder().setReturnCode(Broker.QUERY_RETRY).build();
                }
            }
            private void commitWriteQuery(int shardNum, long txID, WriteQueryPlan<R, S> writeQueryPlan) {
                S shard;
                if (dataStore.readWriteAtomicity) {
                    shard = dataStore.multiVersionShardMap.get(shardNum).get(txID);
                } else {
                    shard = dataStore.shardMap.get(shardNum);
                }
                for (StreamObserver<ReplicaWriteMessage> observer : replicaObservers) {
                    ReplicaWriteMessage rm = ReplicaWriteMessage.newBuilder()
                            .setWriteState(DataStore.COMMIT)
                            .build();
                    observer.onNext(rm);
                    observer.onCompleted();
                }
                writeQueryPlan.commit(shard);
                dataStore.shardMap.put(shardNum, shard);
                int newVersionNumber = dataStore.shardVersionMap.get(shardNum) + 1;
                if (rows.size() < 10000) {
                    Map<Integer, Pair<WriteQueryPlan<R, S>, List<R>>> shardWriteLog = dataStore.writeLog.get(shardNum);
                    shardWriteLog.put(newVersionNumber, new Pair<>(writeQueryPlan, rows));
                }
                dataStore.shardVersionMap.put(shardNum, newVersionNumber);  // Increment version number
                // Upload the updated shard.
                if (dataStore.dsCloud != null) {
                    dataStore.uploadShardToCloud(shardNum);
                }
                try {
                    commitSemaphore.acquire(replicaObservers.size());
                } catch (InterruptedException e) {
                    logger.error("DS{} Write Query Interrupted Shard {}: {}", dataStore.dsID, shardNum, e.getMessage());
                    assert (false);
                }
            }
            private void abortWriteQuery(int shardNum, long txID, WriteQueryPlan<R, S> writeQueryPlan) {
                for (StreamObserver<ReplicaWriteMessage> observer : replicaObservers) {
                    ReplicaWriteMessage rm = ReplicaWriteMessage.newBuilder()
                            .setWriteState(DataStore.ABORT)
                            .build();
                    observer.onNext(rm);
                    observer.onCompleted();
                }
                if (dataStore.readWriteAtomicity) {
                    S shard = dataStore.multiVersionShardMap.get(shardNum).get(txID);
                    writeQueryPlan.abort(shard);
                    shard.destroy();
                    dataStore.multiVersionShardMap.get(shardNum).remove(txID);
                } else {
                    S shard = dataStore.shardMap.get(shardNum);
                    writeQueryPlan.abort(shard);
                }
                try {
                    commitSemaphore.acquire(replicaObservers.size());
                } catch (InterruptedException e) {
                    logger.error("DS{} Write Query Interrupted Shard {}: {}", dataStore.dsID, shardNum, e.getMessage());
                    assert (false);
                }
            }

        };
    }
    @Override
    public StreamObserver<WriteQueryMessage> simpleWriteQuery(StreamObserver<WriteQueryResponse> responseObserver) {
        return new StreamObserver<>() {
            int shardNum;
            long txID;
            SimpleWriteQueryPlan<R, S> writeQueryPlan;
            List<R[]> rowArrayList = new ArrayList<>();
            int lastState = DataStore.COLLECT;
            List<R> rows;
            List<StreamObserver<ReplicaWriteMessage>> replicaObservers = new ArrayList<>();

            @Override
            public void onNext(WriteQueryMessage writeQueryMessage) {
                int writeState = writeQueryMessage.getWriteState();
                if (writeState == DataStore.COLLECT) {
                    assert (lastState == DataStore.COLLECT);
                    shardNum = writeQueryMessage.getShard();
                    dataStore.createShardMetadata(shardNum);
                    txID = writeQueryMessage.getTxID();
                    writeQueryPlan = (SimpleWriteQueryPlan<R, S>) Utilities.byteStringToObject(writeQueryMessage.getSerializedQuery()); // TODO:  Only send this once.
                    R[] rowChunk = (R[]) Utilities.byteStringToObject(writeQueryMessage.getRowData());
                    rowArrayList.add(rowChunk);
                } else if (writeState == DataStore.PREPARE) {
                    assert (lastState == DataStore.COLLECT);
                    rows = rowArrayList.stream().flatMap(Arrays::stream).collect(Collectors.toList());
                    if (dataStore.shardLockMap.containsKey(shardNum)) {
                        long tStart = System.currentTimeMillis();
                        dataStore.shardLockMap.get(shardNum).writerLockLock();
                        responseObserver.onNext(executeWriteQuery(shardNum, txID, writeQueryPlan));
                        logger.info("DS{} SimpleWrite {} Execution Time: {}", dataStore.dsID, txID, System.currentTimeMillis() - tStart);
                    } else {
                        responseObserver.onNext(WriteQueryResponse.newBuilder().setReturnCode(Broker.QUERY_RETRY).build());
                    }
                }
                lastState = writeState;
            }

            @Override
            public void onError(Throwable throwable) {
                logger.warn("DS{} Primary SimpleWrite RPC Error Shard {} {}", dataStore.dsID, shardNum, throwable.getMessage());
                // TODO: Implement.
            }

            @Override
            public void onCompleted() {
                responseObserver.onCompleted();
            }

            private WriteQueryResponse executeWriteQuery(int shardNum, long txID, SimpleWriteQueryPlan<R, S> writeQueryPlan) {
                if (dataStore.consistentHash.getBuckets(shardNum).contains(dataStore.dsID)) {
                    logger.info("attempt to execute DataStore.ensureShardCached for shard {} in SBDS.SWQ", shardNum);
                    dataStore.ensureShardCached(shardNum);
                    S shard = dataStore.shardMap.get(shardNum);
                    assert(shard != null);
                    List<DataStoreDataStoreGrpc.DataStoreDataStoreStub> replicaStubs =
                            dataStore.replicaDescriptionsMap.get(shardNum).stream().map(i -> i.stub).collect(Collectors.toList());
                    int numReplicas = replicaStubs.size();
                    R[] rowArray;
                    rowArray = (R[]) rows.toArray(new Row[0]);
                    AtomicBoolean success = new AtomicBoolean(true);

                    boolean primaryWriteSuccess = writeQueryPlan.write(shard, rows);

                    int newVersionNumber = dataStore.shardVersionMap.get(shardNum) + 1;
                    if (rows.size() < 10000) {
                        Map<Integer, Pair<WriteQueryPlan<R, S>, List<R>>> shardWriteLog = dataStore.writeLog.get(shardNum);
                        shardWriteLog.put(newVersionNumber, new Pair<>(null, rows)); // TODO: Fix
                    }
                    dataStore.shardVersionMap.put(shardNum, newVersionNumber);  // Increment version number
                    // Upload the updated shard.
                    if (dataStore.dsCloud != null) {
                        logger.info("uploading shard {} on cloud", shardNum);
                        dataStore.uploadShardToCloud(shardNum);
                    }

                    dataStore.shardLockMap.get(shardNum).writerLockUnlock();

                    CountDownLatch replicaLatch = new CountDownLatch(replicaStubs.size());
                    for (DataStoreDataStoreGrpc.DataStoreDataStoreStub stub : replicaStubs) {
                        StreamObserver<ReplicaWriteMessage> observer = stub.simpleReplicaWrite(new StreamObserver<>() {
                            @Override
                            public void onNext(ReplicaWriteResponse replicaResponse) {
                                if (replicaResponse.getReturnCode() != 0) {
                                    logger.warn("DS{} SimpleReplica Prepare Failed Shard {}", dataStore.dsID, shardNum);
                                    success.set(false);
                                }
                                replicaLatch.countDown();
                            }

                            @Override
                            public void onError(Throwable throwable) {
                                logger.warn("DS{} SimpleReplica Prepare RPC Failed Shard {} {}", dataStore.dsID, shardNum, throwable.getMessage());
                                success.set(false);
                                replicaLatch.countDown();
                            }

                            @Override
                            public void onCompleted() {}
                        });
                        final int stepSize = 10000;
                        for (int i = 0; i < rowArray.length; i += stepSize) {
                            ByteString serializedQuery = Utilities.objectToByteString(writeQueryPlan);
                            R[] rowSlice = Arrays.copyOfRange(rowArray, i, Math.min(rowArray.length, i + stepSize));
                            ByteString rowData = Utilities.objectToByteString(rowSlice);
                            ReplicaWriteMessage rm = ReplicaWriteMessage.newBuilder()
                                    .setShard(shardNum)
                                    .setSerializedQuery(serializedQuery)
                                    .setRowData(rowData)
                                    .setVersionNumber(dataStore.shardVersionMap.get(shardNum))
                                    .setWriteState(DataStore.COLLECT)
                                    .setTxID(txID)
                                    .build();
                            observer.onNext(rm);
                        }
                        ReplicaWriteMessage rm = ReplicaWriteMessage.newBuilder()
                                .setWriteState(DataStore.PREPARE)
                                .build();
                        observer.onNext(rm);
                        replicaObservers.add(observer);
                    }
                    try {
                        replicaLatch.await();
                    } catch (InterruptedException e) {
                        logger.error("Write SimpleReplication Interrupted: {}", e.getMessage());
                        assert (false);
                    }

                    int returnCode;
                    if (primaryWriteSuccess) {
                        returnCode = Broker.QUERY_SUCCESS;
                    } else {
                        returnCode = Broker.QUERY_FAILURE;
                    }

                    return WriteQueryResponse.newBuilder().setReturnCode(returnCode).build();
                } else {
                    dataStore.shardLockMap.get(shardNum).writerLockUnlock();
                    logger.warn("DS{} Primary got SimpleWrite request for unassigned shard {}", dataStore.dsID, shardNum);
                    return WriteQueryResponse.newBuilder().setReturnCode(Broker.QUERY_RETRY).build();
                }
            }
        };
    }

    @Override
    public StreamObserver<MapQueryMessage> mapQuery(StreamObserver<MapQueryResponse> responseObserver) {
        return new StreamObserver<>() {
            final List<R[]> rowSlicesToBeTransformed = new ArrayList<>();
            MapQueryPlan<R> mapQueryPlan;

            @Override
            public void onNext(MapQueryMessage mapQueryMessage) {
                mapQueryPlan = (MapQueryPlan<R>) Utilities.byteStringToObject(mapQueryMessage.getSerializedQuery());
                R[] rowChunk = (R[]) Utilities.byteStringToObject(mapQueryMessage.getRowData());
                rowSlicesToBeTransformed.add(rowChunk);
            }

            @Override
            public void onError(Throwable throwable) {
                responseObserver.onError(new Exception("gRPC error while communicating row chunks"));
                responseObserver.onCompleted();
            }

            @Override
            public void onCompleted() {
                for (R[] rowArray : rowSlicesToBeTransformed) {
                    List<R> subList = new ArrayList<>();
                    for(R row: rowArray){
                        subList.add(row);
                    }

                    boolean mapSuccess = mapQueryPlan.map(subList);

                    if (mapSuccess) {
                        R[] result = (R[]) new Row[subList.size()];
                        for (int i = 0; i<result.length; i++){
                            result[i] = subList.get(i);
                        }
                        ByteString resultData = Utilities.objectToByteString(result);
                        MapQueryResponse m = MapQueryResponse.newBuilder()
                                .setTransformedData(resultData)
                                .setState(0)
                                .build();
                        responseObserver.onNext(m);
                    } else {
                        responseObserver.onError(new Exception("Map function error"));
                        break;
                    }
                }
                responseObserver.onCompleted();
            }
        };
    }

    @Override
    public void anchoredReadQuery(AnchoredReadQueryMessage request,  StreamObserver<AnchoredReadQueryResponse> responseObserver) {
        responseObserver.onNext(anchoredReadQueryHandler(request));
        responseObserver.onCompleted();
    }
    /**
     * This method is executed once for each shard of the anchor table for a given AnchoredReadQueryPlan

     * Given a shard identifier of the anchor table, this method retrieves the locations of all non-anchor shards
     * involved in the query and for each of those interrogates the remote service anchoredShuffle of
     * ServiceDataStoreDataStore, which returns the results of the scatter operation associated with the given shard id.

     * A scatter operation is executed for each non-anchor shard, and it has access to all anchor-shard identifiers and
     * partition keys of the rows of the anchor table involved in the query (for a given anchor shard, the partition keys
     * are the ones returned by the AnchoredReadQueryPlan.getPartitionKeys(anchor-shard) method defined by the user,
     * these values are part of the query definition)

     * The results of all the scatter operations for all non-anchor shards are then passed as parameters to a gather method
     * call and the results will be either directly returned to the caller or will be stored into an intermediate shard
     * local to the datastore executing the method. In the latter case, the location of this newly created intermediate shard
     * is returned to the caller to be later accessed.

     * TL;DR Given a shard of the anchor table, executes a scatter operation for each non-anchor shard and a single
     * gather operation whose results are returned to the caller either as a ByteString value or as a location to a
     * locally stored intermediate shard storing the results.
     *
     * @param m The AnchoredReadQueryMessage storing the following information:
     *          <p>- serializedQuery: The serialized AnchoredReadQueryPlan encoding the logic and the information needed
     *                  to perform the query</p>
     *          <p>- targetShards: A serialized Map< String, List< Integer>> where the key represent a table name and
     *                  the elements of the associated list represents the partition keys of the rows to be queried on
     *                  that table.</p>
     *          <p>- intermediateShards: A serialized Map< String, Map< Integer, Integer>> object where a key represents a
     *                  table name and an entry's value is a mapping between the intermediate shard identifier and the
     *                  datastore identifier storing this intermediate shard.</p>
     *          <p>- targetShard: the anchor shard identifier this service execution is responsible for.</p>
     *          <p>- lastCommittedVersion</p>
     * */
    private AnchoredReadQueryResponse anchoredReadQueryHandler(AnchoredReadQueryMessage m) {

        /*
        * This method is executed once for each shard of the anchor query. The results returned by this call can be
        * either an actual value to be combined via the combine function or some sort of intermediate shard location
        * The message content are:
        *   -plan: the query plan generating the request
        *   -allTargetShards: a Map<TableName, List<ShardsIDs to be queried relative to the table>> that also includes all
        *           shards resulting from intermediate queries
        *   -intermediateShards: a Map<TableName, Map<ShardID to be queried on the table, datastoreID storing the shard>>.
        *           where this information is related to all previously executed sub queries
        *   -localShardNum: the anchor table's shard that caused this very call
        *   -lastCommittedVersion
        *
        * */

        AnchoredReadQueryPlan<S, Object> plan =
                (AnchoredReadQueryPlan<S, Object>) Utilities.byteStringToObject(m.getSerializedQuery());
        Map<String, List<Integer>> allTargetShards =
                (Map<String, List<Integer>>) Utilities.byteStringToObject(m.getTargetShards());
        Map<String, Map<Integer, Integer>> intermediateShards =
                (Map<String, Map<Integer, Integer>>) Utilities.byteStringToObject(m.getIntermediateShards());
        Map<String, List<ByteString>> ephemeralData = new HashMap<>();
        Map<String, S> ephemeralShards = new HashMap<>();

        /*The shard of the anchor table for which this method has been called is retrieved*/

        int localShardNum = m.getTargetShard();
        String anchorTableName = plan.getAnchorTable();
        dataStore.createShardMetadata(localShardNum);
        dataStore.shardLockMap.get(localShardNum).readerLockLock();
        if (!dataStore.consistentHash.getBuckets(localShardNum).contains(dataStore.dsID)) {
            logger.warn("DS{} Got anchored read request for unassigned local shard {}", dataStore.dsID, localShardNum);
            dataStore.shardLockMap.get(localShardNum).readerLockUnlock();
            return AnchoredReadQueryResponse.newBuilder().setReturnCode(Broker.QUERY_RETRY).build();
        }
        dataStore.ensureShardCached(localShardNum);
        S localShard;
        long lastCommittedVersion = m.getLastCommittedVersion();
        if (dataStore.readWriteAtomicity) {
            if (dataStore.multiVersionShardMap.get(localShardNum).containsKey(lastCommittedVersion)) {
                localShard = dataStore.multiVersionShardMap.get(localShardNum).get(lastCommittedVersion);
            } else { // TODO: Retrieve the older version from somewhere else?
                logger.info("DS{} missing shard {} version {}", dataStore.dsID, localShardNum, lastCommittedVersion);
                return AnchoredReadQueryResponse.newBuilder().setReturnCode(Broker.QUERY_FAILURE).build();
            }
        } else {
            localShard = dataStore.shardMap.get(localShardNum);
        }
        assert(localShard != null);

        /*
         * If there's more than a single queried table in the plan, an ephemeral shard is created for each table and
         *   placed in the ephemeralShard mapping.
         * The list of tables is iterated and if the table is not the anchor table (meaning that there's more than a
         *   table being queried, therefore there is an ephemeral shard associated with the currently iterated table),
         *   The list of shards to be queried in the table is retrieved and a random datastore is selected
         *   for each shard. The datastore is interrogated via the AnchoredShuffle method.
         *       The anchoredShuffle executes the AnchoredReadQueryPlan.scatter method once for each non-anchor shard,
         *       once the datastore has been interrogated by all anchor shards.
         *       A scatter method returns a list of ByteStrings for each anchor shard, and it is called for each non-anchor
         *       shard.
         *       The anchoredShuffle response message for a given (anchor, non-anchor) shard pair consists of the list
         *       of ByteStrings associated to the anchor shard among those lists returned by the scatter operation
         *       related to the non anchor shard.
         *   This procedure is blocking, the next table's iteration does not start until all non-anchor shards of the
         *   current table have been processed and their partial scatter's results (the ByteStrings associated with the
         *   anchor shard this thread is responsible for) stored in the tableEphemeralData list.
         *
         *   The resulting tableEphemeralData list is stored in the Map<Non-Anchor table name, tableEphemeralData>
         *   called ephemeralData before the next table starts processing.
         *
         * */

        List<Integer> partitionKeys = plan.getPartitionKeys(localShard);
        for (String tableName: plan.getQueriedTables()) {
            if (plan.getQueriedTables().size() > 1) {
                S ephemeralShard = dataStore.createNewShard(dataStore.ephemeralShardNum.decrementAndGet()).get();
                ephemeralShards.put(tableName, ephemeralShard);
            }
            if (!tableName.equals(anchorTableName)) {
                List<Integer> targetShards = allTargetShards.get(tableName);
                List<ByteString> tableEphemeralData = new CopyOnWriteArrayList<>();
                CountDownLatch latch = new CountDownLatch(targetShards.size());
                for (int targetShard : targetShards) {
                    int targetDSID = intermediateShards.containsKey(tableName) ?
                            intermediateShards.get(tableName).get(targetShard) :
                            dataStore.consistentHash.getRandomBucket(targetShard); // TODO:  If it's already here, use it.
                    ManagedChannel channel = dataStore.getChannelForDSID(targetDSID);
                    DataStoreDataStoreGrpc.DataStoreDataStoreStub stub = DataStoreDataStoreGrpc.newStub(channel);
                    AnchoredShuffleMessage g = AnchoredShuffleMessage.newBuilder()
                            .setShardNum(targetShard)
                            .setNumRepartitions(m.getNumRepartitions())
                            .setRepartitionShardNum(localShardNum)
                            .setSerializedQuery(m.getSerializedQuery())
                            .setLastCommittedVersion(lastCommittedVersion)
                            .setTxID(m.getTxID())
                            .addAllPartitionKeys(partitionKeys)
                            .setTargetShardIntermediate(intermediateShards.containsKey(tableName))
                            .build();
                    StreamObserver<AnchoredShuffleResponse> responseObserver = new StreamObserver<>() {
                        @Override
                        public void onNext(AnchoredShuffleResponse r) {
                            if (r.getReturnCode() == Broker.QUERY_RETRY) {
                                onError(new Throwable());
                            } else {
                                tableEphemeralData.add(r.getShuffleData());
                            }
                        }

                        @Override
                        public void onError(Throwable throwable) {
                            logger.info("DS{}  Shuffle data error shard {}", dataStore.dsID, targetShard);
                            // TODO: First remove all ByteStrings added from this shard.
                            int targetDSID = dataStore.consistentHash.getRandomBucket(targetShard); // TODO:  If it's already here, use it.
                            ManagedChannel channel = dataStore.getChannelForDSID(targetDSID);
                            DataStoreDataStoreGrpc.DataStoreDataStoreStub stub = DataStoreDataStoreGrpc.newStub(channel);
                            stub.anchoredShuffle(g, this);
                        }

                        @Override
                        public void onCompleted() {
                            latch.countDown();
                        }
                    };
                    /*
                    * The returned messages contain the result of the scatter operation for the pair
                    * non anchor shard - anchor shard. For each of this pair, the scatter returns a List of ByteStrings
                    * */
                    stub.anchoredShuffle(g, responseObserver);
                }
                try {
                    latch.await();
                } catch (InterruptedException ignored) {}

                ephemeralData.put(tableName, tableEphemeralData);
            }
        }
        AnchoredReadQueryResponse r;

        /*
        * If the query returns an aggregate value, this is computed as the value returned by the call of the user-defined
        * gather method, passing the anchor shard identifier, the ephemeralData structure (Map<TableName, List<ByteStrings>>
        * where the ByteStrings objects are returned by the AnchoredShuffle calls) and the result is sent back to the
        * Broker call.
        *
        * If the query returns a shard, the result shard is created and passed as return shard of the gather method call
        * A mapping intermediateShardLocation only containing the entry <ShardID, this DSid> is created and returned
        * to the Broker.
        * */

        try {
            if (plan.returnTableName().isEmpty()) {
                ByteString b = plan.gather(localShard, ephemeralData, ephemeralShards);
                r = AnchoredReadQueryResponse.newBuilder().setReturnCode(Broker.QUERY_SUCCESS).setResponse(b).build();
            } else {
                int intermediateShardNum = dataStore.ephemeralShardNum.decrementAndGet();
                dataStore.createShardMetadata(intermediateShardNum);
                S intermediateShard = dataStore.shardMap.get(intermediateShardNum);
                plan.gather(localShard, ephemeralData, ephemeralShards, intermediateShard);
                HashMap<Integer, Integer> intermediateShardLocation =
                        new HashMap<>(Map.of(intermediateShardNum, dataStore.dsID));
                ByteString b = Utilities.objectToByteString(intermediateShardLocation);
                r = AnchoredReadQueryResponse.newBuilder().setReturnCode(Broker.QUERY_SUCCESS).setResponse(b).build();
            }
        } catch (Exception e) {
            logger.warn("Read Query Exception: {}", e.getMessage());
            r = AnchoredReadQueryResponse.newBuilder().setReturnCode(Broker.QUERY_FAILURE).build();
        }
        dataStore.shardLockMap.get(localShardNum).readerLockUnlock();

        /*
        * The ephemeral shards related to all tables are destroyed, the Query Per Shard value associated to the
        * anchor shard is incremented and the response message that contain the result/resultShardLocation is returned
        * to the caller and sent back to the broker.
        * */

        ephemeralShards.values().forEach(S::destroy);
        long unixTime = Instant.now().getEpochSecond();
        dataStore.QPSMap.get(localShardNum).merge(unixTime, 1, Integer::sum);
        return r;
    }

    @Override
    public void shuffleReadQuery(ShuffleReadQueryMessage request,  StreamObserver<ShuffleReadQueryResponse> responseObserver) {
        responseObserver.onNext(shuffleReadQueryHandler(request));
        responseObserver.onCompleted();
    }
    private ShuffleReadQueryResponse shuffleReadQueryHandler(ShuffleReadQueryMessage m) {

        /*The message triggering the execution contains:
        * - repartitionNum: this datastore's dsID
        * - serializedQuery
        * - numRepartitions: the total number of datastores
        * - txID
        * - targetShards: the Map<tableName, List<Shards ids queried on the table>>*/

        ShuffleReadQueryPlan<S, Object> plan =
                (ShuffleReadQueryPlan<S, Object>) Utilities.byteStringToObject(m.getSerializedQuery());
        Map<String, List<Integer>> allTargetShards = (Map<String, List<Integer>>) Utilities.byteStringToObject(m.getTargetShards());
        Map<String, List<ByteString>> ephemeralData = new HashMap<>();
        Map<String, S> ephemeralShards = new HashMap<>();
        for (String tableName: plan.getQueriedTables()) {
            /*An ephemeral shard is created for each table being queried and these structures are
            * stored in the ephemeralShards mapping.
            *
            * A count down latch is initialized, needing a number of permits equal to the number of queried tables*/
            S ephemeralShard = dataStore.createNewShard(dataStore.ephemeralShardNum.decrementAndGet()).get();
            ephemeralShards.put(tableName, ephemeralShard);
            List<Integer> targetShards = allTargetShards.get(tableName);
            List<ByteString> tableEphemeralData = new CopyOnWriteArrayList<>();
            CountDownLatch latch = new CountDownLatch(targetShards.size());
            for (int targetShard : targetShards) {
                /*For each table, the shards to be queried are iterated and for each shard a datastore is
                * queried via the remote shuffle method. The message triggering the call contains the following
                * fields:
                * - shardNum: the identifier of the queried shard
                * - numRepartition: the total number of datastores
                * - repartitionNum: the datastore identifier of the client datastore
                * - serializedQuery
                * - txID
                *
                * The server returns a stream of ByteString objects obtained as result of the user-defined
                * scatter method. The scatter method executes once for each shard and returns a list of ByteString
                * for each datastore. The current datastore only receives the results associated with its identifier.
                *
                * The results are stored per table*/
                int targetDSID = dataStore.consistentHash.getRandomBucket(targetShard); // TODO:  If it's already here, use it.
                ManagedChannel channel = dataStore.getChannelForDSID(targetDSID);
                DataStoreDataStoreGrpc.DataStoreDataStoreStub stub = DataStoreDataStoreGrpc.newStub(channel);
                ShuffleMessage g = ShuffleMessage.newBuilder()
                        .setShardNum(targetShard)
                        .setNumRepartition(m.getNumRepartitions())
                        .setRepartitionNum(m.getRepartitionNum())
                        .setSerializedQuery(m.getSerializedQuery())
                        .setTxID(m.getTxID())
                        .build();
                StreamObserver<ShuffleResponse> responseObserver = new StreamObserver<>() {
                    @Override
                    public void onNext(ShuffleResponse r) {
                        if (r.getReturnCode() == Broker.QUERY_RETRY) {
                            onError(new Throwable());
                        } else {
                            tableEphemeralData.add(r.getShuffleData());
                        }
                    }

                    @Override
                    public void onError(Throwable throwable) {
                        logger.info("DS{}  Shuffle data error shard {}", dataStore.dsID, targetShard);
                        // TODO: First remove all ByteStrings added from this shard.
                        int targetDSID = dataStore.consistentHash.getRandomBucket(targetShard); // TODO:  If it's already here, use it.
                        ManagedChannel channel = dataStore.getChannelForDSID(targetDSID);
                        DataStoreDataStoreGrpc.DataStoreDataStoreStub stub = DataStoreDataStoreGrpc.newStub(channel);
                        stub.shuffle(g, this);
                    }

                    @Override
                    public void onCompleted() {
                        latch.countDown();
                    }
                };
                stub.shuffle(g, responseObserver);
            }
            try {
                latch.await();
            } catch (InterruptedException ignored) {
            }
            ephemeralData.put(tableName, tableEphemeralData);
        }

        /*Once all the shards have been queried, all the scatter's results associated with this identifier are stored
         * in the ephemeral data mapping
         * This map object is passed as argument to the user-defined gather operation, along with the mapping between
         * table identifiers and ephemeral shard objects created before
         *
         * Map<tableName, List<Scatter results associated with shards of the current table-ds pair>>
         * Map<tableName, ephemeral Shard associated to the table in this datastore>
         *
         * The gather returns a single bytestring object, which is forwarded to the client (broker)
         * */

        ByteString b = plan.gather(ephemeralData, ephemeralShards);
        ephemeralShards.values().forEach(S::destroy);
        return ShuffleReadQueryResponse.newBuilder().setReturnCode(Broker.QUERY_SUCCESS).setResponse(b).build();
    }

    @Override
    public void retrieveAndCombineQuery(RetrieveAndCombineQueryMessage request, StreamObserver<RetrieveAndCombineQueryResponse> responseObserver){
        responseObserver.onNext(retrieveAndCombineQueryHandler(request));
        responseObserver.onCompleted();
    }
    private RetrieveAndCombineQueryResponse retrieveAndCombineQueryHandler(RetrieveAndCombineQueryMessage request){
        int shardID = request.getShardID();
        ByteString serializedPlan = request.getSerializedQueryPlan();
        RetrieveAndCombineQueryPlan<S, Object> plan = (RetrieveAndCombineQueryPlan<S, Object>) Utilities.byteStringToObject(serializedPlan);
        if (!dataStore.consistentHash.getBuckets(shardID).contains(dataStore.dsID)) {
            logger.warn("DS{} Got r&c read request for unassigned local shard {}", dataStore.dsID, shardID);
            dataStore.shardLockMap.get(shardID).readerLockUnlock();
            return RetrieveAndCombineQueryResponse.newBuilder().setState(Broker.QUERY_FAILURE).build();
        }
        dataStore.ensureShardCached(shardID);
        S localShard;
        long lastCommittedVersion = request.getLastCommittedVersion();
        if (dataStore.readWriteAtomicity) {
            if (dataStore.multiVersionShardMap.get(shardID).containsKey(lastCommittedVersion)) {
                localShard = dataStore.multiVersionShardMap.get(shardID).get(lastCommittedVersion);
            } else { // TODO: Retrieve the older version from somewhere else?
                logger.info("DS{} missing shard {} version {}", dataStore.dsID, shardID, lastCommittedVersion);
                return RetrieveAndCombineQueryResponse.newBuilder().setState(Broker.QUERY_FAILURE).build();
            }
        } else {
            localShard = dataStore.shardMap.get(shardID);
        }
        ByteString retrievedData = plan.retrieve(localShard);
        return RetrieveAndCombineQueryResponse.newBuilder().setData(retrievedData).setState(Broker.QUERY_SUCCESS).build();
    }

    @Override
    public StreamObserver<StoreVolatileDataMessage> storeVolatileData(StreamObserver<StoreVolatileDataResponse> responseObserver){
     return new StreamObserver<>() {
         ArrayList<ByteString> volatileData = new ArrayList<>();
         Long txID;
         final AtomicBoolean success = new AtomicBoolean(true);
         @Override
         public void onNext(StoreVolatileDataMessage message) {
             if (message.getState() == DataStore.COLLECT) {
                 volatileData.add(message.getData());
                 txID = message.getTransactionID();
             } else if (message.getState() == DataStore.COMMIT) {
                 for (ByteString rowChunk : volatileData) {
                     success.set(dataStore.addVolatileData(txID, rowChunk) && success.get());
                 }
                 if (success.get()) {
                     responseObserver.onNext(StoreVolatileDataResponse.newBuilder().setState(Broker.QUERY_SUCCESS).build());
                 }else{
                     responseObserver.onNext(StoreVolatileDataResponse.newBuilder().setState(Broker.QUERY_FAILURE).build());
                 }
             }
         }

         @Override
         public void onError(Throwable throwable) {
             logger.error("SBDS: volatile store failed for transaction {} on datastore {}", txID, dataStore.dsID);
             responseObserver.onError(throwable);
         }

         @Override
         public void onCompleted() {
             responseObserver.onCompleted();
         }
     };
    }

    @Override
    public void removeVolatileData(RemoveVolatileDataMessage request, StreamObserver<RemoveVolatileDataResponse> responseObserver){
        responseObserver.onNext(removeVolatileDataHandler(request));
        responseObserver.onCompleted();
    }
    private RemoveVolatileDataResponse removeVolatileDataHandler(RemoveVolatileDataMessage m){
        dataStore.removeVolatileData(m.getTransactionID());
        return RemoveVolatileDataResponse.newBuilder().build();
    }

    @Override
    public void scatterVolatileData(ScatterVolatileDataMessage request, StreamObserver<ScatterVolatileDataResponse> responseStreamObserver){
        responseStreamObserver.onNext(scatterVolatileDataHandler(request));
        responseStreamObserver.onCompleted();
    }
    private ScatterVolatileDataResponse scatterVolatileDataHandler(ScatterVolatileDataMessage message){
        /*The return message contains the state and eventually all the datastore ids to which the
        * scattered data has been sent.
        * */
        VolatileShuffleQueryPlan<R, Object> plan = (VolatileShuffleQueryPlan<R, Object>) Utilities.byteStringToObject(message.getPlan());
        long txID = message.getTransactionID();
        List<ByteString> serializedVolatileData = dataStore.getVolatileData(txID);
        List<R> volatileData = new ArrayList<>();
        int actorCount = message.getActorCount();
        for(ByteString serializedRowChunk: serializedVolatileData){
            R[] rowChunk = (R[]) Utilities.byteStringToObject(serializedRowChunk);
            for(R row: rowChunk){
                volatileData.add(row);
            }
        }

        Map<Integer, List<ByteString>> scatterResults = plan.scatter(volatileData, actorCount);
        AtomicInteger shuffleStatus = new AtomicInteger(0);
        List<ForwardScatterResultsThread> forwardScatterResultsThreads = new ArrayList<>();

        for(Integer dsID: scatterResults.keySet()){
            ManagedChannel channel = dataStore.getChannelForDSID(dsID);
            DataStoreDataStoreGrpc.DataStoreDataStoreStub stub = DataStoreDataStoreGrpc.newStub(channel);
            ForwardScatterResultsThread t = new ForwardScatterResultsThread(dsID,scatterResults.get(dsID),txID,shuffleStatus, stub);
            t.start();
            forwardScatterResultsThreads.add(t);
        }

        for(ForwardScatterResultsThread t: forwardScatterResultsThreads){
            try {
                t.join();
            }catch (InterruptedException e ){
                logger.error("Broker: Volatile scatter query interrupted: {}", e.getMessage());
                assert(false);
                shuffleStatus.set(1);
            }
        }

        ScatterVolatileDataResponse response;
        Set<Integer> setDSids = scatterResults.keySet();
        ArrayList<Integer> listDSids = new ArrayList<>();
        for(Integer dsID: setDSids ){
            if(!listDSids.contains(dsID) && scatterResults.get(dsID) != null) {
                listDSids.add(dsID);
            }
        }
        ByteString serializedDsIDlist = Utilities.objectToByteString(listDSids);
        if(shuffleStatus.get()==1){
            /*An error occurred, delete volatile data from all ds and
            * return error code to the broker that will proceed to
            * delete other datastore's volatile*/
            response = ScatterVolatileDataResponse.newBuilder()
                    .setState(Broker.QUERY_FAILURE)
                    .setIdsDsGather(serializedDsIDlist)
                    .build();

        }else{
            response = ScatterVolatileDataResponse.newBuilder()
                    .setState(Broker.QUERY_SUCCESS)
                    .setIdsDsGather(serializedDsIDlist)
                    .build();
        }
        return response;
    }

    @Override
    public void removeVolatileScatteredData(RemoveVolatileDataMessage m, StreamObserver<RemoveVolatileDataResponse> responseObserver){
        responseObserver.onNext(removeVolatileScatteredDataHandler(m));
        responseObserver.onCompleted();
    }
    private RemoveVolatileDataResponse removeVolatileScatteredDataHandler(RemoveVolatileDataMessage m){
        dataStore.removeVolatileScatterData(m.getTransactionID());
        return RemoveVolatileDataResponse.newBuilder().build();
    }

    @Override
    public void gatherVolatileData(GatherVolatileDataMessage m, StreamObserver<GatherVolatileDataResponse> responseStreamObserver){
        responseStreamObserver.onNext(gatherVolatileDataHandler(m));
        responseStreamObserver.onCompleted();
    }
    private GatherVolatileDataResponse gatherVolatileDataHandler(GatherVolatileDataMessage message){
        /*The return message contains the state and eventually all the datastore ids to which the
         * scattered data has been sent.
         * */
        VolatileShuffleQueryPlan<R,Object> plan = (VolatileShuffleQueryPlan<R, Object>) Utilities.byteStringToObject(message.getPlan());
        long txID = message.getTransactionID();
        List<ByteString> scatterResults = dataStore.getVolatileScatterData(txID);
        ByteString gatherResult = plan.gather(scatterResults);
        GatherVolatileDataResponse response;
        if(gatherResult == null){
            response = GatherVolatileDataResponse.newBuilder().setState(Broker.QUERY_FAILURE).build();
        }else {
            response = GatherVolatileDataResponse.newBuilder().setGatherResult(gatherResult).setState(Broker.QUERY_SUCCESS).build();
        }
        return response;
    }

    private class ForwardScatterResultsThread extends Thread{
        private final Integer dsID;
        private final List<ByteString> dataToBeForwarded;
        private final long txID;
        private final AtomicInteger status;
        private final DataStoreDataStoreGrpc.DataStoreDataStoreStub stub;

        public ForwardScatterResultsThread(
                Integer dsID,
                List<ByteString> dataToBeForwarded,
                long txID,
                AtomicInteger status,
                DataStoreDataStoreGrpc.DataStoreDataStoreStub stub
        ){
            this.dsID = dsID;
            this.dataToBeForwarded = dataToBeForwarded;
            this.txID = txID;
            this.status = status;
            this.stub = stub;
        }

        @Override
        public void run(){forwardScatterResults();}

        private void forwardScatterResults(){
            CountDownLatch forwardLatch = new CountDownLatch(1);

            StreamObserver<StoreVolatileShuffleDataMessage> observer = stub.storeVolatileShuffledData(new StreamObserver<StoreVolatileShuffleDataResponse>(){
                @Override
                public void onNext(StoreVolatileShuffleDataResponse response){
                    forwardLatch.countDown();
                }
                @Override
                public void onError(Throwable th){
                    logger.error("Scattered data forwarding failed for transaction {} on datastore {}  towards datastore {}", txID, dataStore.dsID, dsID);
                    logger.info(th.getMessage());
                    status.set(1);
                    forwardLatch.countDown();
                }
                @Override
                public void onCompleted(){
                            forwardLatch.countDown();
                        }
            });

            if(dataToBeForwarded == null){
                ;
            }else{
                for(ByteString dataItem: dataToBeForwarded){
                    StoreVolatileShuffleDataMessage payload = StoreVolatileShuffleDataMessage.newBuilder()
                            .setTransactionID(txID)
                            .setData(dataItem)
                            .setState(0)
                            .build();
                    observer.onNext(payload);
                }
                StoreVolatileShuffleDataMessage confirm = StoreVolatileShuffleDataMessage.newBuilder()
                        .setTransactionID(txID)
                        .setState(1)
                        .build();
                observer.onNext(confirm);
            }
            try {
                forwardLatch.await();
            }catch (InterruptedException e){
                status.set(1);
                logger.error("SBDS.ForwardScatterResultsThread: forward failed from DS {} to DS {}", dataStore.dsID, dsID);
                logger.info(e.getMessage());
                assert (false);
            }
        }
    }
}