package edu.stanford.futuredata.uniserve.datastore;

import com.google.protobuf.ByteString;
import edu.stanford.futuredata.uniserve.*;
import edu.stanford.futuredata.uniserve.broker.Broker;
import edu.stanford.futuredata.uniserve.interfaces.*;
import edu.stanford.futuredata.uniserve.utilities.DataStoreDescription;
import edu.stanford.futuredata.uniserve.utilities.Utilities;
import edu.stanford.futuredata.uniserve.utilities.ZKShardDescription;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

class ServiceDataStoreDataStore<R extends Row, S extends Shard<R>> extends DataStoreDataStoreGrpc.DataStoreDataStoreImplBase {

    private static final Logger logger = LoggerFactory.getLogger(ServiceBrokerDataStore.class);
    private final DataStore<R, S> dataStore;

    private final Map<Pair<Long, Integer>, Semaphore> txSemaphores = new ConcurrentHashMap<>();
    private final Map<Pair<Long, Integer>, Map<Integer, List<ByteString>>> txShuffledData = new ConcurrentHashMap<>();
    private final Map<Pair<Long, Integer>, Map<Integer, List<Integer>>> txPartitionKeys = new ConcurrentHashMap<>();
    private final Map<Pair<Long, Integer>, AtomicInteger> txCounts = new ConcurrentHashMap<>();

    ServiceDataStoreDataStore(DataStore<R, S> dataStore) {
        this.dataStore = dataStore;
    }

    @Override
    public void bootstrapReplica(BootstrapReplicaMessage request, StreamObserver<BootstrapReplicaResponse> responseObserver) {
        responseObserver.onNext(bootstrapReplicaHandler(request));
        responseObserver.onCompleted();
    }
    @Override
    public StreamObserver<ReplicaWriteMessage> simpleReplicaWrite(StreamObserver<ReplicaWriteResponse> responseObserver) {
        return new StreamObserver<>() {
            int shardNum;
            int versionNumber;
            long txID;
            SimpleWriteQueryPlan<R, S> writeQueryPlan;
            final List<R[]> rowArrayList = new ArrayList<>();
            List<R> rowList;
            int lastState = DataStore.COLLECT;

            @Override
            public void onNext(ReplicaWriteMessage replicaWriteMessage) {
                int writeState = replicaWriteMessage.getWriteState();
                if (writeState == DataStore.COLLECT) {
                    assert(lastState == DataStore.COLLECT);
                    versionNumber = replicaWriteMessage.getVersionNumber();
                    shardNum = replicaWriteMessage.getShard();
                    txID = replicaWriteMessage.getTxID();
                    writeQueryPlan = (SimpleWriteQueryPlan<R, S>) Utilities.byteStringToObject(replicaWriteMessage.getSerializedQuery()); // TODO:  Only send this once.
                    R[] rowChunk = (R[]) Utilities.byteStringToObject(replicaWriteMessage.getRowData());
                    rowArrayList.add(rowChunk);
                } else if (writeState == DataStore.PREPARE) {
                    assert(lastState == DataStore.COLLECT);
                    rowList = rowArrayList.stream().flatMap(Arrays::stream).collect(Collectors.toList());
                    dataStore.shardLockMap.get(shardNum).writerLockLock();
                    responseObserver.onNext(executeReplicaWrite(shardNum, writeQueryPlan, rowList));
                    dataStore.shardLockMap.get(shardNum).writerLockUnlock();
                }
                lastState = writeState;
            }

            @Override
            public void onError(Throwable throwable) {
                logger.warn("DS{} SimpleReplica RPC Error Shard {} {}", dataStore.dsID, shardNum, throwable.getMessage());
                // TODO:  Implement.
            }

            @Override
            public void onCompleted() {
                responseObserver.onCompleted();
            }

            private ReplicaWriteResponse executeReplicaWrite(int shardNum, SimpleWriteQueryPlan<R, S> writeQueryPlan, List<R> rows) {
                if (dataStore.shardMap.containsKey(shardNum)) {
                    S shard = dataStore.shardMap.get(shardNum);
                    boolean success =  writeQueryPlan.write(shard, rows);
                    if (success) {
                        return ReplicaWriteResponse.newBuilder().setReturnCode(0).build();
                    } else {
                        return ReplicaWriteResponse.newBuilder().setReturnCode(1).build();
                    }
                } else {
                    logger.warn("DS{} SimpleReplica got write request for absent shard {}", dataStore.dsID, shardNum);
                    return ReplicaWriteResponse.newBuilder().setReturnCode(1).build();
                }
            }
        };
    }
    @Override
    public StreamObserver<ReplicaWriteMessage> replicaWrite(StreamObserver<ReplicaWriteResponse> responseObserver) {
        return new StreamObserver<>() {
            int shardNum;
            int versionNumber;
            long txID;
            WriteQueryPlan<R, S> writeQueryPlan;
            final List<R[]> rowArrayList = new ArrayList<>();
            List<R> rowList;
            int lastState = DataStore.COLLECT;
            WriteLockerThread t;

            @Override
            public void onNext(ReplicaWriteMessage replicaWriteMessage) {
                int writeState = replicaWriteMessage.getWriteState();
                if (writeState == DataStore.COLLECT) {
                    assert(lastState == DataStore.COLLECT);
                    versionNumber = replicaWriteMessage.getVersionNumber();
                    shardNum = replicaWriteMessage.getShard();
                    txID = replicaWriteMessage.getTxID();
                    writeQueryPlan = (WriteQueryPlan<R, S>) Utilities.byteStringToObject(replicaWriteMessage.getSerializedQuery()); // TODO:  Only send this once.
                    R[] rowChunk = (R[]) Utilities.byteStringToObject(replicaWriteMessage.getRowData());
                    rowArrayList.add(rowChunk);
                } else if (writeState == DataStore.PREPARE) {
                    assert(lastState == DataStore.COLLECT);
                    rowList = rowArrayList.stream().flatMap(Arrays::stream).collect(Collectors.toList());
                    t = new WriteLockerThread(dataStore.shardLockMap.get(shardNum));
                    t.acquireLock();
                    // assert(versionNumber == dataStore.shardVersionMap.get(shardNum));
                    responseObserver.onNext(prepareReplicaWrite(shardNum, writeQueryPlan, rowList));
                    lastState = writeState;
                } else if (writeState == DataStore.COMMIT) {
                    assert(lastState == DataStore.PREPARE);
                    commitReplicaWrite(shardNum, writeQueryPlan, rowList);
                    lastState = writeState;
                    t.releaseLock();
                } else if (writeState == DataStore.ABORT) {
                    assert(lastState == DataStore.PREPARE);
                    abortReplicaWrite(shardNum, writeQueryPlan);
                    lastState = writeState;
                    t.releaseLock();
                }
                lastState = writeState;
            }
            @Override
            public void onError(Throwable throwable) {
                logger.warn("DS{} Replica RPC Error Shard {} {}", dataStore.dsID, shardNum, throwable.getMessage());
                // TODO:  What if the primary fails after reporting a successful prepare but before the commit?
                if (lastState == DataStore.PREPARE) {
                    if (dataStore.zkCurator.getTransactionStatus(txID) == DataStore.COMMIT) {
                        commitReplicaWrite(shardNum, writeQueryPlan, rowList);
                    } else {
                        abortReplicaWrite(shardNum, writeQueryPlan);
                    }
                    t.releaseLock();
                }
            }
            @Override
            public void onCompleted() {
                responseObserver.onCompleted();
            }

            private ReplicaWriteResponse prepareReplicaWrite(int shardNum, WriteQueryPlan<R, S> writeQueryPlan, List<R> rows) {
                if (dataStore.shardMap.containsKey(shardNum)) {
                    S shard;
                    if (dataStore.readWriteAtomicity) {
                        ZKShardDescription z = dataStore.zkCurator.getZKShardDescription(shardNum);
                        if (z == null) {
                            shard = dataStore.shardMap.get(shardNum); // This is the first commit.
                        } else {
                            Optional<S> shardOpt = dataStore.downloadShardFromCloud(shardNum, z.cloudName, z.versionNumber);
                            assert (shardOpt.isPresent());
                            shard = shardOpt.get();
                        }
                        dataStore.multiVersionShardMap.get(shardNum).put(txID, shard);
                    } else {
                        shard = dataStore.shardMap.get(shardNum);
                    }
                    boolean success =  writeQueryPlan.preCommit(shard, rows);
                    if (success) {
                        return ReplicaWriteResponse.newBuilder().setReturnCode(0).build();
                    } else {
                        return ReplicaWriteResponse.newBuilder().setReturnCode(1).build();
                    }
                } else {
                    logger.warn("DS{} replica got write request for absent shard {}", dataStore.dsID, shardNum);
                    return ReplicaWriteResponse.newBuilder().setReturnCode(1).build();
                }
            }
            private void commitReplicaWrite(int shardNum, WriteQueryPlan<R, S> writeQueryPlan, List<R> rows) {
                S shard;
                if (dataStore.readWriteAtomicity) {
                    shard = dataStore.multiVersionShardMap.get(shardNum).get(txID);
                } else {
                    shard = dataStore.shardMap.get(shardNum);
                }
                writeQueryPlan.commit(shard);
                dataStore.shardMap.put(shardNum, shard);
                int newVersionNumber = dataStore.shardVersionMap.get(shardNum) + 1;
                Map<Integer, Pair<WriteQueryPlan<R, S>, List<R>>> shardWriteLog = dataStore.writeLog.get(shardNum);
                shardWriteLog.put(newVersionNumber, new Pair<>(writeQueryPlan, rows));
                dataStore.shardVersionMap.put(shardNum, newVersionNumber);  // Increment version number
            }
            private void abortReplicaWrite(int shardNum, WriteQueryPlan<R, S> writeQueryPlan) {
                if (dataStore.readWriteAtomicity) {
                    S shard = dataStore.multiVersionShardMap.get(shardNum).get(txID);
                    writeQueryPlan.abort(shard);
                    shard.destroy();
                    dataStore.multiVersionShardMap.get(shardNum).remove(txID);
                } else {
                    S shard = dataStore.shardMap.get(shardNum);
                    writeQueryPlan.abort(shard);
                }
            }
        };
    }
    @Override
    public void dataStorePing(DataStorePingMessage request, StreamObserver<DataStorePingResponse> responseObserver) {
        responseObserver.onNext(DataStorePingResponse.newBuilder().build());
        responseObserver.onCompleted();
    }
    @Override
    public void anchoredShuffle(AnchoredShuffleMessage m, StreamObserver<AnchoredShuffleResponse> responseObserver) {

        /*This remote method is called once for each shard of the anchor table, and a call is related to a shard
        * of a non-anchor table stored in the datastore responding to the remote call. The message contains:
        *   - txID
        *   - shardNum: shard of the non-anchor table stored here being interrogated
        *   - plan: AnchoredReadQueryPlan generating this call
        **/

        /*
        * A pair txID, non-anchor is created and the method checks that the datastore actually hosts the shard being
        * queried
        * */

        long txID = m.getTxID();
        int shardNum = m.getShardNum();
        AnchoredReadQueryPlan<S, Object> plan = (AnchoredReadQueryPlan<S, Object>) Utilities.byteStringToObject(m.getSerializedQuery());
        Pair<Long, Integer> mapID = new Pair<>(txID, shardNum);
        dataStore.createShardMetadata(shardNum);
        dataStore.shardLockMap.get(shardNum).readerLockLock();
        if (!dataStore.consistentHash.getBuckets(shardNum).contains(dataStore.dsID)
        && (!m.getTargetShardIntermediate() || !dataStore.shardVersionMap.containsKey(shardNum))) {
            logger.warn("DS{} Got read request for unassigned shard {}", dataStore.dsID, shardNum);
            dataStore.shardLockMap.get(shardNum).readerLockUnlock();
            responseObserver.onNext(AnchoredShuffleResponse.newBuilder().setReturnCode(Broker.QUERY_RETRY).build());
            responseObserver.onCompleted();
            return;
        }

        /*
        * The local txPartitionKeys is checked.
        * If this mapping does not contain an entry having key "mapID" (<txID, non-anchor-shard>), such an entry is created
        *   and associated with an empty Map<Integer, List<Integer>>.
        *   The map is then immediately populated with the entry <AnchorShardID, PartitionKeys>, where AnchorShardID
        *   is the identifier of the anchor shard associated with the caller creating the map and PartitionKeys is the
        *   result of the user-defined AnchoredReadQueryPlan.getPartitionKeys( AnchorShard ).
        * If the mapping does contain an entry having key "mapID" (created by a call associated with a different anchor
        *   shard), the mapping is updated in a similar fashion by adding to the associated mapping the entry
        *   <AnchorShardID, PartitionKeys>.
        *
        * Note that a call to this method, related to the same non anchor shard and transaction (i.e. to the same mapID)
        * will be performed by all datastores interrogated for an anchor shard. At the end of the day, this method
        * for this mapID will be called a number of times equal to the number of anchor shards being queried.
        *
        * For a generic call, the txPartitionKeys structure may contain an entry for the current mapID, whose associated
        * value is a mapping containing entries associated with some anchor shards. Assuming there are 3 anchor shards,
        * the last call will leave txPartitionKeys looking like something like this:
        *
        * Map < <txID, nonAnchorShard> , Map < AnchorShard1, <2,3,4,5> > >
        *                                    < AnchorShard2, <1,6>     > >
        *                                    < AnchorShard3, <9>       > >
        *
        * Note that no partition key belongs to two distinct shards for the same table.
        * Note that a partition key can be associated to different shards if those shards are part of distinct tables.
        *
        * */

        txPartitionKeys.computeIfAbsent(mapID, k -> new ConcurrentHashMap<>()).put(m.getRepartitionShardNum(), m.getPartitionKeysList());

        /*A semaphore with 0 permits is created, then the txCounts local structure is checked for an entry related to
        * the current mapID. If such an entry does not exist, it is initialized equal to 0
        * If this entry does exist (i.e. it has been created as a result of a call related to
        * a different anchor shard for the same non anchor shard), then the value is incremented and checked.
        *
        * If the value is NOT equal to the number of anchor shards of the anchor table of the current query, then the
        *   thread tries to acquire a permit from the semaphore, but since no permit has been released, it waits.
        *
        * If the value reaches the number of anchor shards of the anchor table, then the non-anchor shard is retrieved,
        *   the user-defined AnchoredReadQueryPlan.scatter method is called, passing:
        *       - The retrieved shard
        *       - All Map<AnchorShardID, List<PartitionKeys>> entries contained in the txPartitionKeys mapping
        *           that has been built before. Note that all lists content have been defined as a result of the
        *           getPartitionKeys method og the AnchoredReadQueryPlan, therefore it is user-defined and has no
        *           intrinsic propriety. It can be whatever list.
        *   The results of the scatter are stored in the txShuffledData and enough permits are released in order to let
        *   all previously blocked threads continue.
        *
        * TL;DR: The last call related to the transaction executes the scatter, all previous simply populate the
        *   txPartitionKeys structure then wait.
        *   */

        Semaphore s = txSemaphores.computeIfAbsent(mapID, k -> new Semaphore(0));
        if (txCounts.computeIfAbsent(mapID, k -> new AtomicInteger(0)).incrementAndGet() == m.getNumRepartitions()) {
            dataStore.ensureShardCached(shardNum);
            S shard;
            if (dataStore.readWriteAtomicity) {
                long lastCommittedVersion = m.getLastCommittedVersion();
                if (dataStore.multiVersionShardMap.get(shardNum).containsKey(lastCommittedVersion)) {
                    shard = dataStore.multiVersionShardMap.get(shardNum).get(lastCommittedVersion);
                } else { // TODO: Retrieve the older version from somewhere else?
                    logger.info("DS{} missing shard {} version {}", dataStore.dsID, shardNum, lastCommittedVersion);
                    responseObserver.onNext(AnchoredShuffleResponse.newBuilder().setReturnCode(Broker.QUERY_FAILURE).build());
                    responseObserver.onCompleted();
                    return;
                }
            } else {
                shard = dataStore.shardMap.get(shardNum);
            }
            assert (shard != null);
            Map<Integer, List<ByteString>> scatterResult = plan.scatter(shard, txPartitionKeys.get(mapID));
            txShuffledData.put(mapID, scatterResult);
            // long unixTime = Instant.now().getEpochSecond();
            // dataStore.QPSMap.get(shardNum).merge(unixTime, 1, Integer::sum);
            s.release(m.getNumRepartitions() - 1);
        } else {
            s.acquireUninterruptibly();
        }

        /*
        * All threads related to the various anchor shards will retrieve the scatterResults, which are in the
        * format Map<AnchorShardID, List<ByteString>>.
        *
        * Each call is related to an anchor shard and each one will retrieve the List related to its entry as
        * ephemeralData and send it back to the caller.
        *
        * */
        Map<Integer, List<ByteString>> scatterResult = txShuffledData.get(mapID);
        assert(scatterResult.containsKey(m.getRepartitionShardNum()));
        List<ByteString> ephemeralData = scatterResult.get(m.getRepartitionShardNum());
        scatterResult.remove(m.getRepartitionShardNum());  // TODO: Make reliable--what if map immutable?.
        if (scatterResult.isEmpty()) {
            txShuffledData.remove(mapID);
        }
        dataStore.shardLockMap.get(shardNum).readerLockUnlock();
        for (ByteString item: ephemeralData) {
            responseObserver.onNext(AnchoredShuffleResponse.newBuilder().setReturnCode(Broker.QUERY_SUCCESS).setShuffleData(item).build());
        }
        responseObserver.onCompleted();
    }
    @Override
    public void shuffle(ShuffleMessage m, StreamObserver<ShuffleResponse> responseObserver) {

        /*For each table, the shards to be queried are iterated and for each shard a datastore is
        * queried via the remote shuffle method. The message triggering the call contains the following
        * fields:
        * - shardNum: the identifier of the queried shard
        * - numRepartition: the total number of datastores
        * - repartitionNum: the datastore identifier of the client datastore
        * - serializedQuery
        * - txID
        *
        * The current datastore checks the requested shard is stored here. If so, it creates (if needed) a semaphore
        * having 0 permits, associated with the pair txID-requestedShardID
        * */

        long txID = m.getTxID();
        int shardNum = m.getShardNum();
        ShuffleOnReadQueryPlan<S, Object> plan=(ShuffleOnReadQueryPlan<S, Object>) Utilities.byteStringToObject(m.getSerializedQuery());

        Pair<Long, Integer> mapID = new Pair<>(txID, shardNum);
        dataStore.createShardMetadata(shardNum);
        dataStore.shardLockMap.get(shardNum).readerLockLock();
        if (!dataStore.consistentHash.getBuckets(shardNum).contains(dataStore.dsID)) {
            logger.warn("DS{} Got read request for unassigned shard {}", dataStore.dsID, shardNum);
            dataStore.shardLockMap.get(shardNum).readerLockUnlock();
            responseObserver.onNext(ShuffleResponse.newBuilder().setReturnCode(Broker.QUERY_RETRY).build());
            responseObserver.onCompleted();
            return;
        }
        Semaphore s = txSemaphores.computeIfAbsent(mapID, k -> new Semaphore(0));

        /*An atomic integer associated with the pair txID-queriedShardID is used to check that one and
        * only one ds executes the if block (in particular, the first one). The other datastore requests will wait
        * until the one executing the block completes its execution*/

        if (txCounts.computeIfAbsent(mapID, k -> new AtomicInteger(0)).compareAndSet(0, 1)) {

            /*The requested shard is retrieved and passed to the user-defined scatter method along with the total
            * number of datastores involved in the query (i.e. all datastores) and the results are in the form of a
            * Map < dsID, List<ByteString objects that are the result associated with the id>>
            *
            *  */

            dataStore.ensureShardCached(shardNum);
            S shard = dataStore.shardMap.get(shardNum);
            assert (shard != null);
            Map<Integer, List<ByteString>> scatterResult = plan.scatter(shard, m.getNumRepartition());
            if(scatterResult != null) {
                txShuffledData.put(mapID, scatterResult);
            }
            long unixTime = Instant.now().getEpochSecond();
            dataStore.QPSMap.get(shardNum).merge(unixTime, 1, Integer::sum);
            s.release(m.getNumRepartition() - 1);
        } else {
            s.acquireUninterruptibly();
        }

        /*Each datastore retrieves the results of the scatter associated with its ID and sends them back to the client*/

        Map<Integer, List<ByteString>> scatterResult = txShuffledData.get(mapID);
        if(scatterResult == null){
            responseObserver.onNext(ShuffleResponse
                    .newBuilder().setReturnCode(Broker.READ_NON_EXISTING_SHARD).build());
            responseObserver.onCompleted();
            return;
        }
        List<ByteString> ephemeralData = scatterResult.get(m.getRepartitionNum());
        if(ephemeralData == null){
            responseObserver.onNext(ShuffleResponse
                    .newBuilder().setReturnCode(Broker.READ_NON_EXISTING_SHARD).build());
            responseObserver.onCompleted();
            return;
        }
        scatterResult.remove(m.getRepartitionNum());  // TODO: Make reliable--what if map immutable?.
        if (scatterResult.isEmpty()) {
            txShuffledData.remove(mapID);
        }
        dataStore.shardLockMap.get(shardNum).readerLockUnlock();
        for (ByteString item: ephemeralData) {
            responseObserver.onNext(ShuffleResponse
                    .newBuilder().setReturnCode(Broker.QUERY_SUCCESS).setShuffleData(item).build());
        }
        responseObserver.onCompleted();
    }

    @Override
    public StreamObserver<StoreVolatileShuffleDataMessage> storeVolatileShuffledData(
            StreamObserver<StoreVolatileShuffleDataResponse> responseObserver){
        return new StreamObserver<StoreVolatileShuffleDataMessage>() {
            final ArrayList<ByteString> volatileScatterData = new ArrayList<>();
            Long txID;

            @Override
            public void onNext(StoreVolatileShuffleDataMessage message) {
                if(message.getState() == 0){
                    volatileScatterData.add(message.getData());
                    txID = message.getTransactionID();
                } else if (message.getState() == 1) {
                    for(ByteString dataItem : volatileScatterData){
                        dataStore.addVolatileScatterData(txID, dataItem);
                    }
                    responseObserver.onNext(StoreVolatileShuffleDataResponse.newBuilder().setState(0).build());
                    responseObserver.onCompleted();
                }
            }

            @Override
            public void onError(Throwable throwable) {
                logger.error("volatile store of scattered data failed for transaction {} on datastore {}", txID, dataStore.dsID);
                responseObserver.onError(throwable);
            }

            @Override
            public void onCompleted() {
            }
        };
    }


    private BootstrapReplicaResponse bootstrapReplicaHandler(BootstrapReplicaMessage request) {
        int shardNum = request.getShard();
        dataStore.shardLockMap.get(shardNum).writerLockLock();
        Integer replicaVersion = request.getVersionNumber();
        Integer primaryVersion = dataStore.shardVersionMap.get(shardNum);
        assert(primaryVersion != null);
        assert(replicaVersion <= primaryVersion);
        assert(dataStore.shardMap.containsKey(shardNum));  // TODO: Could fail during shard transfers?
        Map<Integer, Pair<WriteQueryPlan<R, S>, List<R>>> shardWriteLog = dataStore.writeLog.get(shardNum);
        if (replicaVersion.equals(primaryVersion)) {
            DataStoreDescription dsDescription = dataStore.zkCurator.getDSDescription(request.getDsID());
            ManagedChannel channel = ManagedChannelBuilder.forAddress(dsDescription.host, dsDescription.port).usePlaintext().build();
            DataStoreDataStoreGrpc.DataStoreDataStoreStub asyncStub = DataStoreDataStoreGrpc.newStub(channel);
            ReplicaDescription rd = new ReplicaDescription(request.getDsID(), channel, asyncStub);
            dataStore.replicaDescriptionsMap.get(shardNum).add(rd);
        }
        dataStore.shardLockMap.get(shardNum).writerLockUnlock();
        List<WriteQueryPlan<R, S>> writeQueryPlans = new ArrayList<>();
        List<R[]> rowListList = new ArrayList<>();
        for (int v = replicaVersion + 1; v <= primaryVersion; v++) {
            writeQueryPlans.add(shardWriteLog.get(v).getValue0());
            rowListList.add((R[]) shardWriteLog.get(v).getValue1().toArray(new Row[0]));
        }
        WriteQueryPlan<R, S>[] writeQueryPlansArray = writeQueryPlans.toArray(new WriteQueryPlan[0]);
        R[][] rowArrayArray = rowListList.toArray((R[][]) new Row[0][]);
        return BootstrapReplicaResponse.newBuilder().setReturnCode(0)
                .setVersionNumber(primaryVersion)
                .setWriteData(Utilities.objectToByteString(rowArrayArray))
                .setWriteQueries(Utilities.objectToByteString(writeQueryPlansArray))
                .build();
    }

}
