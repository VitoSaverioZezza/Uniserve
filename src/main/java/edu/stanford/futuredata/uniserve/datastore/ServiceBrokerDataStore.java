package edu.stanford.futuredata.uniserve.datastore;

import com.google.protobuf.ByteString;
import edu.stanford.futuredata.uniserve.*;
import edu.stanford.futuredata.uniserve.interfaces.ReadQueryPlan;
import edu.stanford.futuredata.uniserve.interfaces.Row;
import edu.stanford.futuredata.uniserve.interfaces.Shard;
import edu.stanford.futuredata.uniserve.interfaces.WriteQueryPlan;
import edu.stanford.futuredata.uniserve.utilities.Utilities;
import io.grpc.stub.StreamObserver;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

class ServiceBrokerDataStore<R extends Row, S extends Shard> extends BrokerDataStoreGrpc.BrokerDataStoreImplBase {

    private static final Logger logger = LoggerFactory.getLogger(ServiceBrokerDataStore.class);
    private final DataStore<R, S> dataStore;

    ServiceBrokerDataStore(DataStore<R, S> dataStore) {
        this.dataStore = dataStore;
    }

    private Map<Integer, CommitLockerThread<R, S>> activeCLTs = new HashMap<>();

    @Override
    public void writeQueryPreCommit(WriteQueryPreCommitMessage request, StreamObserver<WriteQueryPreCommitResponse> responseObserver) {
        responseObserver.onNext(writeQueryPreCommitHandler(request));
        responseObserver.onCompleted();
    }

    private WriteQueryPreCommitResponse writeQueryPreCommitHandler(WriteQueryPreCommitMessage rowMessage) {
        int shardNum = rowMessage.getShard();
        long txID = rowMessage.getTxID();
        if (dataStore.primaryShardMap.containsKey(shardNum)) {
            WriteQueryPlan<R, S> writeQueryPlan;
            List<R> rows;
            writeQueryPlan = (WriteQueryPlan<R, S>) Utilities.byteStringToObject(rowMessage.getSerializedQuery());
            rows = Arrays.asList((R[]) Utilities.byteStringToObject(rowMessage.getRowData()));
            S shard = dataStore.primaryShardMap.get(shardNum);
            // Use the CommitLockerThread to acquire the shard's write lock.
            CommitLockerThread<R, S> commitLockerThread = new CommitLockerThread<>(activeCLTs, shardNum, writeQueryPlan, rows, dataStore.shardLockMap.get(shardNum).writeLock(), txID, dataStore.dsID);
            commitLockerThread.acquireLock();
            List<DataStoreDataStoreGrpc.DataStoreDataStoreStub> replicaStubs = dataStore.replicaStubsMap.get(shardNum);
            int numReplicas = replicaStubs.size();
            ReplicaPreCommitMessage rm = ReplicaPreCommitMessage.newBuilder()
                    .setShard(shardNum)
                    .setSerializedQuery(rowMessage.getSerializedQuery())
                    .setRowData(rowMessage.getRowData())
                    .setTxID(txID)
                    .setVersionNumber(dataStore.shardVersionMap.get(shardNum))
                    .build();
            AtomicInteger numReplicaSuccesses = new AtomicInteger(0);
            Semaphore semaphore = new Semaphore(0);
            for (DataStoreDataStoreGrpc.DataStoreDataStoreStub stub: replicaStubs) {
                StreamObserver<ReplicaPreCommitResponse> responseObserver = new StreamObserver<>() {
                    @Override
                    public void onNext(ReplicaPreCommitResponse replicaPreCommitResponse) {
                        numReplicaSuccesses.incrementAndGet();
                        semaphore.release();
                    }

                    @Override
                    public void onError(Throwable throwable) {
                        logger.error("DS{} Replica PreCommit Error: {}", dataStore.dsID, throwable.getMessage());
                        semaphore.release();
                    }

                    @Override
                    public void onCompleted() {
                    }
                };
                stub.replicaPreCommit(rm, responseObserver);
            }
            boolean primaryWriteSuccess = writeQueryPlan.preCommit(shard, rows);
            try {
                semaphore.acquire(numReplicas);
            } catch (InterruptedException e) {
                logger.error("DS{} Write Query Interrupted Shard {}: {}", dataStore.dsID, shardNum, e.getMessage());
                assert(false);
            }
            int addRowReturnCode;
            if (primaryWriteSuccess && numReplicaSuccesses.get() == numReplicas) {
                addRowReturnCode = 0;
            } else {
                addRowReturnCode = 1;
            }
            return WriteQueryPreCommitResponse.newBuilder().setReturnCode(addRowReturnCode).build();
        } else {
            logger.warn("DS{} Primary got write request for absent shard {}", dataStore.dsID, shardNum);
            return WriteQueryPreCommitResponse.newBuilder().setReturnCode(1).build();
        }
    }

    @Override
    public void writeQueryCommit(WriteQueryCommitMessage request, StreamObserver<WriteQueryCommitResponse> responseObserver) {
        responseObserver.onNext(writeQueryCommitHandler(request));
        responseObserver.onCompleted();
    }

    private WriteQueryCommitResponse writeQueryCommitHandler(WriteQueryCommitMessage rowMessage) {
        int shardNum = rowMessage.getShard();
        CommitLockerThread<R, S> commitCLT = activeCLTs.get(shardNum);
        assert(commitCLT != null);  // The commit locker thread holds the shard's write lock.
        assert(commitCLT.txID == rowMessage.getTxID());
        WriteQueryPlan<R, S> writeQueryPlan = commitCLT.writeQueryPlan;
        if (dataStore.primaryShardMap.containsKey(shardNum)) {
            boolean commitOrAbort = rowMessage.getCommitOrAbort(); // Commit on true, abort on false.
            S shard = dataStore.primaryShardMap.get(shardNum);

            List<DataStoreDataStoreGrpc.DataStoreDataStoreStub> replicaStubs = dataStore.replicaStubsMap.get(shardNum);
            int numReplicas = replicaStubs.size();
            ReplicaCommitMessage rm = ReplicaCommitMessage.newBuilder()
                    .setShard(shardNum)
                    .setCommitOrAbort(commitOrAbort)
                    .setTxID(rowMessage.getTxID())
                    .build();
            Semaphore semaphore = new Semaphore(0);
            for (DataStoreDataStoreGrpc.DataStoreDataStoreStub stub: replicaStubs) {
                StreamObserver<ReplicaCommitResponse> responseObserver = new StreamObserver<>() {
                    @Override
                    public void onNext(ReplicaCommitResponse replicaCommitResponse) {
                        semaphore.release();
                    }

                    @Override
                    public void onError(Throwable throwable) {
                        logger.error("DS{} Replica Commit Error: {}", dataStore.dsID, throwable.getMessage());
                        semaphore.release();
                    }

                    @Override
                    public void onCompleted() {
                    }
                };
                stub.replicaCommit(rm, responseObserver);
            }
            if (commitOrAbort) {
                writeQueryPlan.commit(shard);
                int newVersionNumber = dataStore.shardVersionMap.get(shardNum) + 1;
                Map<Integer, Pair<WriteQueryPlan<R, S>, List<R>>> shardWriteLog = dataStore.writeLog.get(shardNum);
                shardWriteLog.put(newVersionNumber, new Pair<>(writeQueryPlan, commitCLT.rows));
                dataStore.shardVersionMap.put(shardNum, newVersionNumber);  // Increment version number
            } else {
                writeQueryPlan.abort(shard);
            }
            try {
                semaphore.acquire(numReplicas);
            } catch (InterruptedException e) {
                logger.error("DS{} Write Query Interrupted Shard {}: {}", dataStore.dsID, shardNum, e.getMessage());
                assert(false);
            }
            // Have the commit locker thread release the shard's write lock.
            commitCLT.releaseLock();
        } else {
            logger.error("DS{} Got valid commit request on absent shard {} (!!!!!)", dataStore.dsID, shardNum);
            assert(false);
        }
        return WriteQueryCommitResponse.newBuilder().build();
    }

    @Override
    public void readQuery(ReadQueryMessage request,  StreamObserver<ReadQueryResponse> responseObserver) {
        responseObserver.onNext(readQueryHandler(request));
        responseObserver.onCompleted();
    }

    private ReadQueryResponse readQueryHandler(ReadQueryMessage readQuery) {
        int shardNum = readQuery.getShard();
        S shard = dataStore.replicaShardMap.getOrDefault(shardNum, null);
        if (shard == null) {
            shard = dataStore.primaryShardMap.getOrDefault(shardNum, null);
        }
        if (shard != null) {
            ByteString serializedQuery = readQuery.getSerializedQuery();
            ReadQueryPlan<S, Serializable, Object> readQueryPlan;
            readQueryPlan = (ReadQueryPlan<S, Serializable, Object>) Utilities.byteStringToObject(serializedQuery);
            dataStore.shardLockMap.get(shardNum).readLock().lock();
            Serializable queryResult = readQueryPlan.queryShard(shard);
            dataStore.shardLockMap.get(shardNum).readLock().unlock();
            ByteString queryResponse;
            queryResponse = Utilities.objectToByteString(queryResult);
            return ReadQueryResponse.newBuilder().setReturnCode(0).setResponse(queryResponse).build();
        } else {
            logger.warn("DS{} Got read request for absent shard {}", dataStore.dsID, shardNum);
            return ReadQueryResponse.newBuilder().setReturnCode(1).build();
        }
    }
}