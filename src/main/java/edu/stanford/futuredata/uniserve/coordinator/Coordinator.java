package edu.stanford.futuredata.uniserve.coordinator;

import com.google.protobuf.ByteString;
import edu.stanford.futuredata.uniserve.*;
import edu.stanford.futuredata.uniserve.broker.Broker;
import edu.stanford.futuredata.uniserve.datastore.DataStore;
import edu.stanford.futuredata.uniserve.relationalapi.ReadQuery;
import edu.stanford.futuredata.uniserve.utilities.ConsistentHash;
import edu.stanford.futuredata.uniserve.utilities.DataStoreDescription;
import edu.stanford.futuredata.uniserve.utilities.TableInfo;
import edu.stanford.futuredata.uniserve.utilities.Utilities;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.javatuples.Triplet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

public class Coordinator {

    private static final Logger logger = LoggerFactory.getLogger(Coordinator.class);
    /**Coordinator's IP*/
    private final String coordinatorHost;
    /**Coordinator's process port*/
    private final int coordinatorPort;
    /**Server offering services specified in the Broker_datastore.proto and datastore_coordinator.proto files*/
    private final Server server;
    /**Coordinator's ZooKeeper client*/
    final CoordinatorCurator zkCurator;
    /**Load Balancer thread*/
    final LoadBalancer loadBalancer;
    /**AutoScaling thread*/
    final AutoScaler autoscaler;
    /**Coordinator's Cloud interface*/
    private final CoordinatorCloud cCloud;
    /**Lock on the server-actor assignment*/
    public final Lock consistentHashLock = new ReentrantLock();
    /**Server-actor assignment*/
    public final ConsistentHash consistentHash = new ConsistentHash();
    /**Used to assign each server a unique incremental ID.*/
    final AtomicInteger dataStoreNumber = new AtomicInteger(0);
    /**Map from server IDs to their description object*/
    final Map<Integer, DataStoreDescription> dataStoresMap = new ConcurrentHashMap<>();
    /**Used to assign each table a unique incremental ID.*/
    final AtomicInteger tableNumber = new AtomicInteger(0);
    /**Map from table names to tableInfos.*/
    final Map<String, TableInfo> tableInfoMap = new ConcurrentHashMap<>();
    /**Map from server IDs to their channels.*/
    final Map<Integer, ManagedChannel> dataStoreChannelsMap = new ConcurrentHashMap<>();
    /**Map from server IDs to their gRPC servers' stubs.*/
    final Map<Integer, CoordinatorDataStoreGrpc.CoordinatorDataStoreBlockingStub> dataStoreStubsMap = new ConcurrentHashMap<>();
    /**Map from actor id to their replicas.*/
    final Map<Integer, List<Integer>> shardToReplicaDataStoreMap = new ConcurrentHashMap<>();
    /**Map from sets of shards touched by query to frequency.*/
    public Map<Set<Integer>, Integer> queryStatistics = new ConcurrentHashMap<>();
    /** Lock on the queryStatistics map.*/
    final Lock statisticsLock = new ReentrantLock();
    /**Map from server ids to the cloud IDs uniquely assigned by the autoscaler to new datastores.*/
    public Map<Integer, Integer> dsIDToCloudID = new ConcurrentHashMap<>();

    /**If true, the load balancing routine will run*/
    public boolean runLoadBalancerDaemon = true;
    /**Load balancing thread*/
    private final LoadBalancerDaemon loadBalancerDaemon;
    /**Period of time between load balancing runs in milliseconds*/
    public static int loadBalancerSleepDurationMillis = 60000;
    public final Semaphore loadBalancerSemaphore = new Semaphore(0);
    /**Query per shards on the cached actors*/
    public Map<Integer, Integer> cachedQPSLoad = null;
    public Map<String, List<Integer>> tablesToAllocatedShards = new HashMap<>();


    // Lock protects shardToPrimaryDataStoreMap, shardToReplicaDataStoreMap, and shardToReplicaRatioMap.
    // Each operation modifying these maps follows this process:  Lock, change the local copies, unlock, perform
    // the operation, lock, retrieve the local copies, set the ZK mirrors to the local copies, unlock.
    public final Lock shardMapLock = new ReentrantLock();

    public Coordinator(CoordinatorCloud cCloud, LoadBalancer loadBalancer, AutoScaler autoScaler, String zkHost, int zkPort, String coordinatorHost, int coordinatorPort) {
        this.coordinatorHost = coordinatorHost;
        this.coordinatorPort = coordinatorPort;
        this.loadBalancer = loadBalancer;
        this.autoscaler = autoScaler;
        zkCurator = new CoordinatorCurator(zkHost, zkPort);
        this.server = ServerBuilder.forPort(coordinatorPort)
                .addService(new ServiceDataStoreCoordinator(this))
                .addService(new ServiceBrokerCoordinator(this))
                .build();
        this.loadBalancerDaemon = new LoadBalancerDaemon();
        this.cCloud = cCloud;
    }

    /** Start serving requests. */
    public boolean startServing() {
        try {
            server.start();
            zkCurator.registerCoordinator(coordinatorHost, coordinatorPort);
        } catch (Exception e) {
            logger.warn("Coordinator startup failed: {}", e.getMessage());
            this.stopServing();
            return false;
        }
        logger.info("Coordinator server started, listening on " + coordinatorPort);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                Coordinator.this.stopServing();
            }
        });
        loadBalancerDaemon.start();
        return true;
    }

    /** Stop serving requests and shutdown resources. */
    public void stopServing() {
        if (cCloud != null) {
            cCloud.shutdown();
        }
        if (server != null) {
            server.shutdown();
        }
        for(ManagedChannel channel: dataStoreChannelsMap.values()) {
            channel.shutdown();
        }
        runLoadBalancerDaemon = false;
        try {
            loadBalancerDaemon.interrupt();
            loadBalancerDaemon.join();
        } catch (InterruptedException ignored) {}
        zkCurator.close();
    }

    /**ONLY USED IN TESTING.
     * Adds a replica of the given shard on the given datastore having id replicaID*/
    public void addReplica(int shardNum, int replicaID) {
        shardMapLock.lock();
        if (dataStoresMap.get(replicaID).status.get() == DataStoreDescription.DEAD) {
            shardMapLock.unlock();
            logger.info("AddReplica failed for shard {} DEAD DataStore {}", shardNum, replicaID);
            return;
        }
        int primaryDataStore = consistentHash.getRandomBucket(shardNum);
        List<Integer> replicaDataStores = shardToReplicaDataStoreMap.putIfAbsent(shardNum, new ArrayList<>());
        if (replicaDataStores == null) {
            replicaDataStores = shardToReplicaDataStoreMap.get(shardNum);
        }
        if (replicaDataStores.contains(replicaID)) {
            // Replica already exists.
            shardMapLock.unlock();
        } else if (replicaID != primaryDataStore) {
            replicaDataStores.add(replicaID);
            shardMapLock.unlock();
            // Create new replica.
            CoordinatorDataStoreGrpc.CoordinatorDataStoreBlockingStub stub = dataStoreStubsMap.get(replicaID);
            LoadShardReplicaMessage m = LoadShardReplicaMessage.newBuilder().
                    setShard(shardNum).
                    setIsReplacementPrimary(false).
                    build();
            try {
                LoadShardReplicaResponse r = stub.loadShardReplica(m);
                if (r.getReturnCode() != 0) {  // TODO: Might lead to unavailable shard.
                    logger.warn("Shard {} load failed on DataStore {}", shardNum, replicaID);
                    shardMapLock.lock();
                    replicaDataStores = shardToReplicaDataStoreMap.get(shardNum);
                    if (replicaDataStores.contains(replicaID)) {
                        replicaDataStores.remove(Integer.valueOf(replicaID));
                    }
                    shardMapLock.unlock();
                    return;
                }
            } catch (StatusRuntimeException e) {
                logger.warn("Shard {} load RPC failed on DataStore {}", shardNum, replicaID);
            }
        } else {
            shardMapLock.unlock();
            logger.info("AddReplica failed for shard {} PRIMARY DataStore {}", shardNum, replicaID);
        }
    }
    public void removeShard(int shardNum, int targetID) {
        shardMapLock.lock();
        int primaryDataStore = consistentHash.getRandomBucket(shardNum);
        List<Integer> replicaDataStores = shardToReplicaDataStoreMap.get(shardNum);
        if (primaryDataStore == targetID) {
            logger.error("Cannot remove shard {} from primary {}", shardNum, targetID);
        } else {
            if (!replicaDataStores.contains(targetID)) {
                logger.info("RemoveShard Failed DataStore {} does not have shard {}", targetID, shardNum);
                shardMapLock.unlock();
                return;
            }
            int targetIndex = replicaDataStores.indexOf(targetID);
            replicaDataStores.remove(Integer.valueOf(targetID));
            shardMapLock.unlock();
            CoordinatorDataStoreGrpc.CoordinatorDataStoreBlockingStub stub = dataStoreStubsMap.get(targetID);
            RemoveShardMessage m = RemoveShardMessage.newBuilder().setShard(shardNum).build();
            try {
                RemoveShardResponse r = stub.removeShard(m);
            } catch (StatusRuntimeException e) {
                logger.warn("Shard {} remove RPC failed on DataStore {}", shardNum, targetID);
            }
            shardMapLock.lock();
            primaryDataStore = consistentHash.getRandomBucket(shardNum);
            NotifyReplicaRemovedMessage nm = NotifyReplicaRemovedMessage.newBuilder().setDsID(targetID).setShard(shardNum).build();
            try {
                NotifyReplicaRemovedResponse r = dataStoreStubsMap.get(primaryDataStore).notifyReplicaRemoved(nm);
            } catch (StatusRuntimeException e) {
                logger.warn("Shard {} removal notification RPC for DataStore {} failed on primary DataStore {}", shardNum, targetID, primaryDataStore);
            }
            shardMapLock.unlock();
        }
    }

    /** Construct three load maps:
     * 1.  Shard number to shard QPS.
     * 2.  Shard number to shard memory usage.
     * 3.  DSID to datastore CPU usage.
     * **/
    public Triplet<Map<Integer, Integer>, Map<Integer, Integer>, Map<Integer, Double>> collectLoad() {
        Map<Integer, Integer> qpsMap = new ConcurrentHashMap<>();
        Map<Integer, Integer> memoryUsagesMap = new ConcurrentHashMap<>();
        Map<Integer, Integer> shardCountMap = new ConcurrentHashMap<>();
        Map<Integer, Double> serverCpuUsageMap = new ConcurrentHashMap<>();
        for(DataStoreDescription dsDesc: dataStoresMap.values()) {
            if (dsDesc.status.get() == DataStoreDescription.ALIVE) {
                CoordinatorDataStoreGrpc.CoordinatorDataStoreBlockingStub stub = dataStoreStubsMap.get(dsDesc.dsID);
                ShardUsageMessage m = ShardUsageMessage.newBuilder().build();
                try {
                    ShardUsageResponse response = stub.shardUsage(m);
                    Map<Integer, Integer> dataStoreQPSMap = response.getShardQPSMap();
                    Map<Integer, Integer> dataStoreUsageMap = response.getShardMemoryUsageMap();
                    dataStoreQPSMap.forEach((key, value) -> qpsMap.merge(key, value, Integer::sum));
                    dataStoreUsageMap.forEach((key, value) -> memoryUsagesMap.merge(key, value, Integer::sum));
                    dataStoreUsageMap.forEach((key, value) -> shardCountMap.merge(key, 1, (v1, v2) -> v1 + 1));
                    serverCpuUsageMap.put(response.getDsID(), response.getServerCPUUsage());
                } catch (StatusRuntimeException e) {
                    logger.info("DS{} load collection failed: {}", dsDesc.dsID, e.getMessage());
                }
            }
        }
        memoryUsagesMap.replaceAll((k, v) -> shardCountMap.get(k) > 0 ? v / shardCountMap.get(k) : v);
        return new Triplet<>(qpsMap, memoryUsagesMap, serverCpuUsageMap);
    }

    public void addDataStore() {
        logger.info("Adding DataStore");
        if (!cCloud.addDataStore()) {
            logger.error("DataStore addition failed");
        }
    }
    public void removeDataStore() {
        List<Integer> removeableDSIDs = dataStoresMap.keySet().stream()
                .filter(i -> dataStoresMap.get(i).status.get() == DataStoreDescription.ALIVE)
                .filter(i -> dsIDToCloudID.containsKey(i))
                .collect(Collectors.toList());
        long numActiveDataStores = dataStoresMap.keySet().stream()
                .filter(i -> dataStoresMap.get(i).status.get() == DataStoreDescription.ALIVE)
                .count();
        if (numActiveDataStores > 1 && !removeableDSIDs.isEmpty()) {
            Integer removedDSID = removeableDSIDs.get(ThreadLocalRandom.current().nextInt(removeableDSIDs.size()));
            Integer cloudID = dsIDToCloudID.get(removedDSID);
            logger.info("Removing DataStore DS{} CloudID {}", removedDSID, cloudID);
            consistentHash.removeBucket(removedDSID);
            Set<Integer> otherDatastores = dataStoresMap.values().stream()
                    .filter(i -> i.status.get() == DataStoreDescription.ALIVE && i.dsID != removedDSID)
                    .map(i -> i .dsID).collect(Collectors.toSet());
            assignShards();
            dataStoresMap.get(removedDSID).status.set(DataStoreDescription.DEAD);
            cCloud.removeDataStore(cloudID);
        }
    }

    public void rebalanceConsistentHash(Map<Integer, Integer> qpsLoad) {
        Set<Integer> shards = qpsLoad.keySet();
        Set<Integer> servers = consistentHash.buckets;
        Map<Integer, Integer> currentLocations = shards.stream().collect(Collectors.toMap(i -> i, consistentHash::getRandomBucket));
        Map<Integer, Integer> updatedLocations = loadBalancer.balanceLoad(shards, servers, qpsLoad, currentLocations);
        consistentHash.reassignmentMap.clear();
        for (int shardNum: updatedLocations.keySet()) {
            int newServerNum = updatedLocations.get(shardNum);
            if (newServerNum != consistentHash.getRandomBucket(shardNum)) {
                consistentHash.reassignmentMap.put(shardNum, new ArrayList<>(List.of(newServerNum)));
            }
        }
    }
    public void assignShards(){
        Set<Integer> servers = consistentHash.buckets;
        ByteString newConsistentHash = Utilities.objectToByteString(consistentHash);
        Set<Integer> shardsList = new HashSet<>();
        for(TableInfo t: tableInfoMap.values()) {
            int tableStart = t.id * Broker.SHARDS_PER_TABLE;
            for (int i = tableStart; i < tableStart + t.numShards; i++) {
                shardsList.add(i);
            }
        }
        // Add assigned shards.
        CountDownLatch gainedLatch = new CountDownLatch(servers.size());
        for (int dsID: servers) {
            StreamObserver<ExecuteReshuffleResponse> gainedObserver = new StreamObserver<>() {
                @Override
                public void onNext(ExecuteReshuffleResponse executeReshuffleResponse) {
                }

                @Override
                public void onError(Throwable throwable) {
                    logger.warn("DS{} Reassignment Failure", dsID);
                    gainedLatch.countDown();
                }

                @Override
                public void onCompleted() {
                    gainedLatch.countDown();
                }
            };
            ExecuteReshuffleMessage reshuffleMessage = ExecuteReshuffleMessage.newBuilder()
                    .setNewConsistentHash(newConsistentHash)
                    .addAllShardList(shardsList).setDsID(dsID).build();
            CoordinatorDataStoreGrpc.newStub(dataStoreChannelsMap.get(dsID)).executeReshuffleAdd(reshuffleMessage, gainedObserver);
        }
        try {
            gainedLatch.await();
        } catch (InterruptedException ignored) {}
        // Set the new consistent hash globally.
        zkCurator.setConsistentHashFunction(consistentHash);
        // Remove unassigned shards.
        CountDownLatch lostLatch = new CountDownLatch(servers.size());
        for (int dsID: servers) {
            StreamObserver<ExecuteReshuffleResponse> lostObserver = new StreamObserver<>() {
                @Override
                public void onNext(ExecuteReshuffleResponse executeReshuffleResponse) {
                }

                @Override
                public void onError(Throwable throwable) {
                    logger.warn("DS{} Reassignment Failure", dsID);
                    lostLatch.countDown();
                }

                @Override
                public void onCompleted() {
                    lostLatch.countDown();
                }
            };
            ExecuteReshuffleMessage reshuffleMessage = ExecuteReshuffleMessage.newBuilder()
                    .setNewConsistentHash(newConsistentHash)
                    .addAllShardList(shardsList).setDsID(dsID).build();
            CoordinatorDataStoreGrpc.newStub(dataStoreChannelsMap.get(dsID)).executeReshuffleRemove(reshuffleMessage, lostObserver);
        }
        try {
            lostLatch.await();
        } catch (InterruptedException ignored) {}
    }

    private class LoadBalancerDaemon extends Thread {
        @Override
        public void run() {
            while (runLoadBalancerDaemon) {
                try {
                    loadBalancerSemaphore.tryAcquire(loadBalancerSleepDurationMillis, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    return;
                }
                Triplet<Map<Integer, Integer>, Map<Integer, Integer>, Map<Integer, Double>> load = collectLoad();
                Map<Integer, Integer> qpsLoad = load.getValue0();
                Map<Integer, Integer> memoryUsages = load.getValue1();
                logger.info("Collected QPS Load: {}", qpsLoad);
                logger.info("Collected memory usages: {}", memoryUsages);
                if (!qpsLoad.isEmpty()) {
                    consistentHashLock.lock();
                    cachedQPSLoad = qpsLoad;
                    rebalanceConsistentHash(qpsLoad);
                    assignShards();
                    Map<Integer, Double> serverCpuUsage = load.getValue2();
                    logger.info("Collected DataStore CPU Usage: {}", serverCpuUsage);
                    if (cCloud != null) {
                        int action = autoscaler.autoscale(serverCpuUsage);
                        if (action == AutoScaler.ADD) {
                            addDataStore();
                        } else if (action == AutoScaler.REMOVE) {
                            removeDataStore();
                        }
                    }
                    consistentHashLock.unlock();
                }
            }
        }

    }


    public void registerQuery(ReadQuery query, String sourceTable){
        tableInfoMap.get(sourceTable).registerQuery(query);
    }
    public synchronized void addShardIDToTable(Integer shardID){
        String tableName = getTableIDFromShard(shardID);
        assert (tableName != null);
        tablesToAllocatedShards.computeIfAbsent(tableName, k->new ArrayList<>()).add(shardID);

    }
    private String getTableIDFromShard(Integer shardID){
        Integer tableID = shardID / Broker.SHARDS_PER_TABLE;
        for(Map.Entry<String, TableInfo> entry: tableInfoMap.entrySet()){
            if(entry.getValue().id.equals(tableID))
                return entry.getKey();
        }
        return null;
    }
    public List<Integer> getShardIDsForTable(String table){
        return tablesToAllocatedShards.get(table);
    }


    // Assumes consistentHashLock is held.
}
