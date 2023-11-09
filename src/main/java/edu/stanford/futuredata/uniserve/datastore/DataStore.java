package edu.stanford.futuredata.uniserve.datastore;

import com.google.protobuf.ByteString;
import edu.stanford.futuredata.uniserve.*;
import edu.stanford.futuredata.uniserve.broker.Broker;
import edu.stanford.futuredata.uniserve.interfaces.Row;
import edu.stanford.futuredata.uniserve.interfaces.Shard;
import edu.stanford.futuredata.uniserve.interfaces.ShardFactory;
import edu.stanford.futuredata.uniserve.interfaces.WriteQueryPlan;
import edu.stanford.futuredata.uniserve.relationalapi.ReadQuery;
import edu.stanford.futuredata.uniserve.utilities.*;
import io.grpc.*;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class DataStore<R extends Row, S extends Shard> {
    private static final Logger logger = LoggerFactory.getLogger(DataStore.class);
    // Datastore metadata
    public int dsID = -1;
    private final int cloudID;
    private final String dsHost;
    private final int dsPort;
    final boolean readWriteAtomicity;
    public boolean serving = false;
    public boolean useReflink = false;

    // Map from shard number to shard data structure.
    public final Map<Integer, S> shardMap = new ConcurrentHashMap<>(); // Public for testing.
    // Map from shard number to old versions.  TODO:  Delete older versions.
    final Map<Integer, Map<Long, S>> multiVersionShardMap = new ConcurrentHashMap<>();
    // Map from shard number to shard version number.
    final Map<Integer, Integer> shardVersionMap = new ConcurrentHashMap<>();
    // Map from shard number to access lock.
    final Map<Integer, ShardLock> shardLockMap = new ConcurrentHashMap<>();
    // Map from shard number to maps from version number to write query and data.  TODO:  Clean older entries.
    final Map<Integer, Map<Integer, Pair<WriteQueryPlan<R, S>, List<R>>>> writeLog = new ConcurrentHashMap<>();
    // Map from primary shard number to list of replica descriptions for that shard.
    final Map<Integer, List<ReplicaDescription>> replicaDescriptionsMap = new ConcurrentHashMap<>();
    // Map from primary shard number to last timestamp known for the shard.
    //final Map<Integer, Long> shardTimestampMap = new ConcurrentHashMap<>();
    // Map from Unix second timestamp to number of read queries made during that timestamp, per shard.
    final Map<Integer, Map<Long, Integer>> QPSMap = new ConcurrentHashMap<>();
    // Map from table names to tableInfos.
    //private final Map<String, TableInfo> tableInfoMap = new ConcurrentHashMap<>();
    // Map from dsID to a ManagedChannel.
    private final Map<Integer, ManagedChannel> dsIDToChannelMap = new ConcurrentHashMap<>();

    private final Server server;
    final DataStoreCurator zkCurator;
    final ShardFactory<S> shardFactory;
    final DataStoreCloud dsCloud;
    final Path baseDirectory;
    private DataStoreCoordinatorGrpc.DataStoreCoordinatorBlockingStub coordinatorStub = null;
    private ManagedChannel coordinatorChannel = null;
    public final AtomicInteger ephemeralShardNum = new AtomicInteger(Integer.MAX_VALUE);

    public static int qpsReportTimeInterval = 60; // In seconds -- should be same as load balancer interval.

    public boolean runPingDaemon = true; // Public for testing
    private final PingDaemon pingDaemon;
    public final static int pingDaemonSleepDurationMillis = 100;
    private final static int pingDaemonRefreshInterval = 10;

    // Collect execution times of all read queries.
    public final Collection<Long> readQueryExecuteTimes = new ConcurrentLinkedQueue<>();
    public final Collection<Long> readQueryFullTimes = new ConcurrentLinkedQueue<>();

    public static final int COLLECT = 0;
    public static final int PREPARE = 1;
    public static final int COMMIT = 2;
    public static final int ABORT = 3;

    ConsistentHash consistentHash;

    public DataStore(DataStoreCloud dsCloud, ShardFactory<S> shardFactory, Path baseDirectory, String zkHost, int zkPort, String dsHost, int dsPort, int cloudID, boolean readWriteAtomicity) {
        this.dsHost = dsHost;
        this.dsPort = dsPort;
        this.dsCloud = dsCloud;
        this.readWriteAtomicity = readWriteAtomicity;
        this.shardFactory = shardFactory;
        this.baseDirectory = baseDirectory;
        this.server = ServerBuilder.forPort(dsPort)
                .addService(new ServiceBrokerDataStore<>(this))
                .addService(new ServiceCoordinatorDataStore<>(this))
                .addService(new ServiceDataStoreDataStore<>(this))
                .build();
        this.zkCurator = new DataStoreCurator(zkHost, zkPort);
        this.cloudID = cloudID;
        pingDaemon = new PingDaemon();
    }

    /** Start serving requests.
     * @return true if and only if the starting procedure is successful*/
    public boolean startServing() {
        // Start serving.
        assert(!serving);
        serving = true;
        try {
            server.start();
        } catch (IOException e) {
            logger.warn("DataStore startup failed: {}", e.getMessage());
            return false;
        }
        Runtime.getRuntime().addShutdownHook(new Thread(DataStore.this::shutDown));
        // Notify the coordinator of startup.
        Optional<Pair<String, Integer>> hostPort = zkCurator.getMasterLocation();
        int coordinatorPort;
        String coordinatorHost;
        if (hostPort.isPresent()) {
            coordinatorHost = hostPort.get().getValue0();
            coordinatorPort = hostPort.get().getValue1();
        } else {
            logger.warn("DataStore--Coordinator lookup failed");
            shutDown();
            return false;
        }
        logger.info("DataStore server started, listening on " + dsPort);
        coordinatorChannel =
                ManagedChannelBuilder.forAddress(coordinatorHost, coordinatorPort).usePlaintext().build();
        coordinatorStub = DataStoreCoordinatorGrpc.newBlockingStub(coordinatorChannel);
        RegisterDataStoreMessage m = RegisterDataStoreMessage.newBuilder()
                .setHost(dsHost).setPort(dsPort).setCloudID(cloudID).build();
        try {
            RegisterDataStoreResponse r = coordinatorStub.registerDataStore(m);
            assert r.getReturnCode() == 0;
            this.dsID = r.getDataStoreID();
            ephemeralShardNum.set(Integer.MAX_VALUE - Broker.SHARDS_PER_TABLE * dsID);
        } catch (StatusRuntimeException e) {
            logger.error("Coordinator Unreachable: {}", e.getStatus());
            shutDown();
            return false;
        }
        pingDaemon.start();
        Runtime.getRuntime().addShutdownHook(new Thread(DataStore.this::shutDown));
        return true;
    }
    /** Stop serving requests and shutdown resources. */
    public void shutDown() {
        if (!serving) {
            return;
        }
        serving = false;
        server.shutdown();
        runPingDaemon = false;
        try {
            pingDaemon.join();
        } catch (InterruptedException ignored) {}
        for (List<ReplicaDescription> replicaDescriptions: replicaDescriptionsMap.values()) {
            for (ReplicaDescription rd: replicaDescriptions) {
                rd.channel.shutdownNow();
            }
            for(ReplicaDescription rd: replicaDescriptions){
                try {
                    rd.channel.awaitTermination(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    logger.warn("DS{} Replica managed channel shutdown timed out {}", dsID, e.getMessage());
                }
            }
        }

        coordinatorChannel.shutdownNow();
        try{
            coordinatorChannel.awaitTermination(5, TimeUnit.SECONDS);
        }catch (InterruptedException e){
            logger.warn("DS{} coordinator managed channel shutdown timed out: {}", dsID, e.getMessage());
        }

        for (Map.Entry<Integer, S> entry: shardMap.entrySet()) {
            entry.getValue().destroy();
            shardMap.remove(entry.getKey());
        }

        for (ManagedChannel c: dsIDToChannelMap.values()) {
            c.shutdownNow();
        }
        try {
            for (ManagedChannel c : dsIDToChannelMap.values()) {
                c.awaitTermination(5, TimeUnit.SECONDS);
            }
        }catch (InterruptedException e){
            logger.warn("DS{} Channel shutdown timed out {}", dsID, e.getMessage());
        }
        zkCurator.close();
        int numQueries = readQueryExecuteTimes.size();
        OptionalDouble averageExecuteTime = readQueryExecuteTimes.stream().mapToLong(i -> i).average();
        OptionalDouble averageFullTime = readQueryFullTimes.stream().mapToLong(i -> i).average();
        if (averageExecuteTime.isPresent() && averageFullTime.isPresent()) {
            long p50Exec = readQueryExecuteTimes.stream().mapToLong(i -> i).sorted().toArray()[readQueryExecuteTimes.size() / 2];
            long p99Exec = readQueryExecuteTimes.stream().mapToLong(i -> i).sorted().toArray()[readQueryExecuteTimes.size() * 99 / 100];
            long p50Full = readQueryFullTimes.stream().mapToLong(i -> i).sorted().toArray()[readQueryFullTimes.size() / 2];
            long p99Full = readQueryFullTimes.stream().mapToLong(i -> i).sorted().toArray()[readQueryFullTimes.size() * 99 / 100];
            logger.info("Queries: {} p50 Exec: {}μs p99 Exec: {}μs p50 Full: {}μs p99 Full: {}μs", numQueries, p50Exec, p99Exec, p50Full, p99Full);
        }
        logger.info("DS{} shut down", dsID);
    }

    /**Creates a new shard identified by the given shard number*/
    Optional<S> createNewShard(int shardNum) {
        Path shardPath = Path.of(baseDirectory.toString(), Integer.toString(0), Integer.toString(shardNum));
        File shardPathFile = shardPath.toFile();
        if (!shardPathFile.exists()) {
            boolean mkdirs = shardPathFile.mkdirs();
            if (!mkdirs) {
                logger.error("DS{} Shard directory creation failed {}", dsID, shardNum);
                return Optional.empty();
            }
        }
        Optional<S> newShard = shardFactory.createNewShard(shardPath, shardNum);
        if(newShard.isEmpty()) {
            logger.error("Error in shard creation for shard ID: " + shardNum);
        }
        if(shardNum < ephemeralShardNum.get()) {
            RegisterNewShardMessage message = RegisterNewShardMessage.newBuilder().setShardID(shardNum).build();
            RegisterNewShardResponse response = coordinatorStub.registerNewShard(message);
            if (response.getStatus() == 0) {
                return newShard;
            } else {
                logger.error("Error in shard registration");
                return Optional.empty();
            }
        }else{
            return newShard;
        }
    }

    /** Creates all metadata for a shard not yet seen on this server, creating the shard if it does not yet exist **/
    boolean createShardMetadata(int shardNum) {
        if (shardLockMap.containsKey(shardNum)) {
            return true;
        }
        ShardLock shardLock = new ShardLock();
        shardLock.systemLockLock();
        if (shardLockMap.putIfAbsent(shardNum, shardLock) == null) {
            assert (!shardMap.containsKey(shardNum));
            ZKShardDescription zkShardDescription = zkCurator.getZKShardDescription(shardNum);
            if (zkShardDescription == null) {
                Optional<S> shard = createNewShard(shardNum);
                if (shard.isEmpty()) {
                    logger.error("DS{} Shard creation failed {}", dsID, shardNum);
                    shardLock.systemLockUnlock();
                    return false;
                }
                shardMap.put(shardNum, shard.get());
                shardVersionMap.put(shardNum, 0);
                if(Utilities.logger_flag)
                    logger.info("DS{} Created new primary shard {}", dsID, shardNum);
            } else {
                shardVersionMap.put(shardNum, zkShardDescription.versionNumber);
            }
            QPSMap.put(shardNum, new ConcurrentHashMap<>());
            writeLog.put(shardNum, new ConcurrentHashMap<>());
            replicaDescriptionsMap.put(shardNum, new ArrayList<>());
            multiVersionShardMap.put(shardNum, new ConcurrentHashMap<>());
        }
        shardLock.systemLockUnlock();
        return true;
    }

    /** Downloads the shard if not already cached, evicting if necessary **/
    boolean ensureShardCached(int shardNum) {
        if (!shardMap.containsKey(shardNum)) {
            ZKShardDescription z = zkCurator.getZKShardDescription(shardNum);
            if(z == null){
                return false;
            }
            Optional<S> shard = downloadShardFromCloud(shardNum, z.cloudName, z.versionNumber);
            if (shard.isEmpty()) {
                return false;
            }
            shardMap.putIfAbsent(shardNum, shard.get());
            shardVersionMap.put(shardNum, z.versionNumber);
        }
        return true;
    }

    /** Synchronously upload a shard to the cloud; assumes shard write lock is held **/
    // TODO:  Safely delete old versions.
    public void uploadShardToCloud(int shardNum) {
        long uploadStart = System.currentTimeMillis();
        Integer versionNumber = shardVersionMap.get(shardNum);
        Shard shard = shardMap.get(shardNum);
        // Load the shard's data into files.
        Optional<Path> shardDirectory = shard.shardToData();
        if (shardDirectory.isEmpty()) {
            logger.warn("DS{} Shard {} serialization failed", dsID, shardNum);
            return;
        }
        // Upload the shard's data.
        Optional<String> cloudName = dsCloud.uploadShardToCloud(shardDirectory.get(), Integer.toString(shardNum), versionNumber);
        if (cloudName.isEmpty()) {
            logger.warn("DS{} Shard {}-{} upload failed", dsID, shardNum, versionNumber);
            return;
        }
        // Notify the coordinator about the upload.
        zkCurator.setZKShardDescription(shardNum, cloudName.get(), versionNumber);
        if(Utilities.logger_flag)
            logger.info("DS{} Shard {}-{} upload succeeded. Time: {}ms", dsID, shardNum, versionNumber, System.currentTimeMillis() - uploadStart);
    }

    /** Synchronously download a shard from the cloud **/
    public Optional<S> downloadShardFromCloud(int shardNum, String cloudName, int versionNumber) {
        long downloadStart = System.currentTimeMillis();
        Path downloadDirectory = Path.of(baseDirectory.toString(), Integer.toString(versionNumber));
        File downloadDirFile = downloadDirectory.toFile();
        if (!downloadDirFile.exists()) {
            boolean mkdirs = downloadDirFile.mkdirs();
            if (!mkdirs && !downloadDirFile.exists()) {
                logger.warn("DS{} Shard {} version {} mkdirs failed: {}", dsID, shardNum, versionNumber, downloadDirFile.getAbsolutePath());
                return Optional.empty();
            }
        }
        int downloadReturnCode = dsCloud.downloadShardFromCloud(downloadDirectory, cloudName);
        if (downloadReturnCode != 0) {
            logger.warn("DS{} Shard {} download failed", dsID, shardNum);
            return Optional.empty();
        }
        Path targetDirectory = Path.of(downloadDirectory.toString(), cloudName);
        Optional<S> shard = shardFactory.createShardFromDir(targetDirectory, shardNum);
        if(Utilities.logger_flag)
            logger.info("DS{} Shard {}-{} download succeeded. Time: {}ms", dsID, shardNum, versionNumber, System.currentTimeMillis() - downloadStart);
        return shard;
    }

    /**Copies the shard from the cloud to the appropriate local directory*/
    public Optional<S> copyShardToDir(int shardNum, String cloudName, int versionNumber) {
        long copyStart = System.currentTimeMillis();
        Shard shard = shardMap.get(shardNum);
        // Load the shard's data into files.
        Optional<Path> shardDirectory = shard.shardToData();
        if (shardDirectory.isEmpty()) {
            logger.warn("DS{} Shard {} serialization failed", dsID, shardNum);
            return Optional.empty();
        }
        Path targetDirectory = Path.of(baseDirectory.toString(), Integer.toString(versionNumber), cloudName);
        File targetDirFile = targetDirectory.toFile();
        if (!targetDirFile.exists()) {
            boolean mkdirs = targetDirFile.mkdirs();
            if (!mkdirs && !targetDirFile.exists()) {
                logger.warn("DS{} Shard {} version {} mkdirs failed: {}", dsID, shardNum, versionNumber, targetDirFile.getAbsolutePath());
                return Optional.empty();
            }
        }
        try {
            ProcessBuilder copier = new ProcessBuilder("src/main/resources/copy_shard.sh", String.format("%s/*", shardDirectory.get()),
                    targetDirectory.toString());
            logger.info("Copy Command: {}", copier.command());
            Process writerProcess = copier.inheritIO().start();
            writerProcess.waitFor();
        } catch (InterruptedException | IOException e) {
            logger.warn("DS{} Shard {} version {} copy failed: {}", dsID, shardNum, versionNumber, targetDirFile.getAbsolutePath());
            return Optional.empty();
        }

        Optional<S> retShard = shardFactory.createShardFromDir(targetDirectory, shardNum);
        if(Utilities.logger_flag)
            logger.info("DS{} Shard {}-{} copy succeeded. Time: {}ms", dsID, shardNum, versionNumber, System.currentTimeMillis() - copyStart);
        return retShard;
    }

    /**Retrieves a gRPC channel for the server having the given identifier*/
    ManagedChannel getChannelForDSID(int dsID) {
        if (!dsIDToChannelMap.containsKey(dsID)) {
            DataStoreDescription d = zkCurator.getDSDescription(dsID);
            ManagedChannel channel = ManagedChannelBuilder.forAddress(d.host, d.port).usePlaintext().build();
            if (dsIDToChannelMap.putIfAbsent(dsID, channel) != null) {
                channel.shutdown();
            }
        }
        return dsIDToChannelMap.get(dsID);
    }

    /**Thread responsible for pinging other servers in order to detect failures*/
    private class PingDaemon extends Thread {
        @Override
        public void run() {
            List<DataStoreDescription> dsDescriptions = new ArrayList<>();
            int runCount = 0;
            while (runPingDaemon) {
                if (runCount % pingDaemonRefreshInterval == 0) {
                    dsDescriptions = zkCurator.getOtherDSDescriptions(dsID);
                }
                if (!dsDescriptions.isEmpty()) {
                    DataStoreDescription dsToPing =
                            dsDescriptions.get(ThreadLocalRandom.current().nextInt(dsDescriptions.size()));
                    int pingedDSID = dsToPing.dsID;
                    assert(pingedDSID != dsID);
                    ManagedChannel dsChannel =
                            ManagedChannelBuilder.forAddress(dsToPing.host, dsToPing.port).usePlaintext().build();
                    DataStoreDataStoreGrpc.DataStoreDataStoreBlockingStub stub = DataStoreDataStoreGrpc.newBlockingStub(dsChannel);
                    DataStorePingMessage pm = DataStorePingMessage.newBuilder().build();

                    try {
                        DataStorePingResponse alwaysEmpty = stub.dataStorePing(pm);
                    } catch (StatusRuntimeException e) {
                        if(!coordinatorChannel.isShutdown()) {
                            PotentialDSFailureMessage fm = PotentialDSFailureMessage.newBuilder().setDsID(pingedDSID).build();
                            PotentialDSFailureResponse alwaysEmpty = coordinatorStub.potentialDSFailure(fm);
                        }else{
                            logger.warn("Coordinator channel shut down, ping daemon returning");
                            return;
                        }
                    }
                    dsChannel.shutdown();
                    try{
                        dsChannel.awaitTermination(5, TimeUnit.SECONDS);
                    }catch(InterruptedException e){
                        logger.warn("Channel shutdown timed out: {}", e.getMessage());
                    }
                }
                runCount++;
                try {
                    Thread.sleep(pingDaemonSleepDurationMillis);
                } catch (InterruptedException e) {
                    return;
                }
            }
        }
    }


    public TableInfo getTableInfo(String tableName) {
        DTableInfoResponse r = coordinatorStub.tableInfo(DTableInfoMessage.newBuilder().setTableName(tableName).build());
        if(r.getReturnCode() != Broker.QUERY_SUCCESS){
            System.out.println("\t\tDS.getTableInfo ----- failed for tableID: " + tableName);
        }
        assert(r.getReturnCode() == Broker.QUERY_SUCCESS);
        TableInfo t = new TableInfo(tableName, r.getId(), r.getNumShards());
        Object[] attributeNamesArray = (Object[]) Utilities.byteStringToObject(r.getAttributeNames());
        List<String> attributeNames = new ArrayList<>();
        for (Object o:attributeNamesArray){
            attributeNames.add((String) o);
        }
        t.setAttributeNames(attributeNames);
        t.setKeyStructure((Boolean[]) Utilities.byteStringToObject(r.getKeyStructure()));
        t.setRegisteredQueries((ArrayList<ReadQuery>) Utilities.byteStringToObject(r.getTriggeredQueries()));
        t.setTableShardsIDs((ArrayList<Integer>) Utilities.byteStringToObject(r.getShardIDs()));
        if(t.getRegisteredQueries() == null){
            t.setRegisteredQueries(new ArrayList<>());
        }
        if(t.getTableShardsIDs() == null){
            t.setTableShardsIDs(new ArrayList<>());
        }
        return t;
    }

    private final Map<Pair<Long, Integer>, List<R>> resultsToStore = new ConcurrentHashMap<>();
    public ArrayList<R> getDataToStore(Pair<Long, Integer> dataIndex){
        List<R> ret = resultsToStore.get(dataIndex);
        if(ret == null){
            return new ArrayList<>();
        }
        return new ArrayList<>(ret);
    }
    public void storeResults(Pair<Long, Integer> dataIndex, List<R> data){
        resultsToStore.computeIfAbsent(dataIndex, k->new CopyOnWriteArrayList<>()).addAll(data);
    }
    public void removeCachedData(List<Pair<Long, Integer>> dataIndexes){
        for(Pair<Long, Integer> dataIndex:dataIndexes) {
            resultsToStore.remove(dataIndex);
        }
    }
}
