package edu.stanford.futuredata.uniserve.localcloud;

import edu.stanford.futuredata.uniserve.awscloud.AWSDataStoreCloud;
import edu.stanford.futuredata.uniserve.coordinator.CoordinatorCloud;
import edu.stanford.futuredata.uniserve.datastore.DataStore;
import edu.stanford.futuredata.uniserve.interfaces.Row;
import edu.stanford.futuredata.uniserve.interfaces.Shard;
import edu.stanford.futuredata.uniserve.interfaces.ShardFactory;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class LocalCoordinatorCloud<R extends Row, S extends Shard> implements CoordinatorCloud {

    //For test usage
    private final Map<Integer, DataStore<R, S>> dataStores = new HashMap<>();
    private final ShardFactory<S> factory;
    private final AtomicInteger cloudID = new AtomicInteger(0);
    private String baseDirectoryPath = "/var/tmp/KVUniserve%d";


    public LocalCoordinatorCloud(ShardFactory<S> factory) {
        this.factory = factory;
    }

    public LocalCoordinatorCloud(ShardFactory<S> factory, String baseDirectoryPath){
        this.factory = factory;
        this.baseDirectoryPath = baseDirectoryPath;
    }

    @Override
    public boolean addDataStore() {
        /*
        int cloudID = this.cloudID.getAndIncrement();
        DataStore<R, S> dataStore = new DataStore<>(new LocalDataStoreCloud(),
                factory, Path.of(baseDirectoryPath + cloudID), "127.0.0.1", 2181, "127.0.0.1", 8500 + cloudID, cloudID, false
        );
        dataStores.put(cloudID, dataStore);
        dataStore.runPingDaemon = false;
        return dataStore.startServing();
        */
        return true;
    }

    @Override
    public void removeDataStore(int cloudID) {
        dataStores.get(cloudID).shutDown();
        dataStores.remove(cloudID);
    }

    @Override
    public void shutdown() {
        dataStores.values().forEach(DataStore::shutDown);
    }
}
