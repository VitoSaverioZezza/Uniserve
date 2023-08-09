package edu.stanford.futuredata.uniserve.broker;

import com.google.protobuf.ByteString;
import edu.stanford.futuredata.uniserve.utilities.ConsistentHash;
import edu.stanford.futuredata.uniserve.utilities.DataStoreDescription;
import edu.stanford.futuredata.uniserve.utilities.Utilities;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Optional;

public class BrokerCurator {
    // TODO:  Figure out what to actually do when ZK fails.
    private final CuratorFramework cf;
    private static final Logger logger = LoggerFactory.getLogger(BrokerCurator.class);
    private InterProcessMutex lock;

    /**Returns curator's framework client for the broker's communication*/
    BrokerCurator(String zkHost, int zkPort) {
        String connectString = String.format("%s:%d", zkHost, zkPort);
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        this.cf = CuratorFrameworkFactory.newClient(connectString, retryPolicy);
        cf.start();
        lock = new InterProcessMutex(cf, "/brokerWriteLock");
    }
    /**Closes ZooKeeper's client*/
    void close() {
        cf.close();
    }
    /**Retrieves the DataStore's description of the datastore associated with the given identifier
     * @param dsID server's identifier
     * @return the datastore description of the server associated with the given identifier
     * */
    DataStoreDescription getDSDescriptionFromDSID(int dsID) {
        try {
            String path = String.format("/dsDescription/%d", dsID);
            byte[] b = cf.getData().forPath(path);
            return new DataStoreDescription(new String(b));
        } catch (Exception e) {
            return null;
        }
    }
    /**Writes the status of the given transaction in ZooKeeper to ensure error recovery in case of server failure*/
    void writeTransactionStatus(long txID, int status) {
        try {
            String path = String.format("/txStatus/%d", txID);
            byte[] data = ByteBuffer.allocate(4).putInt(status).array();
            if (cf.checkExists().forPath(path) != null) {
                cf.setData().forPath(path, data);
            } else {
                cf.create().forPath(path, data);
            }
        } catch (Exception e) {
            logger.error("ZK Failure {}", e.getMessage());
            assert(false);
        }
    }
    /**Writes the most recent transaction identifier to ZooKeeper*/
    private void writeTxID(long txID){
        try {
            String path = "/txID";
            byte[] data = ByteBuffer.allocate(8).putLong(txID).array();
            if (cf.checkExists().forPath(path) != null) {
                cf.setData().forPath(path, data);
            } else {
                cf.create().forPath(path, data);
            }
        } catch (Exception e) {
            logger.error("ZK Failure {}", e.getMessage());
            assert(false);
        }
    }

    /**Generates a new transaction identifer as the most recent transaction ID + 1*/
    protected Long getTxID(){
        try {
            String path = "/txID";
            if (cf.checkExists().forPath(path) != null) {
                byte[] b = cf.getData().forPath(path);
                long txID = ByteBuffer.wrap(b).getLong();
                long incrTxID = txID+1;
                writeTxID(incrTxID);
                return txID;
            } else {
                return null;
            }
        } catch (Exception e) {
            logger.error("ZK Failure {}", e.getMessage());
            assert(false);
            return null;
        }
    }
    /**Writes the last committed version to ZooKeeper*/
    protected void writeLastCommittedVersion(long lcv){
        try {
            String path = "/lastCommittedVersion";
            byte[] data = ByteBuffer.allocate(8).putLong(lcv).array();
            if (cf.checkExists().forPath(path) != null) {
                cf.setData().forPath(path, data);
            } else {
                cf.create().forPath(path, data);
            }
        } catch (Exception e) {
            logger.error("ZK Failure {}", e.getMessage());
            assert(false);
        }
    }
    /**Retrieves the last committed version from ZooKeeper*/
    protected Long getLastCommittedVersion(){
        try {
            String path = "/lastCommittedVersion";
            if (cf.checkExists().forPath(path) != null) {
                byte[] b = cf.getData().forPath(path);
                return ByteBuffer.wrap(b).getLong();
            } else {
                writeLastCommittedVersion(0L);
                return 0L;
            }
        } catch (Exception e) {
            logger.error("ZK Failure {}", e.getMessage());
            assert(false);
            return null;
        }
    }
    /**Retrieves the Coordinator's IP and Port*/
    Optional<Pair<String, Integer>> getMasterLocation() {
        try {
            String path = "/coordinator_host_port";
            if (cf.checkExists().forPath(path) != null) {
                byte[] b = cf.getData().forPath(path);
                String connectString = new String(b);
                return Optional.of(Utilities.parseConnectString(connectString));
            } else {
                return Optional.empty();
            }
        } catch (Exception e) {
            logger.error("ZK Failure {}", e.getMessage());
            assert(false);
            return null;
        }
    }
    /**Retrieves shard-server assignment*/
    ConsistentHash getConsistentHashFunction() {
        try {
            String path = "/consistentHash";
            byte[] b = cf.getData().forPath(path);
            return (ConsistentHash) Utilities.byteStringToObject(ByteString.copyFrom(b));
        } catch (Exception e) {
            logger.error("getConsistentHash Error: {}", e.getMessage());
            assert(false);
            return null;
        }
    }

    void acquireWriteLock() {
        try {
            lock.acquire();
        } catch (Exception e) {
            logger.error("WriteLock Acquire Error: {}", e.getMessage());
        }
    }
    void releaseWriteLock() {
        try {
            lock.release();
        } catch (Exception e) {
            logger.error("WriteLock Release Error: {}", e.getMessage());
        }
    }
}


