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

    BrokerCurator(String zkHost, int zkPort) {
        String connectString = String.format("%s:%d", zkHost, zkPort);
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        this.cf = CuratorFrameworkFactory.newClient(connectString, retryPolicy);
        cf.start();
        lock = new InterProcessMutex(cf, "/brokerWriteLock");
    }
    void close() {
        cf.close();
    }

    DataStoreDescription getDSDescriptionFromDSID(int dsID) {
        try {
            String path = String.format("/dsDescription/%d", dsID);
            byte[] b = cf.getData().forPath(path);
            return new DataStoreDescription(new String(b));
        } catch (Exception e) {
            return null;
        }
    }
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

    void writeTxID(long txID){
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

    Long getTxID(){
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

    void writeLastCommittedVersion(long lcv){
        try {
            String path = "/lastCommittedVersion";
            byte[] data = ByteBuffer.allocate(4).putLong(lcv).array();
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

    Long getLastCommittedVersion(){
        try {
            String path = "/lastCommittedVersion";
            if (cf.checkExists().forPath(path) != null) {
                byte[] b = cf.getData().forPath(path);
                return ByteBuffer.wrap(b).getLong();
            } else {
                return null;
            }
        } catch (Exception e) {
            logger.error("ZK Failure {}", e.getMessage());
            assert(false);
            return null;
        }
    }

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


