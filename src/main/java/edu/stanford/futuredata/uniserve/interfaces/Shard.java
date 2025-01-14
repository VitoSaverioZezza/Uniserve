package edu.stanford.futuredata.uniserve.interfaces;

import com.google.protobuf.ByteString;

import java.io.Serializable;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

/**Stateful data structure representing the unit of partitioning, access and replication.
 * Concurrency contract:
 *  -Concurrent writes on the same shard will never run simultaneously
 *  -The shardToData method will never run at the same time as a write operation
 *  -Reads may run at any time.
 *  */
public interface Shard extends Serializable {
    /**@return the amount of memory this shard uses in kilobytes.*/
    int getMemoryUsage();
    /**Destroy the shard data and related processes. After this method terminates, the shard is no longer usable.*/
    void destroy();
    /**@return an Optional object storing a path to a directory containing a serialization of the shard.*/
    Optional<Path> shardToData();

    //boolean insertRows(List data);
    //boolean committRows();

    default boolean writeIntermediateShard(ByteString retrievedResults){return true;}
    default boolean writeEphemeralShard(List<ByteString> scatterResults){return true;}
}