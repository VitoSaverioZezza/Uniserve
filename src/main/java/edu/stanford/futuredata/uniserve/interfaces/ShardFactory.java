package edu.stanford.futuredata.uniserve.interfaces;

import java.nio.file.Path;
import java.util.Optional;

/**Manages the creation and loading of shards*/
public interface ShardFactory<S extends Shard> {
    /**Creates a new shard storing the data stored at shardPath\shardNum.
     * @param shardPath The path of the directory storing the data that will be stored by the Shard being created
     * @param shardNum The identifier of the Shard to be created
     * @return an Optional object that may contain the shard whose serialized data was stored in shardPath\shardNum
     * */
    Optional<S> createNewShard(Path shardPath, int shardNum);

    /**Obtain a shard (having given identifier) that was previously serialized and stored in shardPath\shardNum
     * by the Shard.shardToData method.
     * @param shardPath The path of the directory storing the previously serialized shard to be retreived.
     * @param shardNum The identifier of the shard to be retrieved.
     * @return An Optional object that may contain the Shard that was previously serialized and stored in shardPath\shardNum
      */
    Optional<S> createShardFromDir(Path shardPath, int shardNum);
}
