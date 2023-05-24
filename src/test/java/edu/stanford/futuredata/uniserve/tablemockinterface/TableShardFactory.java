package edu.stanford.futuredata.uniserve.tablemockinterface;

import edu.stanford.futuredata.uniserve.interfaces.ShardFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;

public class TableShardFactory implements ShardFactory<TableShard> {

    private static final Logger logger = LoggerFactory.getLogger(TableShardFactory.class);

    /**Creates a new shard from scratch associated with the given path
     * @param shardPath the path the shard is assigned to
     * @param shardNum shard identifier. Has no role in the creation routine
     * @return An Optional object containing either the newly created shard or nothing. In the latter case
     * the creation procedure has not terminated correctly*/
    @Override
    public Optional<TableShard> createNewShard(Path shardPath, int shardNum) {
        try {
            return Optional.of(new TableShard(shardPath, false));
        } catch (IOException | ClassNotFoundException e) {
            return Optional.empty();
        }
    }

    /**Retrieves a previously serialized shard from the given directory path.
     * @param shardNum the  identifier of the shard being retrieved. Has no role
     * @param shardPath the path from which the shard has to be retrieved
     * @return an Optional Object containing the shard. If the Optional object is empty, the shard has not
     * been retrieved correctly*/
    @Override
    public Optional<TableShard> createShardFromDir(Path shardPath, int shardNum) {
        try {
            return Optional.of(new TableShard(shardPath, true));
        } catch (IOException | ClassNotFoundException e) {
            logger.warn("Shard creation from directory failed: {}: {}", shardPath.toString(), e.getMessage());
            return Optional.empty();
        }
    }
}
