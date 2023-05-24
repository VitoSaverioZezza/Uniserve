package edu.stanford.futuredata.uniserve.tablemockinterface;

import edu.stanford.futuredata.uniserve.interfaces.Shard;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Path;
import java.util.*;

public class TableShard implements Shard {
    private static final Logger logger = LoggerFactory.getLogger(TableShard.class);

    public final List<Map<String, Integer>> table;
    private final Path shardPath;

    /**If the shardExist parameter is true, it retrieves a previously serialized shard stored at the given path, otherwise
     * it creates a new shard assigned to the given path.
     * @param shardPath the path assigned to the shard
     * @param shardExists if true, the shard being created is retrieved from the given shardPath, where it has been stored
     *                    by the shardToData method. If false, the shard is being created from scratch.
     * */
    public TableShard(Path shardPath, boolean shardExists) throws IOException, ClassNotFoundException {
        if (shardExists) {
            Path mapFile = Path.of(shardPath.toString(), "map.obj");
            FileInputStream f = new FileInputStream(mapFile.toFile());
            ObjectInputStream o = new ObjectInputStream(f);
            this.table = (ArrayList<Map<String, Integer>>) o.readObject();
            o.close();
            f.close();
        } else {
            this.table = new ArrayList<>();
        }
        this.shardPath = shardPath;
    }

    @Override
    public void destroy() {}

    /**@return the number of row entries in the shard*/
    @Override
    public int getMemoryUsage() {
        return table.size();
    }

    /**Serializes and stores the shard data in the path specified at shard creation time
     * @return an Optional object that stores the path the shard has been stored in. The optional object is empty
     * if the shard has not been stored correctly.
     * */
    @Override
    public Optional<Path> shardToData() {
        Path mapFile = Path.of(shardPath.toString(), "map.obj");
        try {
            FileOutputStream f = new FileOutputStream(mapFile.toFile());
            ObjectOutputStream o = new ObjectOutputStream(f);
            o.writeObject(table);
            o.close();
            f.close();
        } catch (IOException e) {
            return Optional.empty();
        }
        return Optional.of(shardPath);
    }

}
