package edu.stanford.futuredata.uniserve.relationalmock;

import edu.stanford.futuredata.uniserve.interfaces.Shard;
import org.javatuples.Pair;

import java.io.*;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class RMShard implements Shard {
    private final Path shardPath;
    private final List<RMRowPerson> data;

    public RMShard(Path shardPath, boolean shardExists) throws IOException, ClassNotFoundException {
        if (shardExists) {
            Path mapFile = Path.of(shardPath.toString(), "map.obj");
            FileInputStream f = new FileInputStream(mapFile.toFile());
            ObjectInputStream o = new ObjectInputStream(f);
            this.data = (List<RMRowPerson>) o.readObject();
            o.close();
            f.close();
        } else {
            this.data = new ArrayList<>();
        }
        this.shardPath = shardPath;
    }
    public List<RMRowPerson> getPersons(){
        return data;
    }

    @Override
    public int getMemoryUsage() {
        return 1;
    }

    @Override
    public void destroy() {}

    @Override
    public Optional<Path> shardToData() {
        Path mapFile = Path.of(shardPath.toString(), "map.obj");
        try {
            FileOutputStream f = new FileOutputStream(mapFile.toFile());
            ObjectOutputStream o = new ObjectOutputStream(f);
            o.writeObject(data);
            o.close();
            f.close();
        } catch (IOException e) {
            return Optional.empty();
        }
        return Optional.of(shardPath);
    }
}
