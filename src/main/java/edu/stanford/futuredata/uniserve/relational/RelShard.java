package edu.stanford.futuredata.uniserve.relational;

import edu.stanford.futuredata.uniserve.interfaces.Row;
import edu.stanford.futuredata.uniserve.interfaces.Shard;
import edu.stanford.futuredata.uniserve.utilities.Utilities;

import java.io.*;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class RelShard implements Shard {
    private final List<String> fieldNames = new ArrayList<>();
    private List<RelRow> data;
    private Path shardPath;

    private List<RelRow> uncommittedRows = new ArrayList<>();
    private List<RelRow> rowsToRemove = new ArrayList<>();



    public RelShard(Path shardPath, boolean shardExists) throws IOException, ClassNotFoundException {
        if (shardExists) {
            Path mapFile = Path.of(shardPath.toString(), "map.obj");
            FileInputStream f = new FileInputStream(mapFile.toFile());
            ObjectInputStream o = new ObjectInputStream(f);
            this.data = (List<RelRow>) o.readObject();
            o.close();
            f.close();
        } else {
            this.data = new ArrayList<>();
        }
        this.shardPath = shardPath;
    }

    @Override
    public int getMemoryUsage() {
        return Utilities.objectToByteString(this).size();
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
    public List<RelRow> getData(){
        return data;
    }
    public boolean insertRows(List<RelRow> rows){
        uncommittedRows.addAll(rows);
        return true;
    }
    public boolean committRows(){
        for(RelRow row: rowsToRemove){
            data.remove(row);
        }
        data.addAll(uncommittedRows);
        rowsToRemove.clear();
        uncommittedRows.clear();
        return true;
    }
    public boolean abortTransactions(){
        uncommittedRows.clear();
        rowsToRemove.clear();
        return true;
    }
    public void clear(){
        this.rowsToRemove = data;
    }
    public void removeRows(List<RelRow> rowsToRemove){
        this.rowsToRemove = rowsToRemove;
    }
}
