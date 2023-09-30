package edu.stanford.futuredata.uniserve.relational;

import edu.stanford.futuredata.uniserve.interfaces.ReadQueryResults;
import org.javatuples.Pair;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RelReadQueryResults implements ReadQueryResults<RelShard, RelRow>, Serializable {
    List<RelRow> data = new ArrayList<>();
    List<String> fieldNames = new ArrayList<>();
    String name;
    List<Pair<Integer, Integer>> intermediateLocations = new ArrayList<>();

    public void setIntermediateLocations(List<Pair<Integer, Integer>> intermediateLocations) {
        this.intermediateLocations = intermediateLocations;
    }
    public List<Pair<Integer, Integer>> getIntermediateLocations(){
        return intermediateLocations;
    }

    public List<String> getFieldNames(){return this.fieldNames;}
    public void addData(List<RelRow> data ){this.data.addAll(data);}
    public RelReadQueryResults setFieldNames(List<String> fieldNames){this.fieldNames=fieldNames;return this;}

    public void setName(String name) {
        this.name = name;
    }

    public List<RelRow> getData(){
        return data;
    }

    @Override
    public boolean writeShard(RelShard shard) {
        return shard.insertRows(data) && shard.committRows();
    }

    @Override
    public String getAlias() {
        return name;
    }
}
