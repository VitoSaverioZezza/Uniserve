package edu.stanford.futuredata.uniserve.relational;

import edu.stanford.futuredata.uniserve.interfaces.ReadQueryResults;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class RelReadQueryResults implements ReadQueryResults<RelShard, RelRow>, Serializable {
    List<RelRow> data = new ArrayList<>();
    List<String> fieldNames = new ArrayList<>();
    String name;

    public List<String> getFieldNames(){return this.fieldNames;}
    public void addData(List<RelRow> data ){this.data.addAll(data);}
    public void setFieldNames(List<String> fieldNames){this.fieldNames=fieldNames;}
    public int getIndex(String fieldName){
        int index = this.fieldNames.indexOf(fieldName);
        if(index == -1)
            throw new RuntimeException("Index not specified for attribute " + fieldName);
        return index;
    }
    public boolean isAttributeDefined(String attributeName){
        return this.fieldNames.contains(attributeName);
    }

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
