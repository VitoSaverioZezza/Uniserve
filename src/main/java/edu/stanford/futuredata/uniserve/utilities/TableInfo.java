package edu.stanford.futuredata.uniserve.utilities;

import edu.stanford.futuredata.uniserve.api.PersistentReadQuery;

import java.util.ArrayList;
import java.util.List;

public class TableInfo {
    public final String name;
    public final Integer id;
    public final Integer numShards;
    private List<PersistentReadQuery> queriesTriggeredByAWriteOnThisTable = new ArrayList<>();
    private List<String> queryNames = new ArrayList<>();

    private List<String> attributeNames = new ArrayList<>();
    private Boolean[] keyStructure;

    public TableInfo(String name, Integer id, Integer numShards) {
        this.name = name;
        this.id = id;
        this.numShards = numShards;
    }

    public void setAttributeNames(List<String> attributeNames) {
        this.attributeNames = attributeNames;
    }

    public void setKeyStructure(Boolean[] keyStructure) {
        this.keyStructure = keyStructure;
    }

    public List<String> getAttributeNames() {
        return attributeNames;
    }

    public Boolean[] getKeyStructure() {
        return keyStructure;
    }

    public List<PersistentReadQuery> getQueriesTriggeredByAWriteOnThisTable(){
        return queriesTriggeredByAWriteOnThisTable;
    }

    public void addTriggeredQuery(PersistentReadQuery query){
        if(queryNames.contains(query.getQueryName())){
            query.setRegistered(true);
            return;
        }
        query.setRegistered(true);
        this.queriesTriggeredByAWriteOnThisTable.add(query);
        this.queryNames.add(query.getQueryName());
    }
}

