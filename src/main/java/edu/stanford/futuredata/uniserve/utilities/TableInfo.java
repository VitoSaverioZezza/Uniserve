package edu.stanford.futuredata.uniserve.utilities;

import edu.stanford.futuredata.uniserve.relationalapi.ReadQuery;

import java.util.ArrayList;
import java.util.List;

public class TableInfo {
    public final String name;
    public final Integer id;
    public final Integer numShards;

    private ArrayList<Integer> tableShardsIDs = new ArrayList<>();

    private List<String> attributeNames = new ArrayList<>();
    private Boolean[] keyStructure;

    private ArrayList<ReadQuery> registeredQueries = new ArrayList<>();


    public TableInfo(String name, Integer id, Integer numShards) {
        this.name = name;
        this.id = id;
        this.numShards = numShards;
    }

    public ArrayList<Integer> getTableShardsIDs() {
        return tableShardsIDs;
    }
    public void setTableShardsIDs(ArrayList<Integer> tableShardsIDs) {
        this.tableShardsIDs = tableShardsIDs;
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

    public void setRegisteredQueries(ArrayList<ReadQuery> registeredQueries) {
        this.registeredQueries = registeredQueries;
    }

    public ArrayList<ReadQuery> getRegisteredQueries() {
        return registeredQueries;
    }
    public void registerQuery(ReadQuery query){
        this.registeredQueries.add(query);
    }
}

