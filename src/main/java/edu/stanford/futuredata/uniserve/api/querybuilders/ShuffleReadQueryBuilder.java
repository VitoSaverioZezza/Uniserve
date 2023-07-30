package edu.stanford.futuredata.uniserve.api.querybuilders;

import edu.stanford.futuredata.uniserve.api.MalformedQueryException;
import edu.stanford.futuredata.uniserve.api.ShuffleOnReadQuery;
import edu.stanford.futuredata.uniserve.api.lambdamethods.CombineLambdaShuffle;
import edu.stanford.futuredata.uniserve.api.lambdamethods.GatherLambda;
import edu.stanford.futuredata.uniserve.api.lambdamethods.ScatterLambda;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ShuffleReadQueryBuilder {
    private List<String> tableNames = null;
    private Map<String, List<Integer>> keysForQuery = null;
    private Map<String, Serializable> scatterLogics = new HashMap<>();
    private Serializable gatherLogic = null;
    private Serializable combineLogic = null;

    public List<String> getTableNames() {
        return tableNames;
    }
    public Map<String, List<Integer>> getKeysForQuery() {
        return keysForQuery;
    }
    public Map<String, Serializable> getScatterLogics() {
        return scatterLogics;
    }
    public Serializable getCombineLogic() {
        return combineLogic;
    }
    public Serializable getGatherLogic() {
        return gatherLogic;
    }

    public ShuffleReadQueryBuilder setTableNames(List<String> tableNames) {
        this.tableNames = tableNames;
        return this;
    }
    public ShuffleReadQueryBuilder setCombineLogic(Serializable combineLogic) {
        this.combineLogic = (CombineLambdaShuffle & Serializable) combineLogic;
        return this;
    }
    public ShuffleReadQueryBuilder setGatherLogic(Serializable gatherLogic) {
        this.gatherLogic = (Serializable & GatherLambda) gatherLogic;
        return this;
    }
    public ShuffleReadQueryBuilder setScatterLogics(Map<String, Serializable> scatterLogics) {
        for(Map.Entry<String, Serializable> entry: scatterLogics.entrySet()){
            this.scatterLogics.put(entry.getKey(), (Serializable & ScatterLambda) entry.getValue() );
        }
        return this;
    }
    public ShuffleReadQueryBuilder setKeysForQuery(Map<String, List<Integer>> keysForQuery) {
        this.keysForQuery = keysForQuery;
        return this;
    }

    public ShuffleOnReadQuery build() throws MalformedQueryException {
        if(tableNames == null )
            throw new MalformedQueryException("Malformed Shuffle query. null table name");
        if(keysForQuery == null){
            keysForQuery = new HashMap<>();
            for(String tableName: tableNames){
                keysForQuery.put(tableName, List.of(-1));
            }
        }
        for(String tableName: tableNames){
            if(!keysForQuery.containsKey(tableName)){
                keysForQuery.put(tableName, List.of(-1));
            }
            if(!keysForQuery.containsKey(tableName) || !scatterLogics.containsKey(tableName)){
                throw new MalformedQueryException("Malformed Shuffle query, no scatterLogic defined for table " + tableName);
            }
        }
        if(gatherLogic == null || combineLogic == null){
            throw new MalformedQueryException("Malformed Shuffle query, no gather or combine logic defined");
        }
        return new ShuffleOnReadQuery(this);
    }
}
