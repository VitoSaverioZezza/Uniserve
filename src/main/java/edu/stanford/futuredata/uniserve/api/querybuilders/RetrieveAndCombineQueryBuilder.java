package edu.stanford.futuredata.uniserve.api.querybuilders;

import edu.stanford.futuredata.uniserve.api.MalformedQueryException;
import edu.stanford.futuredata.uniserve.api.RetrieveAndCombineQuery;
import edu.stanford.futuredata.uniserve.api.lambdamethods.CombineLambdaRetAndComb;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**Builder class for a RetrieveAndCombineQuery*/
public class RetrieveAndCombineQueryBuilder {
    List<String> tableNames = new ArrayList<>();
    Map<String, List<Integer>> keysForQuery = null;
    Map<String, Serializable> retrieveLogics = new HashMap<>();
    Serializable combineLogic = null;

    public RetrieveAndCombineQueryBuilder setTableNames(List<String> tableNames) {
        this.tableNames = tableNames;
        return this;
    }
    public RetrieveAndCombineQueryBuilder setKeysForQuery(Map<String, List<Integer>> keysForQuery) {
        this.keysForQuery = keysForQuery;
        return this;
    }
    public RetrieveAndCombineQueryBuilder setRetrieveLogics(Map<String, Serializable> retrieveLogics) {
        this.retrieveLogics = retrieveLogics;
        return this;
    }
    public RetrieveAndCombineQueryBuilder setCombineLogic(Serializable combineLogic) {
        this.combineLogic = (Serializable & CombineLambdaRetAndComb) combineLogic;
        return this;
    }


    /**Builds a Retrieve and combine query, checking whether the query is well-formed before returning it.
     *@throws Exception if the query is not well-formed*/
    public RetrieveAndCombineQuery build() throws MalformedQueryException {
        if(tableNames == null || tableNames.size() == 0){
            throw new MalformedQueryException("Missing source tables");
        }
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
            if(!retrieveLogics.containsKey(tableName) || combineLogic == null){
                throw new MalformedQueryException("Malformed Retrieve and combine query, missing lambda function");
            }
        }
        return new RetrieveAndCombineQuery(this);
    }

    //GETTER METHODS, used by the RetrieveAndCombineQuery's constructor
    public List<String> getTableNames(){
        return this.tableNames;
    }
    public Map<String, List<Integer>> getKeysForQuery() {
        return keysForQuery;
    }
    public Map<String, Serializable> getRetrieveLogics() {
        return retrieveLogics;
    }
    public Serializable getCombineLogic() {
        return combineLogic;
    }
}
