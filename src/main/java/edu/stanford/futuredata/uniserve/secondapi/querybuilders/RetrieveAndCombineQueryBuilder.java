package edu.stanford.futuredata.uniserve.secondapi.querybuilders;

import edu.stanford.futuredata.uniserve.secondapi.RetrieveAndCombineQuery;
import edu.stanford.futuredata.uniserve.secondapi.lambdamethods.CombineLambdaRetAndComb;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**Builder class for a RetrieveAndCombineQuery*/
public class RetrieveAndCombineQueryBuilder {
    List<String> tableNames = new ArrayList<>();
    Map<String, List<Integer>> keysForQuery = null;
    Serializable combineLambda = null;
    Map<String, Serializable> retrieveLogic = new HashMap<>();

    public RetrieveAndCombineQueryBuilder setCombineLambda(Serializable combineLambda) {
        this.combineLambda = (Serializable & CombineLambdaRetAndComb) combineLambda;
        return this;
    }
    public RetrieveAndCombineQueryBuilder setKeysForQuery(Map<String, List<Integer>> keysForQuery) {
        this.keysForQuery = keysForQuery;
        return this;
    }
    public RetrieveAndCombineQueryBuilder setRetrieveLogic(Map<String, Serializable> retrieveLogic) {
        this.retrieveLogic = retrieveLogic;
        return this;
    }
    public RetrieveAndCombineQueryBuilder setTableNames(List<String> tableNames) {
        this.tableNames = tableNames;
        return this;
    }


    /**Builds a Retrieve and combine query, checking whether the query is well-formed before returning it.
     *@throws Exception if the query is not well-formed*/
    public RetrieveAndCombineQuery build() throws Exception{
        if(tableNames == null || tableNames.size() == 0){
            throw new Exception("Malformed retrieve and combine query, missing table names");
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
            if(!retrieveLogic.containsKey(tableName) || combineLambda == null){
                throw new Exception("Malformed Retrieve and combine query, missing lambda function");
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
    public Map<String, Serializable> getRetrieveLogic() {
        return retrieveLogic;
    }
    public Serializable getCombineLambda() {
        return combineLambda;
    }
}
