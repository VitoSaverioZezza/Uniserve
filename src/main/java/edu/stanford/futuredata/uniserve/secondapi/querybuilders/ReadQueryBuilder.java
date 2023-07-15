package edu.stanford.futuredata.uniserve.secondapi.querybuilders;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**Builds a Read Query, automatically detecting where the query is a Shuffle read query or a retrieve and combine query
 * by virtue of the passed parameters. Once it is clear which query is to be formed the returned builder is one of the
 * suitable Class rather than a ReadQueryBuilder.
 *
 * */
public class ReadQueryBuilder {
    private List<String> tableNames = new ArrayList<>();
    private Map<String, List<Integer>> keysForQuery = new HashMap<>();
    private Map<String, Serializable> retrieveLogics = new HashMap<>();
    private Serializable combineLogic = null;
    private Map<String, Serializable> scatterLogics = new HashMap<>();
    private Serializable gatherLogic = null;

    boolean shuffle = false;

    public ReadQueryBuilder setKeysForQuery(Map<String, List<Integer>> keysForQuery) {
        this.keysForQuery = keysForQuery;
        return this;
    }
    public ReadQueryBuilder setTableNames(List<String> tableNames) {
        this.tableNames = tableNames;
        return this;
    }
    public ReadQueryBuilder setCombineLogic(Serializable combineLogic) {
        this.combineLogic = combineLogic;
        return this;
    }
    public ShuffleReadQueryBuilder setGatherLogic(Serializable gatherLogic) {
        ShuffleReadQueryBuilder builder = new ShuffleReadQueryBuilder();
        return builder.setKeysForQuery(keysForQuery)
                .setTableNames(tableNames)
                .setCombineLogic(combineLogic)
                .setGatherLogic(gatherLogic)
                .setScatterLogics(scatterLogics);
    }
    public RetrieveAndCombineQueryBuilder setRetrieveLogics(Map<String, Serializable> retrieveLogics) {
        RetrieveAndCombineQueryBuilder builder = new RetrieveAndCombineQueryBuilder();
        return builder.setKeysForQuery(keysForQuery)
                .setTableNames(tableNames)
                .setCombineLogic(combineLogic)
                .setRetrieveLogics(retrieveLogics);
    }
    public ShuffleReadQueryBuilder setScatterLogics(Map<String, Serializable> scatterLogics) {
        ShuffleReadQueryBuilder builder = new ShuffleReadQueryBuilder();
        return builder.setKeysForQuery(keysForQuery)
                .setTableNames(tableNames)
                .setCombineLogic(combineLogic)
                .setGatherLogic(gatherLogic)
                .setScatterLogics(scatterLogics);
    }
    public RetrieveAndCombineQueryBuilder build() throws Exception {throw new Exception("Malformed read query");}
}
