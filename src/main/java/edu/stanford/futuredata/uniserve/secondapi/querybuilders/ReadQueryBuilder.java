package edu.stanford.futuredata.uniserve.secondapi.querybuilders;

import edu.stanford.futuredata.uniserve.secondapi.lambdamethods.*;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ReadQueryBuilder {
    private List<String> tableNames = new ArrayList<>();
    private Map<String, List<Integer>> keysForQuery = new HashMap<>();
    private Map<String, Serializable> retrieveLambdaMap = new HashMap<>();
    private Serializable combRetLambda = null;
    private Map<String, Serializable> scatterLambdaMap = new HashMap<>();
    private Serializable gatherLambda = null;
    private Serializable combineLambdaShuffle = null;

    boolean shuffle = false;

    public ReadQueryBuilder setKeysForQuery(Map<String, List<Integer>> keysForQuery) {
        this.keysForQuery = keysForQuery;
        return this;
    }

    public ReadQueryBuilder setTableNames(List<String> tableNames) {
        this.tableNames = tableNames;
        return this;
    }

    public ShuffleReadQueryBuilder setCombineLambdaShuffle(Serializable combineLambdaShuffle) {
        ShuffleReadQueryBuilder builder = new ShuffleReadQueryBuilder();
        return builder.setKeysForQuery(keysForQuery)
                .setTableNames(tableNames)
                .setCombineLogic(combineLambdaShuffle)
                .setGatherLogic(gatherLambda)
                .setScatterLogics(scatterLambdaMap);
    }

    public RetrieveAndCombineQueryBuilder setCombRetLambda(Serializable combRetLambda) {
        this.combRetLambda = combRetLambda;
        RetrieveAndCombineQueryBuilder builder = new RetrieveAndCombineQueryBuilder();
        return builder.setKeysForQuery(keysForQuery)
                .setTableNames(tableNames)
                .setCombineLambda(combRetLambda)
                .setRetrieveLogic(retrieveLambdaMap);
    }

    public ShuffleReadQueryBuilder setGatherLambda(Serializable gatherLambda) {
        ShuffleReadQueryBuilder builder = new ShuffleReadQueryBuilder();
        return builder.setKeysForQuery(keysForQuery)
                .setTableNames(tableNames)
                .setCombineLogic(combineLambdaShuffle)
                .setGatherLogic(gatherLambda)
                .setScatterLogics(scatterLambdaMap);
    }

    public RetrieveAndCombineQueryBuilder setRetrieveLambdaMap(Map<String, Serializable> retrieveLambdaMap) {
        RetrieveAndCombineQueryBuilder builder = new RetrieveAndCombineQueryBuilder();
        return builder.setKeysForQuery(keysForQuery)
                .setTableNames(tableNames)
                .setCombineLambda(combRetLambda)
                .setRetrieveLogic(retrieveLambdaMap);
    }

    public ShuffleReadQueryBuilder setScatterLambdaMap(Map<String, Serializable> scatterLambdaMap) {
        ShuffleReadQueryBuilder builder = new ShuffleReadQueryBuilder();
        return builder.setKeysForQuery(keysForQuery)
                .setTableNames(tableNames)
                .setCombineLogic(combineLambdaShuffle)
                .setGatherLogic(gatherLambda)
                .setScatterLogics(scatterLambdaMap);
    }
}
