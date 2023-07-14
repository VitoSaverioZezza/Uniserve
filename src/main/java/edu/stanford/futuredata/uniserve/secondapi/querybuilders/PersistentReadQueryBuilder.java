package edu.stanford.futuredata.uniserve.secondapi.querybuilders;

import edu.stanford.futuredata.uniserve.secondapi.*;
import edu.stanford.futuredata.uniserve.secondapi.lambdamethods.*;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PersistentReadQueryBuilder {
    private String queryName;
    private String sinkTable = null;
    private List<String> sourceTables = null;
    private Serializable writeLambda = null;
    private Serializable commitLambda = null;
    private Serializable preCommitLambda = null;
    private Serializable abortLambda = null;
    private Map<String, Serializable> retrieveLambdasMap = null;
    private Serializable combineLambdaReC = null;
    private Map<String, Serializable> scatterLambdasMap = null;
    private Serializable gatherLambda = null;
    private Serializable combineLambdaShuffle = null;
    private Map<String, List<Integer>> keysForQuery = null;
    private boolean twoPhaseCommit = false;
    private boolean shuffle = false;

    private ShuffleOnReadQuery shuffleOnReadQuery = null;
    private RetrieveAndCombineQuery retrieveAndCombineQuery = null;
    private WriteQueryOperator twoPhaseCommitWriteQuery = null;
    private SimpleWriteOperator simpleWriteQuery = null;

    public PersistentReadQueryBuilder setQueryName(String name){
        this.queryName = name;
        return this;
    }
    public PersistentReadQueryBuilder setPreCommitLambda(Serializable preCommitLambda) {
        this.preCommitLambda = preCommitLambda;
        return this;
    }
    public PersistentReadQueryBuilder setCommitLambda(Serializable commitLambda) {
        this.commitLambda = commitLambda;
        return this;
    }
    public PersistentReadQueryBuilder setAbortLambda(Serializable abortLambda) {
        this.abortLambda = abortLambda;
        return this;
    }
    public PersistentReadQueryBuilder setGatherLambda(Serializable gatherLambda) {
        this.gatherLambda = gatherLambda;
        return this;
    }
    public PersistentReadQueryBuilder setCombineLambdaShuffle(Serializable combineLambdaShuffle) {
        this.combineLambdaShuffle = combineLambdaShuffle;
        return this;
    }
    public PersistentReadQueryBuilder setCombineLambdaReC(Serializable combineLambdaReC) {
        this.combineLambdaReC = combineLambdaReC;
        return this;
    }
    public PersistentReadQueryBuilder setKeysForQuery(Map<String, List<Integer>> keysForQuery) {
        this.keysForQuery = keysForQuery;
        return this;
    }
    public PersistentReadQueryBuilder setRetrieveLambdasMap(Map<String, Serializable> retrieveLambdasMap) {
        this.retrieveLambdasMap = retrieveLambdasMap;
        return this;
    }
    public PersistentReadQueryBuilder setScatterLambdasMap(Map<String, Serializable> scatterLambdasMap) {
        this.scatterLambdasMap = scatterLambdasMap;
        return this;
    }
    public PersistentReadQueryBuilder setSinkTable(String sinkTable) {
        this.sinkTable = sinkTable;
        return this;
    }
    public PersistentReadQueryBuilder setSourceTables(List<String> sourceTables) {
        this.sourceTables = sourceTables;
        return this;
    }
    public PersistentReadQueryBuilder setWriteLambda(Serializable writeLambda) {
        this.writeLambda = writeLambda;
        return this;
    }
    public PersistentReadQueryBuilder setTwoPhaseCommit(boolean twoPhaseCommit){
        this.twoPhaseCommit = twoPhaseCommit;
        return this;
    }
    public PersistentReadQueryBuilder setShuffle(boolean shuffle) {
        this.shuffle = shuffle;
        return this;
    }

    public PersistentReadQuery build() throws Exception{
        if(queryName == null){
            throw new Exception("Malformed persistent query, no name defined");
        }
        if(sourceTables == null || sourceTables.size() == 0){
            throw new Exception("Malformed read query, no source tables defined");
        }
        if(keysForQuery == null){
            keysForQuery = new HashMap<>();
            for(String source: sourceTables){
                keysForQuery.put(source, List.of(-1));
            }
        }
        if(sinkTable == null){
            throw new Exception("Malformed write query, no sink table defined");
        }

        PersistentReadQuery ret = new PersistentReadQuery();
        if(shuffle){
            if(scatterLambdasMap == null || gatherLambda == null || combineLambdaShuffle == null){
                if(scatterLambdasMap == null){
                    throw new Exception("Malformed shuffle read query, missing scatter lambda function");
                }else if(gatherLambda == null){
                    throw new Exception("Malformed shuffle read query, missing gather lambda function");
                }else{
                    throw new Exception("Malformed shuffle read query, missing combine lambda function");
                }
            }
            shuffleOnReadQuery = new ShuffleReadQueryBuilder()
                    .setTableNames(sourceTables)
                    .setKeysForQuery(keysForQuery)
                    .setScatterLogics(scatterLambdasMap)
                    .setGatherLogic(gatherLambda)
                    .setCombineLogic(combineLambdaShuffle)
                    .build();
            ret.setShuffleOnReadQuery(shuffleOnReadQuery);
        }else{
            if(retrieveLambdasMap == null || combineLambdaReC == null)
                throw new Exception("Malformed retrieve and combine read query, missing lambda function");
            retrieveAndCombineQuery = new RetrieveAndCombineQueryBuilder()
                    .setTableNames(sourceTables)
                    .setKeysForQuery(keysForQuery)
                    .setRetrieveLogic(retrieveLambdasMap)
                    .setCombineLambda(combineLambdaReC)
                    .build();
            ret.setRetrieveAndCombineQuery(retrieveAndCombineQuery);
        }

        if(twoPhaseCommit){
            if(commitLambda == null || preCommitLambda == null || abortLambda == null)
                throw new Exception("Malformed 2PC write query, missing lambda function");
            twoPhaseCommitWriteQuery = new Write2PCQueryBuilder()
                    .setQueriedTable(sinkTable)
                    .setCommitLambda(commitLambda)
                    .setPreCommitLambda(preCommitLambda)
                    .setAbortLambda(abortLambda)
                    .build();
            ret.setTwoPCWriteQuery(twoPhaseCommitWriteQuery);
        }else{
            if(writeLambda == null)
                throw new Exception("Malformed eventually consistent write query, missing lambda function");
            simpleWriteQuery = new SimpleWriteQueryBuilder()
                    .setQueriedTable(sinkTable)
                    .setSerWriteLambda(writeLambda)
                    .build();
            ret.setSimpleWriteQuery(simpleWriteQuery);
        }
        return ret;
    }
}
