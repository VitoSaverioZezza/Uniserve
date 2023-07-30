package edu.stanford.futuredata.uniserve.api.querybuilders;

import edu.stanford.futuredata.uniserve.api.*;
import edu.stanford.futuredata.uniserve.api.lambdamethods.WriteShardLambda;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PersistentReadQueryBuilder {
    private String queryName;
    private String sinkTable = null;
    private List<String> sourceTables = null;

    private WriteShardLambda writeLogic = null;
    private Serializable commitLogic = null;
    private Serializable preCommitLogic = null;
    private Serializable abortLogic = null;
    private Map<String, Serializable> retrieveLogics = null;
    private Serializable combineLogic = null;
    private Map<String, Serializable> scatterLogics = null;
    private Serializable gatherLogic = null;

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
    public PersistentReadQueryBuilder setPreCommitLogic(Serializable preCommitLogic) {
        this.preCommitLogic = preCommitLogic;
        return this;
    }
    public PersistentReadQueryBuilder setCommitLogic(Serializable commitLogic) {
        this.commitLogic = commitLogic;
        return this;
    }
    public PersistentReadQueryBuilder setAbortLogic(Serializable abortLogic) {
        this.abortLogic = abortLogic;
        return this;
    }
    public PersistentReadQueryBuilder setGatherLogic(Serializable gatherLogic) {
        this.gatherLogic = gatherLogic;
        return this;
    }
    public PersistentReadQueryBuilder setCombineLogic(Serializable combineLogic) {
        this.combineLogic = combineLogic;
        return this;
    }
    public PersistentReadQueryBuilder setKeysForQuery(Map<String, List<Integer>> keysForQuery) {
        this.keysForQuery = keysForQuery;
        return this;
    }
    public PersistentReadQueryBuilder setRetrieveLogics(Map<String, Serializable> retrieveLogics) {
        this.retrieveLogics = retrieveLogics;
        return this;
    }
    public PersistentReadQueryBuilder setScatterLogics(Map<String, Serializable> scatterLogics) {
        this.scatterLogics = scatterLogics;
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
    public PersistentReadQueryBuilder setWriteLogic(WriteShardLambda writeLogic) {
        this.writeLogic = writeLogic;
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

    /**Returns a PersistentReadQuery object that defines a single write query (either eventually consistent or two phase
     * commit style) and a single read operation extracting the results to be written. The read can be a shuffle read
     * query or a retrieve and combine read query.
     * If not explicitly specified, the method tries to build a retrieve-and-combine + eventually consistent combination.
     * The built query is NOT registered and needs to be explicitly registered via the Query Object method.
     * @return a well-formed PersistentReadQuery that can be registered and run
     * @throws MalformedQueryException if the given parameters are not suitable for a well-formed query */
    public PersistentReadQuery build() throws MalformedQueryException{
        if(queryName == null){
            throw new MalformedQueryException("Query name is not defined");
        }
        if(sourceTables == null || sourceTables.size() == 0){
            throw new MalformedQueryException("No source tables defined");
        }
        if(keysForQuery == null){
            keysForQuery = new HashMap<>();
            for(String source: sourceTables){
                keysForQuery.put(source, List.of(-1));
            }
        }
        if(sinkTable == null){
            throw new MalformedQueryException("Undefined sink table");
        }
        if(sourceTables.contains(sinkTable)){
            throw new MalformedQueryException("Sink table is a source table");
        }
        PersistentReadQuery ret = new PersistentReadQuery();
        if(shuffle){
            if(scatterLogics == null || gatherLogic == null || combineLogic == null){
                if(scatterLogics == null){
                    throw new MalformedQueryException("Shuffle read query missing scatter logics");
                }else if(gatherLogic == null){
                    throw new MalformedQueryException("Shuffle read query missing gather lambda function");
                }else{
                    throw new MalformedQueryException("Shuffle read query missing combine lambda function");
                }
            }
            for(String sourceTable: sourceTables){
                if(!scatterLogics.containsKey(sourceTable)){
                    throw new MalformedQueryException("No scatter logic defined for table " + sourceTable);
                }
            }
            shuffleOnReadQuery = new ShuffleReadQueryBuilder()
                    .setTableNames(sourceTables)
                    .setKeysForQuery(keysForQuery)
                    .setScatterLogics(scatterLogics)
                    .setGatherLogic(gatherLogic)
                    .setCombineLogic(combineLogic)
                    .build();
            ret.setShuffleOnReadQuery(shuffleOnReadQuery);
        }else{
            if(retrieveLogics == null)
                throw new MalformedQueryException("Malformed R&C query, missing retrieve logics");
            for(String tableName: sourceTables){
                if(!retrieveLogics.containsKey(tableName)){
                    throw new MalformedQueryException("Missing retrieve logic for table " + tableName);
                }
            }
            if(combineLogic == null)
                throw new MalformedQueryException("Malformed R&C query, missing combine logic");
            retrieveAndCombineQuery = new RetrieveAndCombineQueryBuilder()
                    .setTableNames(sourceTables)
                    .setKeysForQuery(keysForQuery)
                    .setRetrieveLogics(retrieveLogics)
                    .setCombineLogic(combineLogic)
                    .build();
            ret.setRetrieveAndCombineQuery(retrieveAndCombineQuery);
        }

        if(twoPhaseCommit){
            if(commitLogic == null)
                throw new MalformedQueryException("missing commit logic");
            if(preCommitLogic == null)
                throw new MalformedQueryException("Missing pre-commit logic");
            if(abortLogic == null)
                throw new MalformedQueryException("Missing abort logic");
            twoPhaseCommitWriteQuery = new Write2PCQueryBuilder()
                    .setQueriedTable(sinkTable)
                    .setCommitLambda(commitLogic)
                    .setPreCommitLambda(preCommitLogic)
                    .setAbortLambda(abortLogic)
                    .build();
            ret.setTwoPCWriteQuery(twoPhaseCommitWriteQuery);
        }else{
            if(writeLogic == null)
                throw new MalformedQueryException("missing write logic");
            simpleWriteQuery = new SimpleWriteQueryBuilder()
                    .setQueriedTable(sinkTable)
                    .setWriteLambda(writeLogic)
                    .build();
            ret.setSimpleWriteQuery(simpleWriteQuery);
        }
        return ret;
    }
}
