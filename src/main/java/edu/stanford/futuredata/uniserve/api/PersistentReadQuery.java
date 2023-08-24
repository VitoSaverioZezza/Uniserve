package edu.stanford.futuredata.uniserve.api;

import edu.stanford.futuredata.uniserve.broker.Broker;
import edu.stanford.futuredata.uniserve.interfaces.Row;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class PersistentReadQuery implements Serializable {
    private String queryName;
    private RetrieveAndCombineQuery retrieveAndCombineQuery = null;
    private ShuffleOnReadQuery shuffleOnReadQuery = null;
    private WriteQueryOperator twoPCWriteQuery = null;
    private SimpleWriteOperator simpleWriteQuery = null;

    private boolean isRegistered = false;

    public void setRetrieveAndCombineQuery(RetrieveAndCombineQuery retrieveAndCombineQuery) {
        this.retrieveAndCombineQuery = retrieveAndCombineQuery;
    }
    public void setShuffleOnReadQuery(ShuffleOnReadQuery shuffleOnReadQuery) {
        this.shuffleOnReadQuery = shuffleOnReadQuery;
    }
    public void setSimpleWriteQuery(SimpleWriteOperator simpleWriteQuery) {
        this.simpleWriteQuery = simpleWriteQuery;
    }
    public void setTwoPCWriteQuery(WriteQueryOperator twoPCWriteQuery) {
        this.twoPCWriteQuery = twoPCWriteQuery;
    }
    public void setQueryName(String queryName){this.queryName = queryName;}

    public String getQueryName(){return this.queryName;}
    public List<String> getSourceTables(){
        if(shuffleOnReadQuery != null){
            return shuffleOnReadQuery.getQueriedTables();
        }else if(retrieveAndCombineQuery != null ){
            return retrieveAndCombineQuery.getTableNames();
        }
        return new ArrayList<>();
    }
    public String getSinkTable(){
        if(twoPCWriteQuery == null){
            return simpleWriteQuery.getQueriedTable();
        }else {
            return twoPCWriteQuery.getQueriedTable();
        }
    }
    public void setRegistered(boolean registered) {
        isRegistered = registered;
    }

    public RetrieveAndCombineQuery getRetrieveAndCombineQuery() {
        return retrieveAndCombineQuery;
    }
    public ShuffleOnReadQuery getShuffleOnReadQuery() {
        return shuffleOnReadQuery;
    }
    public SimpleWriteOperator getSimpleWriteQuery() {
        return simpleWriteQuery;
    }
    public WriteQueryOperator getTwoPCWriteQuery() {
        return twoPCWriteQuery;
    }

    public List<Row> run(Broker broker){

        //TODO: Now, this only executes once, I have to modify the whole shenanigan in the write operation. Good luck to me.
        //check here if it is cyclic, otherwise all writes will do the same check adding unnecessary overhead
        //check every time the query is run is ok, a new query can be defined between two executions that makes this cyclic

        List<Row> readResult;
        if(retrieveAndCombineQuery != null){
            readResult = (List<Row>) broker.retrieveAndCombineReadQuery(retrieveAndCombineQuery);
        }else{
            readResult = (List<Row>) broker.shuffleReadQuery(shuffleOnReadQuery);
        }
        if(simpleWriteQuery != null){
            broker.simpleWriteQuery(simpleWriteQuery, readResult);
        }else{
            broker.writeQuery(twoPCWriteQuery, readResult);
        }
        return readResult;
    }
}
