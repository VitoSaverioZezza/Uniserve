package edu.stanford.futuredata.uniserve.relationalapi;

import edu.stanford.futuredata.uniserve.broker.Broker;
import edu.stanford.futuredata.uniserve.relational.RelReadQueryResults;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class ReadQuery implements Serializable {
    private ProdSelProjQuery simpleQuery = null;
    private List<String> resultSchema = new ArrayList<>();
    private AggregateQuery aggregateQuery = null;
    private SimpleAggregateQuery simpleAggregateQuery = null;
    private String resultTableID = null;
    private List<String> getSources = new ArrayList<>();


    public RelReadQueryResults run(Broker broker){
        if(simpleQuery != null){
            return simpleQuery.run(broker);
        }else if(simpleAggregateQuery != null){
            return simpleAggregateQuery.run(broker);
        }else{
            return aggregateQuery.run(broker);
        }
    }

    public List<String> getSources(){
        return List.of();
    }



    public ReadQuery setResultTableID(String resultTableID){
        this.resultTableID = resultTableID;
        return this;
    }

    public ReadQuery setSimpleAggregateQuery(SimpleAggregateQuery simpleAggregateQuery){
        this.simpleAggregateQuery = simpleAggregateQuery;
        return this;
    }

    public ReadQuery setAggregateQuery(AggregateQuery aggregateQuery){
        this.aggregateQuery = aggregateQuery;
        return this;
    }


    public ReadQuery setSimpleQuery(ProdSelProjQuery simpleQuery) {
        this.simpleQuery = simpleQuery;
        return this;
    }

    public String getResultTableID(){
        return resultTableID;
    }

    public ReadQuery setResultSchema(List<String> resultSchema) {
        this.resultSchema = resultSchema;
        return this;
    }

    public List<String> getResultSchema() {
        return resultSchema;
    }
}
