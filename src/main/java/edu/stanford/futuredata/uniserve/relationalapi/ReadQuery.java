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

    public RelReadQueryResults run(Broker broker){
        if(simpleQuery != null){
            return simpleQuery.run(broker);
        }else{
            return aggregateQuery.run(broker);
        }
    }
    public ReadQuery setAggregateQuery(AggregateQuery aggregateQuery){
        this.aggregateQuery = aggregateQuery;
        return this;
    }


    public ReadQuery setSimpleQuery(ProdSelProjQuery simpleQuery) {
        this.simpleQuery = simpleQuery;
        return this;
    }
    public ProdSelProjQuery getSimpleQuery() {
        return simpleQuery;
    }

    public ReadQuery setResultSchema(List<String> resultSchema) {
        this.resultSchema = resultSchema;
        return this;
    }

    public List<String> getResultSchema() {
        return resultSchema;
    }
}
