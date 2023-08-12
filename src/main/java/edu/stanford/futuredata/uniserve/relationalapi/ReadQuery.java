package edu.stanford.futuredata.uniserve.relationalapi;

import edu.stanford.futuredata.uniserve.broker.Broker;
import edu.stanford.futuredata.uniserve.relational.RelReadQueryResults;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class ReadQuery implements Serializable {
    private List<String> resultSchema = new ArrayList<>();
    private IntermediateQuery intermediateQuery = null;

    public IntermediateQuery getIntermediateQuery() {
        return intermediateQuery;
    }

    public void setIntermediateQuery(IntermediateQuery intermediateQuery) {
        this.intermediateQuery = intermediateQuery;
    }

    public List<String> getResultSchema() {
        return resultSchema;
    }
    public void setResultSchema(List<String> resultSchema) {
        this.resultSchema = resultSchema;
    }

    public RelReadQueryResults run(Broker broker){
        return intermediateQuery.run(broker);
    }
}
