package edu.stanford.futuredata.uniserve.secondapi.querybuilders;

import edu.stanford.futuredata.uniserve.secondapi.SimpleWriteOperator;
import edu.stanford.futuredata.uniserve.secondapi.lambdamethods.WriteShardLambda;

import java.io.Serializable;

public class SimpleWriteQueryBuilder {
    private String queriedTable;
    private Serializable serWriteLambda;

    public Serializable getSerWriteLambda() {
        return serWriteLambda;
    }

    public String getQueriedTable() {
        return queriedTable;
    }

    public SimpleWriteQueryBuilder setQueriedTable(String queriedTable) {
        this.queriedTable = queriedTable;
        return this;
    }
    public SimpleWriteQueryBuilder setSerWriteLambda(Serializable serWriteLambda) {
        this.serWriteLambda = serWriteLambda;
        return this;
    }
    public SimpleWriteOperator build() throws Exception{
        if(queriedTable == null){
            throw new Exception("Malformed write query");
        }
        return new SimpleWriteOperator(this);
    }
}
