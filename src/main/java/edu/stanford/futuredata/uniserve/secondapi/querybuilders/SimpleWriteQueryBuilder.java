package edu.stanford.futuredata.uniserve.secondapi.querybuilders;

import edu.stanford.futuredata.uniserve.secondapi.SimpleWriteOperator;
import edu.stanford.futuredata.uniserve.secondapi.lambdamethods.WriteShardLambda;

import java.io.Serializable;

public class SimpleWriteQueryBuilder {
    private String queriedTable;
    private Serializable writeLambda;

    public Serializable getSerWriteLambda() {
        return writeLambda;
    }

    public String getQueriedTable() {
        return queriedTable;
    }

    public SimpleWriteQueryBuilder setQueriedTable(String queriedTable) {
        this.queriedTable = queriedTable;
        return this;
    }
    public SimpleWriteQueryBuilder setWriteLambda(Serializable writeLambda) {
        this.writeLambda = writeLambda;
        return this;
    }
    public SimpleWriteOperator build() throws Exception{
        if(queriedTable == null){
            throw new Exception("Malformed write query");
        }
        return new SimpleWriteOperator(this);
    }
}
