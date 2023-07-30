package edu.stanford.futuredata.uniserve.api.querybuilders;

import edu.stanford.futuredata.uniserve.api.MalformedQueryException;
import edu.stanford.futuredata.uniserve.api.SimpleWriteOperator;
import edu.stanford.futuredata.uniserve.api.lambdamethods.WriteShardLambda;

import java.io.Serializable;

public class SimpleWriteQueryBuilder {
    private String queriedTable;
    private WriteShardLambda writeLambda;

    public WriteShardLambda getSerWriteLambda() {
        return writeLambda;
    }

    public String getQueriedTable() {
        return queriedTable;
    }

    public SimpleWriteQueryBuilder setQueriedTable(String queriedTable) {
        this.queriedTable = queriedTable;
        return this;
    }
    public SimpleWriteQueryBuilder setWriteLambda(WriteShardLambda writeLambda) {
        this.writeLambda = writeLambda;
        return this;
    }
    public SimpleWriteOperator build() throws MalformedQueryException{
        if(queriedTable == null){
            throw new MalformedQueryException("Malformed write query");
        }
        return new SimpleWriteOperator(this);
    }
}
