package edu.stanford.futuredata.uniserve.api.querybuilders;

import edu.stanford.futuredata.uniserve.api.MalformedQueryException;
import edu.stanford.futuredata.uniserve.api.WriteQueryOperator;

import java.io.Serializable;

public class Write2PCQueryBuilder {
    private String queriedTable = null;
    private Serializable preCommitLambda = null;
    private Serializable commitLambda = null;
    private Serializable abortLambda = null;

    public Write2PCQueryBuilder setCommitLambda(Serializable commitLambda) {
        this.commitLambda = commitLambda;
        return this;
    }

    public Write2PCQueryBuilder setAbortLambda(Serializable abortLambda) {
        this.abortLambda = abortLambda;
        return this;
    }

    public Write2PCQueryBuilder setQueriedTable(String queriedTable) {
        this.queriedTable = queriedTable;
        return this;
    }

    public Write2PCQueryBuilder setPreCommitLambda(Serializable preCommitLambda) {
        this.preCommitLambda = preCommitLambda;
        return this;
    }

    public WriteQueryOperator build() throws MalformedQueryException {
        if(queriedTable == null || commitLambda == null || abortLambda == null || preCommitLambda == null)
            throw new MalformedQueryException("Malformed 2PC write");
        return new WriteQueryOperator(this);
    }


    public String getQueriedTable() {
        return queriedTable;
    }

    public Serializable getAbortLambda() {
        return abortLambda;
    }

    public Serializable getCommitLambda() {
        return commitLambda;
    }

    public Serializable getPreCommitLambda() {
        return preCommitLambda;
    }
}
