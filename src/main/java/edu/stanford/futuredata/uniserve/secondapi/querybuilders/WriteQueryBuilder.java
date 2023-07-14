package edu.stanford.futuredata.uniserve.secondapi.querybuilders;

import edu.stanford.futuredata.uniserve.secondapi.lambdamethods.WriteShardLambda;

import java.io.Serializable;

public class WriteQueryBuilder {
    private String tableName = null;
    private Serializable writeLambda = null;
    private Serializable preCommitLambda = null;
    private Serializable abortLambda = null;
    private Serializable commitLambda = null;

    public WriteQueryBuilder setTableName(String tableName){
        this.tableName = tableName;
        return this;
    }

    public Write2PCQueryBuilder setPreCommitLambda(Serializable preCommitLambda){
        Write2PCQueryBuilder builder = new Write2PCQueryBuilder();
        return builder.setPreCommitLambda(preCommitLambda)
                .setAbortLambda(abortLambda)
                .setCommitLambda(commitLambda)
                .setQueriedTable(tableName);
    }

    public Write2PCQueryBuilder setAbortLambda(Serializable abortLambda){
        Write2PCQueryBuilder builder = new Write2PCQueryBuilder();
        return builder.setPreCommitLambda(preCommitLambda)
                .setAbortLambda(abortLambda)
                .setCommitLambda(commitLambda)
                .setQueriedTable(tableName);
    }

    public Write2PCQueryBuilder setCommitLambda(Serializable commitLambda){
        Write2PCQueryBuilder builder = new Write2PCQueryBuilder();
        return builder.setPreCommitLambda(preCommitLambda)
                .setAbortLambda(abortLambda)
                .setCommitLambda(commitLambda)
                .setQueriedTable(tableName);
    }

    public SimpleWriteQueryBuilder setWriteLambda(Serializable writeLambda){
        SimpleWriteQueryBuilder builder = new SimpleWriteQueryBuilder();
        return builder.setSerWriteLambda(writeLambda).setQueriedTable(tableName);
    }
}
