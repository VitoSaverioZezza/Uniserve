package edu.stanford.futuredata.uniserve.secondapi.querybuilders;


import java.io.Serializable;

public class WriteQueryBuilder {
    private String queriedTable = null;
    private Serializable writeLogic = null;
    private Serializable preCommitLogic = null;
    private Serializable abortLogic = null;
    private Serializable commitLogic = null;

    public WriteQueryBuilder setQueriedTable(String queriedTable){
        this.queriedTable = queriedTable;
        return this;
    }

    public Write2PCQueryBuilder setPreCommitLogic(Serializable preCommitLogic){
        Write2PCQueryBuilder builder = new Write2PCQueryBuilder();
        return builder.setPreCommitLambda(preCommitLogic)
                .setAbortLambda(abortLogic)
                .setCommitLambda(commitLogic)
                .setQueriedTable(queriedTable);
    }

    public Write2PCQueryBuilder setAbortLogic(Serializable abortLogic){
        Write2PCQueryBuilder builder = new Write2PCQueryBuilder();
        return builder.setPreCommitLambda(preCommitLogic)
                .setAbortLambda(abortLogic)
                .setCommitLambda(commitLogic)
                .setQueriedTable(queriedTable);
    }

    public Write2PCQueryBuilder setCommitLogic(Serializable commitLogic){
        Write2PCQueryBuilder builder = new Write2PCQueryBuilder();
        return builder.setPreCommitLambda(preCommitLogic)
                .setAbortLambda(abortLogic)
                .setCommitLambda(commitLogic)
                .setQueriedTable(queriedTable);
    }

    public SimpleWriteQueryBuilder setWriteLogic(Serializable writeLogic){
        SimpleWriteQueryBuilder builder = new SimpleWriteQueryBuilder();
        return builder.setWriteLambda(writeLogic).setQueriedTable(queriedTable);
    }
}
