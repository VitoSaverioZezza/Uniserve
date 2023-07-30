package edu.stanford.futuredata.uniserve.api;

import edu.stanford.futuredata.uniserve.broker.Broker;
import edu.stanford.futuredata.uniserve.interfaces.Row;
import edu.stanford.futuredata.uniserve.interfaces.Shard;
import edu.stanford.futuredata.uniserve.interfaces.WriteQueryPlan;
import edu.stanford.futuredata.uniserve.api.lambdamethods.CommitLambda;
import edu.stanford.futuredata.uniserve.api.lambdamethods.WriteShardLambda;
import edu.stanford.futuredata.uniserve.api.querybuilders.Write2PCQueryBuilder;

import java.io.Serializable;
import java.util.List;

public class WriteQueryOperator implements WriteQueryPlan {
    private String queriedTable;
    private Serializable preCommitLambda;
    private Serializable commitLambda;
    private Serializable abortLambda;


    public WriteQueryOperator(Write2PCQueryBuilder builder){
        queriedTable = builder.getQueriedTable();
        preCommitLambda = builder.getPreCommitLambda();
        commitLambda = builder.getCommitLambda();
        abortLambda = builder.getAbortLambda();
    }


    @Override
    public String getQueriedTable() {
        return queriedTable;
    }

    @Override
    public boolean preCommit(Shard shard, List rows) {
        List<Row> rows1 = (List<Row>) rows;
        return executePreCommit(shard, rows1);
    }

    @Override
    public void commit(Shard shard) {
        executeCommit(shard);
    }

    @Override
    public void abort(Shard shard) {
        executeAbort(shard);
    }

    private boolean executePreCommit(Shard shard, List<Row> rows){
        return ((WriteShardLambda & Serializable) preCommitLambda).write(shard, rows);
    }
    private void executeAbort(Shard shard){
        ((Serializable & CommitLambda) abortLambda).run(shard);
    }
    private void executeCommit(Shard shard){
        ((Serializable & CommitLambda) commitLambda).run(shard);
    }

    public void setAbortLambda(Serializable abortLambda) {
        this.abortLambda = abortLambda;
    }
    public void setCommitLambda(Serializable commitLambda) {
        this.commitLambda = commitLambda;
    }
    public void setPreCommitLambda(Serializable preCommitLambda) {
        this.preCommitLambda = preCommitLambda;
    }

    public boolean run(Broker broker, List<Row> data){
        return broker.writeQuery(this,data);
    }
}
