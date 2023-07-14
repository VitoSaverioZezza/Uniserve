package edu.stanford.futuredata.uniserve.secondapi;

import edu.stanford.futuredata.uniserve.broker.Broker;
import edu.stanford.futuredata.uniserve.interfaces.Row;
import edu.stanford.futuredata.uniserve.interfaces.Shard;
import edu.stanford.futuredata.uniserve.interfaces.SimpleWriteQueryPlan;
import edu.stanford.futuredata.uniserve.secondapi.lambdamethods.WriteShardLambda;
import edu.stanford.futuredata.uniserve.secondapi.querybuilders.SimpleWriteQueryBuilder;

import java.io.Serializable;
import java.util.List;

public class SimpleWriteOperator<S extends Shard> implements SimpleWriteQueryPlan<Row, S> {
    private final String queriedTable;
    private Serializable serWriteLambda;

    public SimpleWriteOperator(SimpleWriteQueryBuilder builder){
        this.queriedTable = builder.getQueriedTable();
        this.serWriteLambda = builder.getSerWriteLambda();

    }
    public SimpleWriteOperator(String queriedTable){
        this.queriedTable = queriedTable;
    }

    @Override
    public String getQueriedTable() {
        return queriedTable;
    }
    @Override
    public boolean write(S shard, List<Row> rows) {
        return executeWrite(shard, rows);
    }

    private boolean executeWrite(S shard, List<Row> data){
        return ((Serializable & WriteShardLambda ) serWriteLambda).write(shard, data);
    }
    public void setWriteLambda(Serializable writeLambda){
        this.serWriteLambda = writeLambda;
    }
    public boolean run(Broker broker, List<Row> data){
        return broker.simpleWriteQuery(this, data);
    }
}
