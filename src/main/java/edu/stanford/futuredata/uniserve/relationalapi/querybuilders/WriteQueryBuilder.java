package edu.stanford.futuredata.uniserve.relationalapi.querybuilders;

import edu.stanford.futuredata.uniserve.broker.Broker;
import edu.stanford.futuredata.uniserve.relational.RelRow;
import edu.stanford.futuredata.uniserve.relationalapi.ReadQuery;
import edu.stanford.futuredata.uniserve.relationalapi.SimpleWriteQuery;
import edu.stanford.futuredata.uniserve.relationalapi.WriteQuery;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

public class WriteQueryBuilder {
    private final Broker broker;
    private String tableName;
    private List<RelRow> data = new ArrayList<>();
    private boolean consistent = false;
    private SimpleWriteQuery simpleWriteQuery = null;

    private WriteQuery writeQuery = null;
    public WriteQueryBuilder(Broker broker){
        this.broker = broker;
    }

    public WriteQueryBuilder table(String tableName){
        if(Character.isDigit(tableName.charAt(0))){
            throw new RuntimeException("Invalid table name");
        }
        this.tableName = tableName;
        return this;
    }

    public WriteQueryBuilder data(RelRow... rows){
        data.addAll(Arrays.asList(rows));
        return this;
    }
    public WriteQueryBuilder data(Collection<RelRow> rows){
        data.addAll(rows);
        return this;
    }
    public WriteQueryBuilder data(ReadQuery subquery){
        data.addAll(subquery.run(broker).getData());
        return this;
    }


    public WriteQueryBuilder consistent(){
        consistent = true;
        return this;
    }
    public WriteQueryBuilder build(){
        if(tableName == null){
            throw new RuntimeException("Malformed write query: no queried table");
        }
        if(consistent){
            writeQuery = new WriteQuery(tableName, broker.getTableInfo(tableName).getKeyStructure());
        }else{
            simpleWriteQuery = new SimpleWriteQuery(tableName, broker.getTableInfo(tableName).getKeyStructure());
        }
        return this;
    }

    public boolean run(){
        if(data == null){
            throw new RuntimeException("No input is defined");
        }
        if(consistent && writeQuery != null){
            return broker.writeQuery(writeQuery, data);
        } else if (simpleWriteQuery != null) {
            return broker.simpleWriteQuery(simpleWriteQuery, data);
        } else{
            throw new RuntimeException("Malformed write query");
        }
    }
}