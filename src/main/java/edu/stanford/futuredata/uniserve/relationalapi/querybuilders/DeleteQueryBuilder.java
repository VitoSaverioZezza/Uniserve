package edu.stanford.futuredata.uniserve.relationalapi.querybuilders;

import edu.stanford.futuredata.uniserve.broker.Broker;
import edu.stanford.futuredata.uniserve.relational.RelRow;
import edu.stanford.futuredata.uniserve.relationalapi.ConsistentDeleteQuery;
import edu.stanford.futuredata.uniserve.relationalapi.DeleteQuery;
import edu.stanford.futuredata.uniserve.relationalapi.ReadQuery;
import edu.stanford.futuredata.uniserve.utilities.TableInfo;

import java.util.List;

public class DeleteQueryBuilder {
    private String table = "";
    private Broker broker;
    private List<RelRow> data;
    private Boolean[] keyStructure;
    private ReadQuery dataQuery = null;
    private boolean consistent = false;

    public DeleteQueryBuilder(Broker broker){
        this.broker = broker;
    }

    public DeleteQueryBuilder consistent(){
        consistent = true;
        return this;
    }
    public DeleteQueryBuilder from(String table) {
        this.table = table;
        return this;
    }
    public DeleteQueryBuilder data(List<RelRow> data){
        this.data = data;
        return this;
    }
    public DeleteQueryBuilder data(ReadQuery query){
        this.dataQuery = query;
        return this;
    }
    public DeleteQueryBuilder data(ReadQueryBuilder queryBuilder){
        return data(queryBuilder.build());
    }


    public DeleteQueryBuilder build(){
        if(table.isEmpty()){
            throw new RuntimeException("No table is specified");
        }
        TableInfo tableInfo = broker.getTableInfo(table);
        if(tableInfo == null){
            throw new RuntimeException("Impossible to find the specified table");
        }
        if(data.isEmpty()){
            throw new RuntimeException("No data to remove has been specified");
        }
        keyStructure = tableInfo.getKeyStructure();
        assert (keyStructure != null);
        return this;
    }

    public boolean run(){
        if(dataQuery != null){
            data = dataQuery.run(broker).getData();
        }
        if(consistent){
            return broker.writeQuery(new ConsistentDeleteQuery(table, keyStructure), data);
        }
        return broker.simpleWriteQuery(new DeleteQuery(table, keyStructure), data);
    }
}
