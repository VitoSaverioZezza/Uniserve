package edu.stanford.futuredata.uniserve.relationalapi;

import edu.stanford.futuredata.uniserve.broker.Broker;
import edu.stanford.futuredata.uniserve.relationalapi.querybuilders.DeleteQueryBuilder;
import edu.stanford.futuredata.uniserve.relationalapi.querybuilders.ReadQueryBuilder;
import edu.stanford.futuredata.uniserve.relationalapi.querybuilders.WriteQueryBuilder;

public class API {
    private final Broker broker;
    public API(Broker broker){
        this.broker = broker;
    }

    public API(String zkHost, int zkPort){
        broker = new Broker(zkHost, zkPort);
    }

    public CreateTableQuery createTable(String tableName){return new CreateTableQuery(tableName, broker);}
    public WriteQueryBuilder write(){return new WriteQueryBuilder(broker);}
    public ReadQueryBuilder read(){return new ReadQueryBuilder(broker);}
    public DeleteQueryBuilder delete(){return new DeleteQueryBuilder(broker);}
}
