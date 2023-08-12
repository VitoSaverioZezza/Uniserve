package edu.stanford.futuredata.uniserve.relationalapi;

import edu.stanford.futuredata.uniserve.broker.Broker;
import edu.stanford.futuredata.uniserve.relationalapi.querybuilders.ReadQueryBuilder;
import edu.stanford.futuredata.uniserve.relationalapi.querybuilders.WriteQueryBuilder;

public class API {
    Broker broker;
    public void start(String zkHost, int zkPort){
        if(broker != null){
            throw new RuntimeException("API Already started");
        }
        broker = new Broker(zkHost, zkPort);
    }

    public CreateTableQuery createTable(String tableName){return new CreateTableQuery(tableName, broker);}
    public WriteQueryBuilder write(){return new WriteQueryBuilder(broker);}
    public ReadQueryBuilder read(){return new ReadQueryBuilder(broker);}
}
