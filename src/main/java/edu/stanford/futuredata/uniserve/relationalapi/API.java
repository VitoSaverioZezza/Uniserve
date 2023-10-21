package edu.stanford.futuredata.uniserve.relationalapi;

import edu.stanford.futuredata.uniserve.broker.Broker;
import edu.stanford.futuredata.uniserve.relationalapi.querybuilders.*;

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
    public RelReadQueryBuilder read(){return new RelReadQueryBuilder(broker);}
    public DeleteQueryBuilder delete(){return new DeleteQueryBuilder(broker);}
    public JoinQueryBuilder join(){return new JoinQueryBuilder(broker);}
    public UnionQueryBuilder union(){return new UnionQueryBuilder(broker);}
    public WriteBatch writeBatch(){return new WriteBatch(broker);}
}
