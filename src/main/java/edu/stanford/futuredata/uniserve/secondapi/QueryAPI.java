package edu.stanford.futuredata.uniserve.secondapi;

import edu.stanford.futuredata.uniserve.broker.Broker;
import edu.stanford.futuredata.uniserve.interfaces.Row;
import edu.stanford.futuredata.uniserve.interfaces.Shard;
import edu.stanford.futuredata.uniserve.secondapi.lambdamethods.ExtractFromShardKey;
import edu.stanford.futuredata.uniserve.secondapi.lambdamethods.ExtractFromShardLambda;
import edu.stanford.futuredata.uniserve.secondapi.lambdamethods.WriteShardLambda;

import java.util.Collection;
import java.util.List;

public class QueryAPI<S extends Shard> {
    private final Broker broker;

    public QueryAPI(String zkHost, Integer zkPort){
        this.broker = new Broker(zkHost, zkPort, null);
    }

    /**
     * Performs an equi-join operation between elements contained by two distinct tables in the shard.
     *
     * As an example:
     * Select A.att1, A.att2, ... B.att1, ...
     * From A, B
     * Where A.key == B.key
     *
     * @param tableOne the name of the first table involved in the query
     * @param tableTwo the name of the second table involved in the query
     * @param extractKeysFromTableOne a lambda function taking a Shard as parameter and returning a collection of objects
     *                                that specifies how the join attribute values should be extracted from the first table.
     *                                In the context of the provided example, it specifies how A.key should be extracted
     *                                from the actor
     * @param extractKeysFromTableTwo a lambda function taking a Shard as parameter and returning a collection of objects
     *                                that specifies how the join attribute values should be extracted from the second table.
     *                                In the context of the provided example, it specifies how B.key should be extracted
     *                                from the actor
     * @param selectTableOneLogic a lambda function taking a Shard as parameter and an Object representing a key of tableOne.
     *                            This lambda function has to specify how the attributes of tableOne that should be returned to the
     *                            caller should be retrieved from the actor passed as parameter.
     * @param selectTableTwoLogic a lambda function taking a Shard as parameter and an Object representing a key of tableOne.
     *                            This lambda function has to specify how the attributes of tableOne that should be returned to the
     *                            caller should be retrieved from the actor passed as parameter.
     * */
    public List<Object> joinOnRead(String tableOne, String tableTwo,
                                   ExtractFromShardLambda<S, Collection> extractKeysFromTableOne,
                                   ExtractFromShardLambda<S, Collection> extractKeysFromTableTwo,
                                   ExtractFromShardKey<S, Object> selectTableOneLogic,
                                   ExtractFromShardKey<S, Object> selectTableTwoLogic){

        JoinOperator<S> queryPlan = new JoinOperator<>(tableOne, tableTwo);
        queryPlan.setExtractKeysOne(extractKeysFromTableOne);
        queryPlan.setExtractKeysTwo(extractKeysFromTableTwo);
        queryPlan.setExtractDataOne(selectTableOneLogic);
        queryPlan.setExtractDataTwo(selectTableTwoLogic);
        return (List<Object>) broker.shuffleReadQuery(queryPlan);
    }

    /**
     * Performs a simple write query in an eventually consistent fashion.
     * @param tableName the name of the table the data will be written to
     * @param writeLogic a lambda function taking a Shard and a List of Row objects that specifies how the objects in the
     *                   list should be written in the actor
     * @param data the data to be written
     **/
    public boolean simpleWrite(String tableName, WriteShardLambda<S> writeLogic, List<Row> data){
        SimpleWriteOperator queryPlan = new SimpleWriteOperator(tableName);
        queryPlan.setWriteLambda(writeLogic);
        return broker.simpleWriteQuery(queryPlan, data);
    }

    /**
     * Performs a select query
     * @param tableName the name of the table involved in the query. It is necessary to provide a table name also
     *                  for a volatile query
     * @param extractLogic lambda function taking a shard as parameter and returning a Collection containing the items
     *                     that get returned to the caller.
     * @param onRead true if the query has to be executed on data stored by the underlying system, false if it has to be
     *               executed on data provided at request time
     * @param rawData data provided at request time on which the query executes if the onRead parameter is set to false
     * @param writeRawDataLogic lambda function specifying how the raw data has to be stored in a shard. The data gets
     *                          retrieved from the shard using the extract logic. This is needed if and only if the onRead
     *                          parameter is set to false. The lambda function takes a Shard as parameter.
     * @return a List containing the objects extracted by the extractLogic function
     * */
    public List<Object> selectQuery(String tableName,
                                    ExtractFromShardLambda<S, Collection> extractLogic,
                                    boolean onRead,
                                    List<Row> rawData,
                                    WriteShardLambda<S> writeRawDataLogic
    ){
        if(onRead){
            SelectQuery<S> query = new SelectQuery<>(tableName);
            query.setExtractFromShardLambda(extractLogic);
            return broker.retrieveAndCombineReadQuery(query);
        }else{
            VolatileSelectQuery query = new VolatileSelectQuery(tableName);
            query.setExtractFromShardLambda(extractLogic);
            query.setWriteRawDataLambda(writeRawDataLogic);
            return broker.volatileShuffleQuery(query, rawData);
        }
    }

    /**Stops the broker*/
    public void shutdown(){
        broker.shutdown();
    }
}
