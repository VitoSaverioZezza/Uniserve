package edu.stanford.futuredata.uniserve.secondapi;

import edu.stanford.futuredata.uniserve.secondapi.querybuilders.PersistentReadQueryBuilder;
import edu.stanford.futuredata.uniserve.secondapi.querybuilders.ReadQueryBuilder;
import edu.stanford.futuredata.uniserve.secondapi.querybuilders.WriteQueryBuilder;

public class SimpleUniserveAPI {

    /**Returns a query generic read query builder, which switches to a specific query builder once the correct
     * parameters have been given.
     *
     * To build a well-formed retrieve and combine query, the user needs to provide via the appropriate setter methods the
     * following items:
     * - a List of table names
     * - a Map between table names and Serializable Lambda Functions implementing the RetrieveLambda functional interface
     *      each lambda describing how to extract the desired data items from actors of that associated table
     * - a Serializable Lambda Function implementing the CombineLambdaRetAndComb functional interface describing how
     *      all retrieved data items should be combined to build the query result
     * additionally the user can provide the keysForQuery Map between table names and list of integers representing
     * row keys to be queried. This can be used to let the query run on a subset of table's rows. If the user wants
     * the query to run on all table's rows they can either not include the table name in the keys for query mapping
     * or include the wildcard integer -1 into the list associated with the specific table.
     *
     * To build a well-formed shuffle query, the user needs to provide via the appropriate setter methods the following:
     * - a list of table names
     * - a map between table names and serializable lambda functions implementing the ScatterLambda functional interface
     *      each lambda describing how to extract data items from actors and how to assign them to the servers in the
     *      cluster.
     * - a Serializable lambda function implementing the GatherLambda functional interface that specifies how each server
     *      should combine the results of the scatter operations executed before.
     * - a Serializable lambda function implementing the CombineLambdaShuffle functional interface that specifies how the
     *      results returned by the execution of all gathers should be combined to obtain the query results.
     *
     * The returned query plans handle the shuffling, serialization and deserialization of all partial results. The user
     * needs to ensure runtime correctness of the query. The query built offers the run(Broker b) method that once invoked
     * returns the results of the query to the caller.
     *
     * An example of query creation and execution is:
     *
     * Broker brk = new Broker(zkHost, zkPort);
     * SimpleUniserveAPI api = new SimpleUniserveAPI();
     * List< Row > res = api.readQuery().setTableNames("tableOne").setRetrieveLogics((Shard s)->s.retrieveAll)
     *                      .setCombineLogic((Map< String, List< Object>> results) -> results.get("tableOne")).build()
     *                      .run(brk);
     * (for reading purposes the exceptions have not been taken into account in the example)
     **/
    public ReadQueryBuilder readQuery(){
        return new ReadQueryBuilder();
    }

    /**Returns a query generic write query builder, which switches to a specific query builder once the correct
     * parameters have been given.
     *
     * To build a well-formed eventually consistent write query the user needs to provide via the appropriate setter
     * methods:
     * - A string representing the name of the table the write needs to be performed on
     * - A Serializable lambda function implementing the WriteShardLambda functional interface. The lambda function
     *      takes as parameters an actor and a List of rows and defines how the rows need to be written in the given
     *      actor. The function returns true if and only if the write operation on that shard has been carried out
     *      successfully
     *
     * To build a well-formed 2PC-style write query the user needs to provide:
     * - A string representing the name of the table the write operation is performed on
     * - A serializable lambda function implementing the WriteShardLambda functional interface, describing how the
     *      pre-commit operation of the given List of rows needs to be carried out on the given actor, returning
     *      true if and only if the pre-commit operation has been carried out successfully
     * - A serializable lambda function implementing the CommitLambda functional interface that describes how the
     *      commit operation needs to be carried out on the given actor. The system ensures that the pre-commit
     *      operation has been successfully executed on all actors before the commit is executed
     * - A serializable lambda function implementing the CommitLambda functional interface that describes how
     *      the abort operation needs to be performed on the given actor. The system ensures that the abort operation
     *      is performed on all shards the pre-commit operation has been successfully executed, effectively aborting
     *      the transaction.
     *
     * The system handles the raw data distribution and transaction atomicity and consistency. The user needs to
     * guarantee the runtime correctness of the query taking into account the data model. The method may throw
     * an Exception if the query is not well-formed.
     *
     * An example of query declaration and execution is:
     *
     * Broker brk = new Broker(zkHost, zkPort);
     * SimpleUniserveAPI api = new SimpleUniserveAPI();
     * boolean res = api.writeQuery().setQueriedTable("tableOne")
     *                      .setWriteLogic((Shard s, List< Row> data) -> s.write(data))
     *                      .build()
     *                      .run(brk);
     * (for reading purposes the exceptions have not been taken into account in the example)
     **/
    public WriteQueryBuilder writeQuery(){
        return new WriteQueryBuilder();
    }

    /**Returns a persistent query builder, which switches to a specific query builder once the correct
     * parameters have been given. The built query is NOT registered.
     *
     * A persistent read query is composed of a read query (either shuffle read or retrieve and combine read) and a
     * write query that takes the results of the read as input.
     * The user needs to provide a string representing the name of the query, the name is used to ensure that
     * the same query is not registered multiple times, therefore a suitable name encapsulates the purpose of the query.
     * The user can also specify the type of the read query by setting the boolean "shuffle" parameter and the
     * boolean "twoPhaseCommit". The two attributes are initialized as false.
     *
     * The user needs to provide all logic needed to build the two queries in the same way the logic is provided to
     * read and write queries. The built query needs to be explicitly registered and run.
     *
     * An example of query declaration, registration and execution is:
     *
     * Broker brk = new Broker(zkHost, zkPort);
     * SimpleUniserveAPI api = new SimpleUniserveAPI();
     * List<Row> res = api.persistentQuery().setQueryName("CopyTableOneInTableTwo)
     *                  .setTwoPhaseCommit(false).setShuffle(false)
     *                  .setSourceTables(List.of("tableOne").setSinkTable("tableTwo")
     *                  .setRetrieveLogic((Shard s) -> s.retrieveAll())
     *                  .setCombineLogic((Map< String, List< Object>> results) -> results.get("tableOne"))
     *                  .setWriteLogic((Shard s, List< Row> data) -> s.write(data))
     *                  .build()
     *                  .registerQuery(brk)
     *                  .run(brk);
     * (for reading purposes the exceptions have not been taken into account in the example)
     */
    public PersistentReadQueryBuilder persistentQuery(){ return new PersistentReadQueryBuilder();}
}
