package edu.stanford.futuredata.uniserve.api;

import edu.stanford.futuredata.uniserve.api.querybuilders.PersistentReadQueryBuilder;
import edu.stanford.futuredata.uniserve.api.querybuilders.ReadQueryBuilder;
import edu.stanford.futuredata.uniserve.api.querybuilders.WriteQueryBuilder;

public class SimpleUniserveAPI {
    /**</p>
     * Returns a query generic read query builder, which switches to a specific query builder once the correct
     * parameters have been given.
     *</p><br><br><p>
     * To build a well-formed retrieve and combine query, the user needs to provide via the appropriate setter methods the
     * following items:<br>
     * - a List of table names<br>
     * - a Map between table names and Serializable Lambda Functions implementing the RetrieveLambda functional interface<br>
     *      &emsp;each lambda describing how to extract the desired data items from actors of that associated table<br>
     * - a Serializable Lambda Function implementing the CombineLambdaRetAndComb functional interface describing how<br>
     *      &emsp;all retrieved data items should be combined to build the query result<br>
     * additionally the user can provide the keysForQuery Map between table names and list of integers representing
     * row keys to be queried. This can be used to let the query run on a subset of table's rows. If the user wants
     * the query to run on all table's rows they can either not include the table name in the keys for query mapping
     * or include the wildcard integer -1 into the list associated with the specific table.
     *</p><br><p>
     * To build a well-formed shuffle query, the user needs to provide via the appropriate setter methods the following:<br>
     * - a list of table names<br>
     * - a map between table names and serializable lambda functions implementing the ScatterLambda functional interface<br>
     *      &emsp;each lambda describing how to extract data items from actors and how to assign them to the servers in the<br>
     *      &emsp;cluster.<br>
     * - a Serializable lambda function implementing the GatherLambda functional interface that specifies how each server<br>
     *      &emsp;should combine the results of the scatter operations executed before.<br>
     * - a Serializable lambda function implementing the CombineLambdaShuffle functional interface that specifies how the<br>
     *      &emsp;results returned by the execution of all gathers should be combined to obtain the query results.<br>
     *</p><br><p>
     * The returned query plans handle the shuffling, serialization and deserialization of all partial results. The user
     * needs to ensure runtime correctness of the query. The query built offers the run(Broker b) method that once invoked
     * returns the results of the query to the caller.
     *</p><br><p>
     * An example of query creation and execution is:
     *<pre>
     * Broker brk = new Broker(zkHost, zkPort);
     * SimpleUniserveAPI api = new SimpleUniserveAPI();
     * List< Row > res = api.readQuery()
     *                      .setTableNames("tableOne")
     *                      .setRetrieveLogics((Shard s)->s.retrieveAll())
     *                      .setCombineLogic((Map< String, List< Object>> results) -> results.get("tableOne"))
     *                      .build()
     *                      .run(brk);
     *                      </pre>
     * (for reading purposes the exceptions have not been taken into account in the example)
     **/
    public ReadQueryBuilder readQuery(){
        return new ReadQueryBuilder();
    }

    /**<p>
     * Returns a query generic write query builder, which switches to a specific query builder once the correct
     * parameters have been given.
     *</p><br><p>
     * To build a well-formed eventually consistent write query the user needs to provide via the appropriate setter
     * methods:<br>
     * - A string representing the name of the table the write needs to be performed on<br>
     * - A Serializable lambda function implementing the WriteShardLambda functional interface. The lambda function<br>
     *      &emsp;takes as parameters an actor and a List of rows and defines how the rows need to be written in the given<br>
     *      &emsp;actor. The function returns true if and only if the write operation on that shard has been carried out<br>
     *      &emsp;successfully
     *</p><br><p>
     * To build a well-formed 2PC-style write query the user needs to provide:<br>
     * - A string representing the name of the table the write operation is performed on<br>
     * - A serializable lambda function implementing the WriteShardLambda functional interface, describing how the<br>
     *      &emsp; pre-commit operation of the given List of rows needs to be carried out on the given actor, returning<br>
     *      &emsp;true if and only if the pre-commit operation has been carried out successfully<br>
     * - A serializable lambda function implementing the CommitLambda functional interface that describes how the<br>
     *      &emsp;commit operation needs to be carried out on the given actor. The system ensures that the pre-commit<br>
     *      &emsp;operation has been successfully executed on all actors before the commit is executed<br>
     * - A serializable lambda function implementing the CommitLambda functional interface that describes how<br>
     *      &emsp;the abort operation needs to be performed on the given actor. The system ensures that the abort operation<br>
     *      &emsp;is performed on all shards the pre-commit operation has been successfully executed, effectively aborting<br>
     *      &emsp;the transaction.
     *</p><br><p>
     * The system handles the raw data distribution and transaction atomicity and consistency. The user needs to
     * guarantee the runtime correctness of the query taking into account the data model. The method may throw
     * an Exception if the query is not well-formed.
     *</p><br><p>
     * An example of query declaration and execution is:
     *<pre>
     * Broker brk = new Broker(zkHost, zkPort);
     * SimpleUniserveAPI api = new SimpleUniserveAPI();
     * boolean res = api.writeQuery()
     *                  .setQueriedTable("tableOne")
     *                  .setWriteLogic((Shard s, List< Row> data) -> s.write(data))
     *                  .build()
     *                  .run(brk);
     * </pre>
     * (for reading purposes the exceptions have not been taken into account in the example)
     **/
    public WriteQueryBuilder writeQuery(){
        return new WriteQueryBuilder();
    }

    /**<p>
     * Returns a persistent query builder, which switches to a specific query builder once the correct
     * parameters have been given. The built query is NOT registered.
     * </p><br>
     *<p>
     * A persistent read query is composed of a read query (either shuffle read or retrieve and combine read) and a
     * write query that takes the results of the read as input.
     * The user needs to provide a string representing the name of the query, the name is used to ensure that
     * the same query is not registered multiple times, therefore a suitable name encapsulates the purpose of the query.
     * The user can also specify the type of the read query by setting the boolean "shuffle" parameter and the
     * boolean "twoPhaseCommit". The two attributes are initialized as false.
     *</p><br>
     * <p>
     * The user needs to provide all logic needed to build the two queries in the same way the logic is provided to
     * read and write queries. The built query needs to be explicitly registered and run.
     *</p><br>
     * An example of query declaration, registration and execution is:
     *<br>
     * <pre>
     * Broker brk = new Broker(zkHost, zkPort);
     * SimpleUniserveAPI api = new SimpleUniserveAPI();
     * List<Row> res = api.persistentQuery()
     *                  .setQueryName("CopyTableOneInTableTwo)
     *                  .setTwoPhaseCommit(false)
     *                  .setShuffle(false)
     *                  .setSourceTables(List.of("tableOne"))
     *                  .setSinkTable("tableTwo")
     *                  .setRetrieveLogic((Shard s) -> s.retrieveAll())
     *                  .setCombineLogic((Map< String, List< Object>> results) -> results.get("tableOne"))
     *                  .setWriteLogic((Shard s, List< Row> data) -> s.write(data))
     *                  .build()
     *                  .registerQuery(brk)
     *                  .run(brk);
     *                  </pre>
     * (for reading purposes the exceptions have not been taken into account in the example)
     */
    public PersistentReadQueryBuilder persistentQuery(){ return new PersistentReadQueryBuilder();}
}