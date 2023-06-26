package edu.stanford.futuredata.uniserve.operators;

import com.google.protobuf.ByteString;
import edu.stanford.futuredata.uniserve.broker.Broker;
import edu.stanford.futuredata.uniserve.interfaces.*;

import java.util.List;
import java.util.Map;

public class QueryEngineOperator<R extends Row, S extends Shard<R>> implements QueryEngine {
    Broker broker;

    public QueryEngineOperator(Broker broker){this.broker = broker;}
    public List<R> filter(List<String> tableNames, Map<String, List<Integer>> partitionKeys, SerializablePredicate<R> filterPredicate){
            return broker.retrieveAndCombineReadQuery(new FilterOnReadOperator<R, S>(partitionKeys, filterPredicate, tableNames));
    }
    public List<R> filter(String tableName, SerializablePredicate<R> filterPredicate, List<R> data){
        broker.mapQuery(new FilterOnWriteOperator<R>(tableName,filterPredicate), data);
        return data;
    }

    public Object aggregate(List<String> tableNames, Map<String, List<Integer>> partitionKeys,
                           RetrieveLambda<S, ByteString> retrieveLogic,
                           CombineLambda<Map<String, List<ByteString>>, Object> combineLogic){
        return broker.retrieveAndCombineReadQuery(new AggregateOnReadOperator<R,S,Object>(tableNames, partitionKeys, retrieveLogic, combineLogic));
    }
}