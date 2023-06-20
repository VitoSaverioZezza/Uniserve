package edu.stanford.futuredata.uniserve.operators;

import com.google.protobuf.ByteString;
import edu.stanford.futuredata.uniserve.interfaces.*;

import java.util.List;
import java.util.Map;

public class AggregateOnReadOperator<R extends Row, S extends Shard<R>, T> implements RetrieveAndCombineQueryPlan<S, T> {
    private final List<String> tableNames;
    private final Map<String, List<Integer>> keysForQuery;
    private final RetrieveLambda<S, ByteString> retrieveLambda;
    private final CombineLambda<Map<String, List<ByteString>>, T> combineLambda;

    public AggregateOnReadOperator(
            List<String> tableNames,
            Map<String, List<Integer>> keysForQuery,
            RetrieveLambda<S, ByteString> retrieveLambda,
            CombineLambda<Map<String, List<ByteString>>, T> combineLambda
    ){
        this.tableNames=tableNames;
        this.keysForQuery=keysForQuery;
        this.retrieveLambda=retrieveLambda;
        this.combineLambda=combineLambda;
    }

    @Override
    public List<String> getTableNames() {
        return tableNames;
    }

    @Override
    public Map<String, List<Integer>> keysForQuery() {
        return keysForQuery;
    }

    @Override
    public ByteString retrieve(S shard) {
        return retrieveLambda.retrieve(shard);
    }

    @Override
    public T combine(Map<String, List<ByteString>> retrieveResults) {
        return combineLambda.combine(retrieveResults);
    }
}
