package edu.stanford.futuredata.uniserve.operators;


import com.google.protobuf.ByteString;
import edu.stanford.futuredata.uniserve.interfaces.*;
import edu.stanford.futuredata.uniserve.utilities.Utilities;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class FilterOnReadOperator<R extends Row, S extends Shard<R>> implements RetrieveAndCombineQueryPlan<S, List<R>> {
    private final Map<String, List<Integer>> keysForQuery;
    private final SerializablePredicate<R> filterPredicate;
    private final List<String> tableNames;

    public FilterOnReadOperator(
            Map<String, List<Integer>> keysForQuery,
            SerializablePredicate<R> filterPredicate,
            List<String> tableNames
    ){
        this.keysForQuery = keysForQuery;
        this.filterPredicate = filterPredicate;
        this.tableNames = tableNames;
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
        List<R> rawData = shard.getData();
        List<ByteString> serFilteredData = new ArrayList<>();
        for(R row: rawData){
            if(filterPredicate.test(row)){
                serFilteredData.add(Utilities.objectToByteString(row));
            }
        }
        ByteString[] serializedResArray = serFilteredData.toArray(new ByteString[0]);
        return Utilities.objectToByteString(serializedResArray);
    }

    @Override
    public List<R> combine(Map<String, List<ByteString>> retrieveResults) {
        List<R> res = new ArrayList<>();
        List<ByteString> serRes = new ArrayList<>();
        for(Map.Entry<String, List<ByteString>> e:retrieveResults.entrySet()){
            for(ByteString serArray: e.getValue()){
                ByteString[] serRowsArray = (ByteString[]) Utilities.byteStringToObject(serArray);
                serRes.addAll(Arrays.asList(serRowsArray));
            }
        }
        for(ByteString serRow: serRes){
            res.add((R) Utilities.byteStringToObject(serRow));
        }
        return res;
    }
}
