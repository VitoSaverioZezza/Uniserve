package edu.stanford.futuredata.uniserve.rel.queryplans;

import com.google.protobuf.ByteString;
import edu.stanford.futuredata.uniserve.interfaces.ReadQueryResults;
import edu.stanford.futuredata.uniserve.interfaces.RetrieveAndCombineQueryPlan;
import edu.stanford.futuredata.uniserve.relational.RelReadQueryResults;
import edu.stanford.futuredata.uniserve.relational.RelRow;
import edu.stanford.futuredata.uniserve.relational.RelShard;
import edu.stanford.futuredata.uniserve.relationalapi.ReadQuery;
import edu.stanford.futuredata.uniserve.utilities.Utilities;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SubquerySimpleRead implements RetrieveAndCombineQueryPlan<RelShard, RelReadQueryResults> {
    Map<String, ReadQuery> rqr = new HashMap<>();

    public void setRQRInput(String alias, ReadQuery res){
        rqr.put(alias, res);
    }

    @Override
    public List<String> getTableNames() {
        return new ArrayList<>();
    }

    @Override
    public boolean isThisSubquery() {
        return false;
    }

    @Override
    public Map<String, List<Integer>> keysForQuery() {
        return new HashMap<>();
    }

    @Override
    public ByteString retrieve(RelShard shard, String tableName, Map<String, ReadQueryResults> concreteSubqueriesResults) {
        List<RelRow> data = shard.getData();
        List<Integer> firstField = new ArrayList<>();
        for(RelRow r: data){
            firstField.add((Integer) r.getField(0));
        }
        Integer[] dataarray = firstField.toArray(new Integer[0]);
        return Utilities.objectToByteString(dataarray);
    }

    @Override
    public RelReadQueryResults combine(Map<String, List<ByteString>> retrieveResults) {
        List<Integer[]> dataArrays = new ArrayList<>();
        for (ByteString bs: retrieveResults.get("Results")){
            dataArrays.add((Integer[]) Utilities.byteStringToObject(bs));
        }
        List<RelRow> resultData = new ArrayList<>();
        for (Integer[] a: dataArrays) {
            for (Integer i : a){
                resultData.add(new RelRow(i));
            }
        }
        RelReadQueryResults rqr = new RelReadQueryResults();
        rqr.addData(resultData);
        return rqr;
    }

    @Override
    public void writeIntermediateShard(RelShard shard,ByteString retrievedData) {
        List<Object> data = (List<Object>) Utilities.byteStringToObject(retrievedData);
        List<RelRow> rows = new ArrayList<>();
        for(Object o: data){
            rows.add((RelRow) o);
        }
        shard.insertRows(rows);
        shard.committRows();
    }

    @Override
    public Map<String, ReadQuery> getVolatileSubqueries() {
        return rqr;
    }
}
