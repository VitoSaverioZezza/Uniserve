package edu.stanford.futuredata.uniserve.rel.queryplans;

import com.google.protobuf.ByteString;
import edu.stanford.futuredata.uniserve.interfaces.ReadQueryResults;
import edu.stanford.futuredata.uniserve.interfaces.RetrieveAndCombineQueryPlan;
import edu.stanford.futuredata.uniserve.relational.RelReadQueryResults;
import edu.stanford.futuredata.uniserve.relational.RelRow;
import edu.stanford.futuredata.uniserve.relational.RelShard;
import edu.stanford.futuredata.uniserve.utilities.Utilities;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SubquerySimpleRead implements RetrieveAndCombineQueryPlan<RelShard, RelReadQueryResults> {
    Map<String, ReadQueryResults> rqr = new HashMap<>();

    public void setRQRInput(String alias, ReadQueryResults res){
        rqr.put(alias, res);
    }

    @Override
    public List<String> getTableNames() {
        return new ArrayList<>();
    }

    @Override
    public Map<String, List<Integer>> keysForQuery() {
        return new HashMap<>();
    }

    @Override
    public ByteString retrieve(RelShard shard, String tableName) {
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
    public boolean writeSubqueryResults(RelShard shard, String tableName, List<Object> data) {
        List<RelRow> rows = new ArrayList<>();
        for(Object o: data){
            rows.add((RelRow) o);
        }
        shard.insertRows(rows);
        shard.committRows();
        return true;
    }

    @Override
    public Map<String, ReadQueryResults> getSubqueriesResults() {
        return rqr;
    }
}
