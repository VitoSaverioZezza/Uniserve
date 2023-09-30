package edu.stanford.futuredata.uniserve.rel.queryplans;

import com.google.protobuf.ByteString;
import edu.stanford.futuredata.uniserve.interfaces.ReadQueryResults;
import edu.stanford.futuredata.uniserve.interfaces.RetrieveAndCombineQueryPlan;
import edu.stanford.futuredata.uniserve.relational.RelReadQueryResults;
import edu.stanford.futuredata.uniserve.relational.RelRow;
import edu.stanford.futuredata.uniserve.relational.RelShard;
import edu.stanford.futuredata.uniserve.utilities.Utilities;

import java.util.*;

public class SimpleReadAll implements RetrieveAndCombineQueryPlan<RelShard, Object> {
    List<String> tableNames = List.of("TableOne", "TableTwo");

    @Override
    public List<String> getTableNames() {
        return tableNames;
    }

    @Override
    public boolean isThisSubquery() {
        return false;
    }

    @Override
    public Map<String, List<Integer>> keysForQuery() {
        Map<String, List<Integer>> kfq = new HashMap<>();
        for(String tn:tableNames){
            kfq.put(tn, List.of(-1));
        }
        return kfq;
    }

    @Override
    public ByteString retrieve(RelShard shard, String tableName, Map<String, ReadQueryResults> concreteSubqueriesResults) {
        List<RelRow> data = shard.getData();
        RelRow[] dataArray = data.toArray(new RelRow[0]);
        return Utilities.objectToByteString(dataArray);
    }


    @Override
    public Object combine(Map<String, List<ByteString>> retrieveResults) {
        RelReadQueryResults relReadQueryResults = new RelReadQueryResults();
        List<RelRow> t1res = new ArrayList<>();
        List<RelRow> t2res = new ArrayList<>();
        List<ByteString>setT1=retrieveResults.get("TableOne");
        List<ByteString>setT2=retrieveResults.get("TableTwo");

        for(ByteString bs:setT1){
            t1res.addAll(Arrays.asList((RelRow[]) Utilities.byteStringToObject(bs)));
        }
        for(ByteString bs:setT2){
            t2res.addAll(Arrays.asList((RelRow[]) Utilities.byteStringToObject(bs)));
        }
        String alias = "Results";
        List<RelRow> resRows = new ArrayList<>();
        for(RelRow r1:t1res){
            resRows.add(new RelRow(r1.getField(0), r1.getField(1), r1.getField(2), null));
        }
        for(RelRow r1:t2res){
            resRows.add(new RelRow(r1.getField(0), r1.getField(1), r1.getField(2), r1.getField(3)));
        }

        relReadQueryResults.addData(resRows);
        relReadQueryResults.setName(alias);
        relReadQueryResults.setFieldNames(new ArrayList<>(Arrays.asList(new String[]{"AD", "BE", "CF", "G"})));
        return relReadQueryResults;
    }
}
