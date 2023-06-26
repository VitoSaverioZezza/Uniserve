package edu.stanford.futuredata.uniserve.relationalmock.queryplans;

import com.google.protobuf.ByteString;
import edu.stanford.futuredata.uniserve.interfaces.RetrieveAndCombineQueryPlan;
import edu.stanford.futuredata.uniserve.interfaces.SerializablePredicate;
import edu.stanford.futuredata.uniserve.relationalmock.RMRow;
import edu.stanford.futuredata.uniserve.relationalmock.RMShard;
import edu.stanford.futuredata.uniserve.utilities.Utilities;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class RMDynFilter implements RetrieveAndCombineQueryPlan<RMShard, List<RMRow>> {
    String tableName;
    SerializablePredicate<RMRow> filterPredicate;

    public RMDynFilter(
            SerializablePredicate<RMRow> filterPredicate,
            String tableName){
        this.filterPredicate = filterPredicate;
        this.tableName = tableName;
    }

    @Override
    public List<String> getTableNames() {
        return List.of(tableName);
    }

    @Override
    public Map<String, List<Integer>> keysForQuery() {
        return Map.of(tableName,List.of(-1));
    }

    @Override
    public ByteString retrieve(RMShard rmShard) {
        List<RMRow> rawData = rmShard.getPersons();
        List<RMRow> filteredData = new ArrayList<>();
        for(RMRow row: rawData){
            if(filterPredicate.test(row)){
                filteredData.add(row);
            }
        }
        RMRow[] filteredDataArray = filteredData.toArray(new RMRow[0]);
        return Utilities.objectToByteString(filteredDataArray);
    }

    @Override
    public List<RMRow> combine(Map<String, List<ByteString>> map) {
        List<RMRow> result = new ArrayList<>();
        for(Map.Entry<String, List<ByteString>> e: map.entrySet()){
            List<ByteString> serializedRowArrays = e.getValue();
            for(ByteString serializedArray: serializedRowArrays){
                RMRow[] rowArray = (RMRow[]) Utilities.byteStringToObject(serializedArray);
                Collections.addAll(result, rowArray);
            }
        }
        return result;
    }
}
