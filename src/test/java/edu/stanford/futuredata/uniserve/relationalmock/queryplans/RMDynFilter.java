package edu.stanford.futuredata.uniserve.relationalmock.queryplans;

import com.google.protobuf.ByteString;
import edu.stanford.futuredata.uniserve.interfaces.RetrieveAndCombineQueryPlan;
import edu.stanford.futuredata.uniserve.interfaces.SerializablePredicate;
import edu.stanford.futuredata.uniserve.relationalmock.RMRowPerson;
import edu.stanford.futuredata.uniserve.relationalmock.RMShard;
import edu.stanford.futuredata.uniserve.utilities.Utilities;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class RMDynFilter implements RetrieveAndCombineQueryPlan<RMShard, List<RMRowPerson>> {
    String tableName;
    SerializablePredicate<RMRowPerson> filterPredicate;

    public RMDynFilter(
            SerializablePredicate<RMRowPerson> filterPredicate,
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
        List<RMRowPerson> rawData = rmShard.getPersons();
        List<RMRowPerson> filteredData = new ArrayList<>();
        for(RMRowPerson row: rawData){
            if(filterPredicate.test(row)){
                filteredData.add(row);
            }
        }
        RMRowPerson[] filteredDataArray = filteredData.toArray(new RMRowPerson[0]);
        return Utilities.objectToByteString(filteredDataArray);
    }

    @Override
    public List<RMRowPerson> combine(Map<String, List<ByteString>> map) {
        List<RMRowPerson> result = new ArrayList<>();
        for(Map.Entry<String, List<ByteString>> e: map.entrySet()){
            List<ByteString> serializedRowArrays = e.getValue();
            for(ByteString serializedArray: serializedRowArrays){
                RMRowPerson[] rowArray = (RMRowPerson[]) Utilities.byteStringToObject(serializedArray);
                Collections.addAll(result, rowArray);
            }
        }
        return result;
    }
}
