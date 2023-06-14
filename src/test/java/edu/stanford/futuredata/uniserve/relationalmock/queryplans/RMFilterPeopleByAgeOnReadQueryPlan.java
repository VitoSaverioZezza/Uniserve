package edu.stanford.futuredata.uniserve.relationalmock.queryplans;

import com.google.protobuf.ByteString;
import edu.stanford.futuredata.uniserve.interfaces.AnchoredReadQueryPlan;
import edu.stanford.futuredata.uniserve.interfaces.RetrieveAndCombineQueryPlan;
import edu.stanford.futuredata.uniserve.relationalmock.RMRowPerson;
import edu.stanford.futuredata.uniserve.relationalmock.RMShard;
import edu.stanford.futuredata.uniserve.relationalmock.queryplans.planbuilders.RMFilterPeopleByAgeOnReadQueryPlanBuilder;
import edu.stanford.futuredata.uniserve.utilities.Utilities;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class RMFilterPeopleByAgeOnReadQueryPlan implements RetrieveAndCombineQueryPlan<RMShard, List<RMRowPerson>> {
    private String tableName;
    private int minAge;
    private int maxAge;

    public RMFilterPeopleByAgeOnReadQueryPlan(RMFilterPeopleByAgeOnReadQueryPlanBuilder builder){
        this.tableName = builder.getTableName();
        this.maxAge = builder.getMaxAge();
        this.minAge = builder.getMinAge();
    }
    @Override
    public String getTableName() {
        return tableName;
    }

    @Override
    public Map<String, List<Integer>> keysForQuery() {
        return Map.of("People", List.of(-1));
    }

    @Override
    public ByteString retrieve(RMShard shard) {
        if(shard == null){
            return Utilities.objectToByteString(new ByteString[0]);
        }
        List<ByteString> results = new ArrayList<>();
        for(RMRowPerson row: shard.getPersons()){
            if(row.getAge() > minAge && row.getAge() < maxAge) {
                results.add(Utilities.objectToByteString(row));
            }
        }
        return Utilities.objectToByteString(results.toArray(new ByteString[0]));
    }

    /**
     * @param retrievedResults Mapping between table names of the tables involved in the query and lists
     *                         containing all the ByteStrings returned by each retrieve operation for all shards.
     *                         The individual ByteStrings are serialized arrays of Rows
     * */
    @Override
    public List<RMRowPerson> combine(Map<String, List<ByteString>> retrievedResults) {
        List<RMRowPerson> res = new ArrayList<>();
        for(Map.Entry<String, List<ByteString>> entry: retrievedResults.entrySet()){
            for(ByteString serializedRowArray: entry.getValue()){
                ByteString[] deserializedArray = (ByteString[]) Utilities.byteStringToObject(serializedRowArray);
                for(ByteString serializedRow: deserializedArray){
                    RMRowPerson row = (RMRowPerson) Utilities.byteStringToObject(serializedRow);
                    res.add(row);
                }
            }
        }
        return res;
    }
}
