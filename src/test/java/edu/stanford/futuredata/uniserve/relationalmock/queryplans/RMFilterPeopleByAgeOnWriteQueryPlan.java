package edu.stanford.futuredata.uniserve.relationalmock.queryplans;

import com.google.protobuf.ByteString;
import edu.stanford.futuredata.uniserve.interfaces.VolatileShuffleQueryPlan;
import edu.stanford.futuredata.uniserve.relationalmock.RMRowPerson;
import edu.stanford.futuredata.uniserve.relationalmock.queryplans.planbuilders.RMFilterPeopleByAgeOnWriteQueryPlanBuilder;
import edu.stanford.futuredata.uniserve.utilities.Utilities;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RMFilterPeopleByAgeOnWriteQueryPlan implements VolatileShuffleQueryPlan<RMRowPerson, List<RMRowPerson>> {

    private final String queriedTables = "People";
    private final int minAge;
    private final int maxAge;

    public RMFilterPeopleByAgeOnWriteQueryPlan(RMFilterPeopleByAgeOnWriteQueryPlanBuilder builder){
        this.minAge = builder.getMinAge();
        this.maxAge = builder.getMaxAge();
    }

    @Override
    public String getQueriedTables() {
        return queriedTables;
    }

    @Override
    public Map<Integer, List<ByteString>> scatter(List<RMRowPerson> data, int actorCount) {
        Map<Integer, List<ByteString>> results = new HashMap<>();
        for(RMRowPerson row: data){
            if(row.getAge() < maxAge && row.getAge() > minAge) {
                int key = row.getAge() % actorCount;
                ByteString serializedRow = Utilities.objectToByteString(row);
                results.computeIfAbsent(key, k -> new ArrayList<>()).add(serializedRow);
            }
        }
        return results;
    }

    @Override
    public ByteString gather(List<ByteString> scatteredData) {
        ByteString[] scatteredDataArray = scatteredData.toArray(new ByteString[0]);
        return Utilities.objectToByteString(scatteredDataArray);
    }

    @Override
    public List<RMRowPerson> combine(List<ByteString> gatherResults) {
        List<RMRowPerson> results = new ArrayList<>();
        for(ByteString serializedGatherResult: gatherResults){
            ByteString[] gatherResultsArray = (ByteString[]) Utilities.byteStringToObject(serializedGatherResult);
            for(ByteString b:gatherResultsArray){
                results.add((RMRowPerson) Utilities.byteStringToObject(b));
            }
        }
        return results;
    }
}
