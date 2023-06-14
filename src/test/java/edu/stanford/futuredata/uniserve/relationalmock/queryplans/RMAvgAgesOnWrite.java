package edu.stanford.futuredata.uniserve.relationalmock.queryplans;

import com.google.protobuf.ByteString;
import edu.stanford.futuredata.uniserve.integration.RMTests;
import edu.stanford.futuredata.uniserve.interfaces.VolatileShuffleQueryPlan;
import edu.stanford.futuredata.uniserve.relationalmock.RMRowPerson;
import edu.stanford.futuredata.uniserve.utilities.Utilities;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RMAvgAgesOnWrite implements VolatileShuffleQueryPlan<RMRowPerson, Integer> {
    String queriedTables = "People";

    @Override
    public String getQueriedTables() {
        return queriedTables;
    }

    @Override
    public Map<Integer, List<ByteString>> scatter(List<RMRowPerson> data, int actorCount) {
        Map<Integer, List<ByteString>> assignment = new HashMap<>();
        for(RMRowPerson row: data){
            int key = row.getPartitionKey() % actorCount;
            assignment.computeIfAbsent(key, k->new ArrayList<>()).add(Utilities.objectToByteString(row));
        }
        return assignment;
    }

    @Override
    public ByteString gather(List<ByteString> scatteredData) {
        int count = 0, sum = 0;

        for(ByteString serializedRow: scatteredData){
            RMRowPerson row = (RMRowPerson) Utilities.byteStringToObject(serializedRow);
            sum = sum + row.getAge();
            count = count +1;
        }
        Pair<Integer, Integer> sumCount = new Pair<>(sum, count);
        return Utilities.objectToByteString(sumCount);
    }

    @Override
    public Integer combine(List<ByteString> gatherResults) {
        int sum = 0, count = 0;
        for(ByteString serializedSumCount: gatherResults){
            Pair<Integer, Integer> sumCount = (Pair<Integer, Integer>) Utilities.byteStringToObject(serializedSumCount);
            sum = sum + sumCount.getValue0();
            count = count + sumCount.getValue1();
        }
        return sum/count;
    }
}
