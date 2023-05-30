package edu.stanford.futuredata.uniserve.kvmockinterface.queryplans;

import com.google.protobuf.ByteString;
import edu.stanford.futuredata.uniserve.datastore.DataStore;
import edu.stanford.futuredata.uniserve.interfaces.VolatileShuffleQueryPlan;
import edu.stanford.futuredata.uniserve.kvmockinterface.KVRow;
import edu.stanford.futuredata.uniserve.utilities.ConsistentHash;
import edu.stanford.futuredata.uniserve.utilities.Utilities;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**Returns the average of all values for all keys*/
public class KVVolatileAverage implements VolatileShuffleQueryPlan<KVRow, Integer> {
    private final String table = "table";
    private static final Logger logger = LoggerFactory.getLogger(KVVolatileAverage.class);

    @Override
    public String getQueriedTables() {
        return table;
    }

    @Override
    public Map<Integer, List<ByteString>> scatter(List<KVRow> data, int actorCount) {
        Integer partitionKey;
        Map<Integer, List<KVRow>> rowAssignment = new HashMap<>();
        for(KVRow row: data){
            int key = row.getKey();
            partitionKey = ConsistentHash.hashFunction(key) % actorCount;
            rowAssignment.computeIfAbsent(partitionKey, k -> new ArrayList<>()).add(row);
            logger.info("row ({},{}) assigned to ds {} by the scatter operation", row.getKey(), row.getValue(), partitionKey);
        }
        Map<Integer, List<ByteString>> serializedRowAssignment = new HashMap<>();
        for(Map.Entry<Integer, List<KVRow>> entry: rowAssignment.entrySet()){
            for(KVRow row: entry.getValue()){
                ByteString serializedRow = Utilities.objectToByteString(row);
                serializedRowAssignment.computeIfAbsent(entry.getKey(), k -> new ArrayList<>()).add(serializedRow);
            }
        }
        return serializedRowAssignment;
    }

    @Override
    public ByteString gather(List<ByteString> serializedScatteredData) {
        /* no objects having the same key */
        List<KVRow> deserializedData = new ArrayList<>();
        for(ByteString serializedRow : serializedScatteredData){
            KVRow deserializedRow = (KVRow) Utilities.byteStringToObject(serializedRow);
            deserializedData.add(deserializedRow);
        }
        Integer count = 0, sum = 0, avg = 0;
        for(KVRow row: deserializedData){
            logger.info("Processing ({},{})", row.getKey(), row.getValue());
            count++;
            sum = sum + row.getValue();
        }
        Pair<Integer, Integer> countSum = new Pair<>(count, sum);
        ByteString serializedResult = Utilities.objectToByteString(countSum);
        return serializedResult;
    }

    @Override
    public Integer combine(List<ByteString> gatherResults) {
        List<Pair<Integer,Integer>> deserializedData = new ArrayList<>();
        for(ByteString serializedDataItem : gatherResults){
            deserializedData.add((Pair<Integer, Integer>) Utilities.byteStringToObject(serializedDataItem));
        }
        int totalSum = 0, totalCount = 0;
        for(Pair<Integer, Integer> deserializedItem: deserializedData){
            logger.info("Processing pair count: {}, sum: {}", deserializedItem.getValue0(), deserializedItem.getValue1());
            totalSum = totalSum + deserializedItem.getValue1();
            totalCount = totalCount + deserializedItem.getValue0();
            logger.info("sum: {}, count: {}", totalSum, totalCount);
        }
        logger.info("computing average");
        int average = totalSum / totalCount;
        logger.info("average {} / {} = {}", totalSum, totalCount, average);
        return average;
    }
}
