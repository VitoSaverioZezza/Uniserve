package edu.stanford.futuredata.uniserve.kvmockinterface.queryplans;

import com.google.protobuf.ByteString;
import edu.stanford.futuredata.uniserve.interfaces.Row;
import edu.stanford.futuredata.uniserve.interfaces.Shard;
import edu.stanford.futuredata.uniserve.interfaces.VolatileShuffleQueryPlan;
import edu.stanford.futuredata.uniserve.kvmockinterface.KVRow;
import edu.stanford.futuredata.uniserve.kvmockinterface.KVShard;
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
public class KVVolatileAverage implements VolatileShuffleQueryPlan< Integer> {
    private String table = "intermediateFilter";
    private static final Logger logger = LoggerFactory.getLogger(KVVolatileAverage.class);

    @Override
    public String getQueriedTables() {
        return table;
    }

    @Override
    public Map<Integer, List<ByteString>> scatter(Shard shard, int actorCount) {
        List<KVRow> data = ((KVShard) shard).getData();
        Integer partitionKey;
        Map<Integer, List<KVRow>> rowAssignment = new HashMap<>();
        for(Object r: data){
            KVRow row = (KVRow) r;
            int key = row.getKey();
            partitionKey = ConsistentHash.hashFunction(key) % actorCount;
            rowAssignment.computeIfAbsent(partitionKey, k -> new ArrayList<>()).add(row);
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


    public Map<Integer, List<ByteString>> scatter(List<Object> data, int actorCount) {
        Integer partitionKey;
        Map<Integer, List<KVRow>> rowAssignment = new HashMap<>();
        for(Object r: data){
            KVRow row = (KVRow) r;
            int key = row.getKey();
            partitionKey = ConsistentHash.hashFunction(key) % actorCount;
            rowAssignment.computeIfAbsent(partitionKey, k -> new ArrayList<>()).add(row);
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
            totalSum = totalSum + deserializedItem.getValue1();
            totalCount = totalCount + deserializedItem.getValue0();
        }
        int average = totalSum / totalCount;
        return average;
    }

    @Override
    public void setTableName(String tableName) {
        this.table = "intermediateFilter";
    }

    @Override
    public boolean write(Shard shard, List<Row> data) {
        ((KVShard) shard).setRows((List)data);
        ((KVShard) shard).insertRows();
        return true;
    }
}
