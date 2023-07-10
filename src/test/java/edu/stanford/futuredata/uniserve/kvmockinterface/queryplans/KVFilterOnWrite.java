package edu.stanford.futuredata.uniserve.kvmockinterface.queryplans;

import com.google.protobuf.ByteString;
import edu.stanford.futuredata.uniserve.interfaces.Row;
import edu.stanford.futuredata.uniserve.interfaces.Shard;
import edu.stanford.futuredata.uniserve.interfaces.VolatileShuffleQueryPlan;
import edu.stanford.futuredata.uniserve.kvmockinterface.KVRow;
import edu.stanford.futuredata.uniserve.kvmockinterface.KVShard;
import edu.stanford.futuredata.uniserve.utilities.Utilities;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class KVFilterOnWrite implements VolatileShuffleQueryPlan<List<KVRow>> {
    private String table = "filterAndAverageRaw";

    @Override
    public String getQueriedTables() {
        return table;
    }

    @Override
    public Map<Integer, List<ByteString>> scatter(Shard shard, int actorCount) {
        List<KVRow> data = ((KVShard) shard).getData();
        Map<Integer, List<ByteString>> result = new HashMap<>();
        for(Object r: data) {
            KVRow row = (KVRow) r;
            if (row.getValue() < 10) {
                int partitionKey = row.getPartitionKey() % actorCount;
                result.computeIfAbsent(partitionKey, k -> new ArrayList<>()).add(Utilities.objectToByteString(row));
            }
        }
        return result;
    }

    public Map<Integer, List<ByteString>> scatter(List<Object> data, int actorCount) {
        Map<Integer, List<ByteString>> result = new HashMap<>();
        for(Object r: data) {
            KVRow row = (KVRow) r;
            if (row.getValue() < 10) {
                int partitionKey = row.getPartitionKey() % actorCount;
                result.computeIfAbsent(partitionKey, k -> new ArrayList<>()).add(Utilities.objectToByteString(row));
            }
        }
        return result;
    }

    @Override
    public ByteString gather(List<ByteString> scatteredData) {
        ByteString[] results = scatteredData.toArray(new ByteString[0]);
        return Utilities.objectToByteString(results);
    }

    @Override
    public List<KVRow> combine(List<ByteString> gatherResults) {
        List<KVRow> results = new ArrayList<>();
        for(ByteString serializedResultArray: gatherResults){
            ByteString[] resultArray = (ByteString[]) Utilities.byteStringToObject(serializedResultArray);
            for(ByteString serializedRow: resultArray){
                results.add((KVRow) Utilities.byteStringToObject(serializedRow));
            }
        }
        return results;
    }

    @Override
    public void setTableName(String tableName) {
        this.table = "filterAndAverageRaw";
    }

    @Override
    public boolean write(Shard shard, List<Row> data) {
        ((KVShard) shard).setRows((List) data);
        ((KVShard) shard).insertRows();
        return true;
    }
}
