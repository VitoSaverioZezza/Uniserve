package edu.stanford.futuredata.uniserve.kvmockinterface.queryplans;

import com.google.protobuf.ByteString;
import edu.stanford.futuredata.uniserve.interfaces.AnchoredReadQueryPlan;
import edu.stanford.futuredata.uniserve.kvmockinterface.KVRow;
import edu.stanford.futuredata.uniserve.kvmockinterface.KVShard;
import edu.stanford.futuredata.uniserve.utilities.Utilities;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class KVFilterOnRead implements AnchoredReadQueryPlan<KVShard, List<KVRow>> {
    private final String table = "filterAndAverageRaw";

    @Override
    public List<String> getQueriedTables() {
        return List.of(table);
    }

    @Override
    public Map<String, List<Integer>> keysForQuery() {
        return Map.of(table, List.of(-1));
    }

    @Override
    public String getAnchorTable() {
        return table;
    }

    @Override
    public List<Integer> getPartitionKeys(KVShard kvShard) {
        return List.of(-1);
    }

    @Override
    public Map<Integer, List<ByteString>> scatter(KVShard shard, Map<Integer, List<Integer>> partitionKeys) {
        assert(false);
        return null;
    }

    @Override
    public ByteString gather(KVShard localShard, Map<String, List<ByteString>> ephemeralData, Map<String, KVShard> ephemeralShards) {
        Map<Integer, Integer> KVMap = localShard.KVMap;
        List<KVRow> result = new ArrayList<>();
        for(Map.Entry<Integer, Integer> e: KVMap.entrySet()){
            if(e.getValue() < 10){
                result.add(new KVRow(e.getKey(), e.getValue()));
            }
        }
        KVRow[] resultArray = result.toArray(new KVRow[0]);
        return Utilities.objectToByteString(resultArray);
    }

    @Override
    public List<KVRow> combine(List<ByteString> shardQueryResults) {
        List<KVRow> result = new ArrayList<>();
        for(ByteString serializedPartialResults :shardQueryResults){
            KVRow[] partialResults = (KVRow[]) Utilities.byteStringToObject(serializedPartialResults);
            for(KVRow r: partialResults){
                result.add(r);
            }
        }
        return result;
    }
}
