package edu.stanford.futuredata.uniserve.kvmockinterface.queryplans;

import com.google.protobuf.ByteString;
import edu.stanford.futuredata.uniserve.interfaces.AnchoredReadQueryPlan;
import edu.stanford.futuredata.uniserve.kvmockinterface.KVRow;
import edu.stanford.futuredata.uniserve.kvmockinterface.KVShard;
import edu.stanford.futuredata.uniserve.utilities.ConsistentHash;
import edu.stanford.futuredata.uniserve.utilities.Utilities;
import org.javatuples.Pair;

import java.util.*;

public class KVAverageRead implements AnchoredReadQueryPlan<KVShard, Integer> {
    private final List<String> tables = List.of("intermediateFilter");


    @Override
    public List<String> getQueriedTables() {
        return tables;
    }

    @Override
    public Map<String, List<Integer>> keysForQuery() {
        return Map.of(tables.get(0), List.of(-1));
    }

    @Override
    public String getAnchorTable() {
        return "intermediateFilter";
    }

    @Override
    public List<Integer> getPartitionKeys(KVShard kvShard) {
        return List.of(-1);
    }

    @Override
    public Map<Integer, List<ByteString>> scatter(KVShard shard, Map<Integer, List<Integer>> partitionKeys) {
        assert (false);
        return null;
    }

    @Override
    public ByteString gather(KVShard localShard, Map<String, List<ByteString>> ephemeralData, Map<String, KVShard> ephemeralShards) {
        int sum = 0, count = 0;
        for(Map.Entry<Integer, Integer> e: localShard.KVMap.entrySet()){
            count++;
            sum = sum + e.getValue();
        }
        Pair<Integer, Integer> sumCount = new Pair<>(sum, count);
        return Utilities.objectToByteString(sumCount);
    }

    @Override
    public Integer combine(List<ByteString> shardQueryResults) {
        int totalSum=0, totalCount=0, avg=0;
        for(ByteString serializedPartialResult: shardQueryResults){
            Pair<Integer, Integer> sumCount = (Pair<Integer, Integer>) Utilities.byteStringToObject(serializedPartialResult);
            totalSum = totalSum + sumCount.getValue0();
            totalCount = totalCount + sumCount.getValue1();
        }
        avg = totalSum/totalCount;
        return avg;
    }
}
