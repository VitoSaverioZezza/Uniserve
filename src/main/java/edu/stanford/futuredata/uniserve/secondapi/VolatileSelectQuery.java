package edu.stanford.futuredata.uniserve.secondapi;

import com.google.protobuf.ByteString;
import edu.stanford.futuredata.uniserve.interfaces.Row;
import edu.stanford.futuredata.uniserve.interfaces.Shard;
import edu.stanford.futuredata.uniserve.interfaces.VolatileShuffleQueryPlan;
import edu.stanford.futuredata.uniserve.secondapi.lambdamethods.ExtractFromShardLambda;
import edu.stanford.futuredata.uniserve.secondapi.lambdamethods.WriteShardLambda;
import edu.stanford.futuredata.uniserve.utilities.Utilities;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class VolatileSelectQuery implements VolatileShuffleQueryPlan<List<Object>> {
    private String tableName;
    private Serializable extractFromShardLambda;
    private Serializable writeRawDataLambda;

    public VolatileSelectQuery(String tableName){
        this.tableName = tableName;
    }

    @Override
    public String getQueriedTables() {
        return tableName;
    }

    @Override
    public Map<Integer, List<ByteString>> scatter(Shard data, int actorCount) {
        Map<Integer, List<ByteString>> result = new HashMap<>();
        List<Object> listOfExtractedData = (List<Object>) extract(data);

        for(Object singleDataItem: listOfExtractedData){
            int key = singleDataItem.hashCode() % actorCount;
            result.computeIfAbsent(key, k->new ArrayList<>());
            Object[] oArray = new Object[]{singleDataItem};
            result.get(key).add(Utilities.objectToByteString(oArray));
        }

        return result;
    }

    @Override
    public ByteString gather(List<ByteString> scatteredData) {
        List<Object> res = new ArrayList<>();
        for(ByteString bs: scatteredData){
            Object[] objArray = (Object[]) Utilities.byteStringToObject(bs);
            res.add(objArray[0]);
        }
        return Utilities.objectToByteString(res.toArray());
    }

    @Override
    public List<Object> combine(List<ByteString> gatherResults) {
        List<Object> result = new ArrayList<>();
        for(ByteString gathRes: gatherResults){
            Object[] res = (Object[]) Utilities.byteStringToObject(gathRes);
            for(Object o: res){
                result.add(o);
            }
        }
        return result;
    }

    @Override
    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public boolean write(Shard shard, List<Row> data) {
        return executeWrite(shard, data);
    }

    //List of Objects->rows->kvRows
    private Object extract(Shard shard){
        return ((ExtractFromShardLambda<Shard, Object>) extractFromShardLambda).extract(shard);
    }
    private boolean executeWrite(Shard shard, List<Row> data){
        return ((WriteShardLambda<Shard>) writeRawDataLambda).write(shard, data);
    }
    public void setExtractFromShardLambda(Serializable extractFromShardLambda){
        this.extractFromShardLambda = extractFromShardLambda;
    }
    public void setWriteRawDataLambda(Serializable writeShard){
        this.writeRawDataLambda = writeShard;
    }
}
