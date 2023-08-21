package edu.stanford.futuredata.uniserve.relationalapi;

import com.google.protobuf.ByteString;
import edu.stanford.futuredata.uniserve.broker.Broker;
import edu.stanford.futuredata.uniserve.interfaces.RetrieveAndCombineQueryPlan;
import edu.stanford.futuredata.uniserve.relational.RelReadQueryResults;
import edu.stanford.futuredata.uniserve.relational.RelRow;
import edu.stanford.futuredata.uniserve.relational.RelShard;
import edu.stanford.futuredata.uniserve.relationalapi.querybuilders.ReadQueryBuilder;
import edu.stanford.futuredata.uniserve.utilities.Utilities;
import org.javatuples.Pair;

import java.util.*;

public class SimpleAggregateQuery implements RetrieveAndCombineQueryPlan<RelShard, RelReadQueryResults> {
    private Map<String, ReadQuery> subquery = new HashMap<>();
    private List<Pair<Integer, String>> aggregatesSubschema = new ArrayList<>();
    private List<String> gatherInputRowsSchema = new ArrayList<>();
    private List<String> userFinalSchema = new ArrayList<>();
    private boolean stored = false;

    public SimpleAggregateQuery setStored(){
        this.stored = true;
        return this;
    }


    public SimpleAggregateQuery setIntermediateQuery(String queryName, ReadQuery intermediateQuery){
        subquery.put(queryName, intermediateQuery);
        return this;
    }
    public SimpleAggregateQuery setAggregatesSubschema(List<Pair<Integer,String>> aggregatesSubschema){
        this.aggregatesSubschema = aggregatesSubschema;
        return this;
    }
    public SimpleAggregateQuery setGatherInputRowsSchema(List<String> gatherInputRowsSchema){
        this.gatherInputRowsSchema = gatherInputRowsSchema;
        return this;
    }
    public SimpleAggregateQuery setFinalSchema(List<String> userFinalSchema){
        this.userFinalSchema = userFinalSchema;
        return this;
    }


    public RelReadQueryResults run(Broker broker){
        RelReadQueryResults res;
        res = broker.retrieveAndCombineReadQuery(this);
        res.setFieldNames(userFinalSchema);
        return res;
    }


    @Override
    public List<String> getTableNames() {
        return new ArrayList<>();
    }
    @Override
    public Map<String, List<Integer>> keysForQuery() {
        return new HashMap<>();
    }


    private List<Object> computePartialResults(List<RelRow> shardData){
        List<Object> partialResults = new ArrayList<>();
        for(Pair<Integer, String> aggregate: aggregatesSubschema){
            if(aggregate.getValue0().equals(ReadQueryBuilder.AVG)){
                Integer[] countSum = new Integer[2];
                Arrays.fill(countSum, 0);
                for(RelRow row: shardData){
                    countSum[0]++;
                    countSum[1] += (Integer) row.getField(gatherInputRowsSchema.indexOf(aggregate.getValue1()));
                }
                partialResults.add(countSum);
            } else if (aggregate.getValue0().equals(ReadQueryBuilder.MIN)) {
                Integer min = Integer.MAX_VALUE;
                for (RelRow row: shardData){
                    min = Integer.min(min, (Integer) row.getField(gatherInputRowsSchema.indexOf(aggregate.getValue1())));
                }
                partialResults.add(min);
            } else if (aggregate.getValue0().equals(ReadQueryBuilder.MAX)) {
                Integer max = Integer.MIN_VALUE;
                for (RelRow row: shardData){
                    max = Integer.max(max, (Integer) row.getField(gatherInputRowsSchema.indexOf(aggregate.getValue1())));
                }
                partialResults.add(max);
            } else if (aggregate.getValue0().equals(ReadQueryBuilder.COUNT)) {
                partialResults.add(shardData.size());
            } else if (aggregate.getValue0().equals(ReadQueryBuilder.SUM)) {
                Integer sum = 0;
                for (RelRow row: shardData){
                    sum += (Integer) row.getField(gatherInputRowsSchema.indexOf(aggregate.getValue1()));
                }
                partialResults.add(sum);
            }
        }
        return partialResults;
    }


    @Override
    public ByteString retrieve(RelShard shard, String tableName) {
        return Utilities.objectToByteString(computePartialResults(shard.getData()).toArray());
    }

    @Override
    public RelReadQueryResults combine(Map<String, List<ByteString>> retrieveResults) {
        List<ByteString> results = retrieveResults.get(new ArrayList<>(subquery.keySet()).get(0));
        List<List<Object>> partialResultList = new ArrayList<>();
        List<Object> res = new ArrayList<>();
        for(ByteString serializedPartialResults: results){
            partialResultList.add(Arrays.asList((Object[]) Utilities.byteStringToObject(serializedPartialResults)));
        }
        for(Pair<Integer, String> aggregate: aggregatesSubschema){
            if(aggregate.getValue0().equals(ReadQueryBuilder.AVG)){
                int sum = 0, count = 0;
                for(List<Object> partialAggregates: partialResultList){
                    Integer[] countSum = (Integer[]) partialAggregates.get(aggregatesSubschema.indexOf(aggregate));
                    sum += countSum[1];
                    count += countSum[0];
                }
                res.add(aggregatesSubschema.indexOf(aggregate), sum/count);
            } else if (aggregate.getValue0().equals(ReadQueryBuilder.MIN)) {
                int min = Integer.MAX_VALUE;
                for(List<Object> partialAggregates: partialResultList){
                    Integer currentMin = (Integer) partialAggregates.get(aggregatesSubschema.indexOf(aggregate));
                    min = Integer.min(min, currentMin);
                }
                res.add(aggregatesSubschema.indexOf(aggregate), min);
            } else if (aggregate.getValue0().equals(ReadQueryBuilder.MAX)) {
                int max = Integer.MIN_VALUE;
                for(List<Object> partialAggregates: partialResultList){
                    Integer currentMin = (Integer) partialAggregates.get(aggregatesSubschema.indexOf(aggregate));
                    max = Integer.max(max, currentMin);
                }
                res.add(aggregatesSubschema.indexOf(aggregate), max);
            } else if (aggregate.getValue0().equals(ReadQueryBuilder.COUNT) || aggregate.getValue0().equals(ReadQueryBuilder.SUM)) {
                int countOrSum = 0;
                for(List<Object> partialAggregates: partialResultList){
                    countOrSum += (Integer) partialAggregates.get(aggregatesSubschema.indexOf(aggregate));
                }
                res.add(aggregatesSubschema.indexOf(aggregate), countOrSum);
            }
        }

        RelReadQueryResults readQueryResults = new RelReadQueryResults();
        readQueryResults.addData(List.of(new RelRow(res.toArray())));
        return readQueryResults;
    }

    @Override
    public boolean writeSubqueryResults(RelShard shard, String tableName, List<Object> data) {
        List<RelRow> data1 = new ArrayList<>();
        for(Object o: data){
            data1.add((RelRow) o);
        }
        return shard.insertRows(data1) && shard.committRows();
    }

    @Override
    public Map<String, ReadQuery> getSubqueriesResults() {
        return subquery;
    }




    public List<Pair<Integer, String>> getAggregatesSubschema(){
        return aggregatesSubschema;
    }

}
