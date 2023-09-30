package edu.stanford.futuredata.uniserve.relationalapi;

import com.google.protobuf.ByteString;
import edu.stanford.futuredata.uniserve.interfaces.ReadQueryResults;
import edu.stanford.futuredata.uniserve.interfaces.ShuffleOnReadQueryPlan;
import edu.stanford.futuredata.uniserve.relational.RelReadQueryResults;
import edu.stanford.futuredata.uniserve.relational.RelRow;
import edu.stanford.futuredata.uniserve.relational.RelShard;
import edu.stanford.futuredata.uniserve.utilities.Utilities;
import org.apache.commons.jexl3.*;
import org.javatuples.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class JoinQuery implements ShuffleOnReadQueryPlan<RelShard, RelReadQueryResults> {
    private String sourceOne = ""; //either the name of the table or the alias of the subquery
    private String sourceTwo = "";
    private boolean sourceOneTable = true; //true if the source is a table
    private boolean sourceTwoTable = true;
    private Map<String, List<String>> sourceSchemas = new HashMap<>(); //schemas of both sources
    private List<String> resultSchema = new ArrayList<>(); //user final schema
    private List<String> systemResultSchema = new ArrayList<>(); //system final schema, this is in dotted notation!
    private Map<String, List<String>> sourcesJoinAttributes = new HashMap<>();
    private Map<String, String> filterPredicates = new HashMap<>(); //filters for both sources
    private Map<String, ReadQuery> sourceSubqueries = new HashMap<>(); //map from subquery alias to subquery
    private boolean stored = false;
    private boolean isThisSubquery = false;
    private boolean isDistinct = false; //still necessary, if one source is a subquery without equal rows
    private Map<String, ReadQuery> predicateSubqueries = new HashMap<>();
    private String resultTableName = "";

    public JoinQuery setSourceOne(String sourceOne) {
        this.sourceOne = sourceOne;
        return this;
    }
    public JoinQuery setSourceTwo(String sourceTwo) {
        this.sourceTwo = sourceTwo;
        return this;
    }
    public JoinQuery setTableFlags(boolean isSourceOneTable, boolean isSourceTwoTable){
        sourceOneTable = isSourceOneTable;
        sourceTwoTable = isSourceTwoTable;
        return this;
    }
    public JoinQuery setSourceSchemas(Map<String, List<String>> sourceSchemas) {
        this.sourceSchemas = sourceSchemas;
        return this;
    }
    public JoinQuery setResultSchema(List<String> resultSchema) {
        this.resultSchema = resultSchema;
        return this;
    }
    public JoinQuery setSystemResultSchema(List<String> systemResultSchema) {
        this.systemResultSchema = systemResultSchema;
        return this;
    }
    public JoinQuery setSourcesJoinAttributes(Map<String, List<String>> sourcesJoinAttributes) {
        this.sourcesJoinAttributes = sourcesJoinAttributes;
        return this;
    }
    public JoinQuery setFilterPredicates(Map<String, String> filterPredicates) {
        this.filterPredicates = filterPredicates;
        return this;
    }
    public JoinQuery setSourceSubqueries(Map<String, ReadQuery> sourceSubqueries) {
        this.sourceSubqueries = sourceSubqueries;
        return this;
    }
    public JoinQuery setStored(){
        this.stored = true;
        return this;
    }
    public JoinQuery setIsThisSubquery(boolean isThisSubquery){
        this.isThisSubquery = true;
        return this;
    }
    public JoinQuery setDistinct(){
        this.isDistinct = true;
        return this;
    }
    public JoinQuery setPredicateSubqueries(Map<String, ReadQuery> predicateSubqueries){
        this.predicateSubqueries = predicateSubqueries;
        return this;
    }
    public JoinQuery setResultTableName(String resultTableName){
        this.resultTableName = resultTableName;
        return this;
    }

    public List<String> getPredicates(){
        return new ArrayList<>(filterPredicates.values());
    }
    public boolean isStored(){
        return stored;
    }
    public boolean isThisSubquery() {
        return isThisSubquery;
    }
    public Map<String, ReadQuery> getVolatileSubqueries(){return sourceSubqueries;}
    public Map<String, ReadQuery> getConcreteSubqueries(){return predicateSubqueries;}
    public String getResultTableName(){return resultTableName;}

    @Override
    public List<String> getQueriedTables() {
        List<String> returned = new ArrayList<>();
        if(sourceTwoTable)
            returned.add(sourceTwo);
        if(sourceOneTable)
            returned.add(sourceOne);
        return returned;
    }
    @Override
    public Map<String, List<Integer>> keysForQuery() {
        Map<String, List<Integer>> returned = new HashMap<>();
        if(sourceTwoTable)
            returned.put(sourceTwo, List.of(-1));
        if(sourceOneTable)
            returned.put(sourceOne, List.of(-1));
        return returned;
    }

    @Override
    public Map<Integer, List<ByteString>> scatter(RelShard shard, int numRepartitions, String sourceName, Map<String, ReadQueryResults> concreteSubqueriesResults) {
        List<RelRow> data = shard.getData();
        List<RelRow> filteredData = filter(data, sourceName, concreteSubqueriesResults);

        Map<Integer, List<ByteString>> returnedAssignment = new HashMap<>();
        List<String> joinAttributes = sourcesJoinAttributes.get(sourceName);
        List<String> sourceSchema = sourceSchemas.get(sourceName);
        for(RelRow row: filteredData){
            int key = 0;
            for(String joinAttribute: joinAttributes){
                key += row.getField(sourceSchema.indexOf(joinAttribute)).hashCode();
            }
            key = key % numRepartitions;
            returnedAssignment.computeIfAbsent(key, k->new ArrayList<>()).add(Utilities.objectToByteString(row));
        }
        return returnedAssignment;
    }
    @Override
    public ByteString gather(Map<String, List<ByteString>> ephemeralData, Map<String, RelShard> ephemeralShards) {
        List<RelRow> rowsSourceOne = new ArrayList<>();
        List<RelRow> rowsSourceTwo = new ArrayList<>();
        if(ephemeralData.get(sourceOne) != null)
            rowsSourceOne = ephemeralData.get(sourceOne).stream().map(v -> (RelRow)Utilities.byteStringToObject(v)).collect(Collectors.toList());
        if(ephemeralData.get(sourceTwo) != null)
            rowsSourceTwo = ephemeralData.get(sourceTwo).stream().map(v -> (RelRow)Utilities.byteStringToObject(v)).collect(Collectors.toList());
        if(rowsSourceOne.isEmpty() || rowsSourceTwo.isEmpty()){
            return Utilities.objectToByteString(new ArrayList<>());
        }
        List<String> schemaSourceOne = sourceSchemas.get(sourceOne);
        List<String> schemaSourceTwo = sourceSchemas.get(sourceTwo);
        List<String> joinAttributesOne = sourcesJoinAttributes.get(sourceOne);
        List<String> joinAttributesTwo = sourcesJoinAttributes.get(sourceTwo);
        ArrayList<RelRow> joinedRows = new ArrayList<>();


        for(RelRow rowOne: rowsSourceOne){
            for(RelRow rowTwo: rowsSourceTwo){
                boolean matching = true;

                for(int i = 0; i < joinAttributesOne.size(); i++){
                    if(!   (rowOne.getField(schemaSourceOne.indexOf(joinAttributesOne.get(i))).equals(
                            rowTwo.getField(schemaSourceTwo.indexOf(joinAttributesTwo.get(i))))))
                    {
                        matching = false;
                        break;
                    }
                }

                if(matching){
                    List<Object> rawNewRow = new ArrayList<>(systemResultSchema.size());
                    for(String systemAttribute: systemResultSchema) {
                        String[] split = systemAttribute.split("\\.");
                        String source = split[0];
                        StringBuilder stringBuilder = new StringBuilder();
                        for (int j = 1; j < split.length - 1; j++) {
                            stringBuilder.append(split[j]);
                            stringBuilder.append(".");
                        }
                        stringBuilder.append(split[split.length - 1]);
                        String attribute = stringBuilder.toString();

                        if (source.equals(sourceOne)) {
                            rawNewRow.add(systemResultSchema.indexOf(systemAttribute),
                                    rowOne.getField(schemaSourceOne.indexOf(attribute))
                            );
                        } else {
                            rawNewRow.add(systemResultSchema.indexOf(systemAttribute),
                                    rowTwo.getField(schemaSourceTwo.indexOf(attribute))
                            );
                        }
                    }
                    joinedRows.add(new RelRow(rawNewRow.toArray()));
                }
            }
        }
        ArrayList<RelRow> res;
        if(isDistinct)
            res = checkDistinct(joinedRows);
        else
            res = joinedRows;
        return Utilities.objectToByteString(res);
    }
    @Override
    public void writeIntermediateShard(RelShard intermediateShard, ByteString gatherResults){
        List<RelRow> rows = (List<RelRow>) Utilities.byteStringToObject(gatherResults);
        intermediateShard.insertRows(rows);
        intermediateShard.committRows();
    }
    @Override
    public RelReadQueryResults combine(List<ByteString> shardQueryResults) {
        RelReadQueryResults results = new RelReadQueryResults();
        results.setFieldNames(resultSchema);
        if(isThisSubquery){
            List<Map<Integer, Integer>> desIntermediateShardLocations = shardQueryResults.stream().map(v->(Map<Integer,Integer>)Utilities.byteStringToObject(v)).collect(Collectors.toList());
            List<Pair<Integer, Integer>> ret = new ArrayList<>();
            for(Map<Integer,Integer> intermediateShardsLocation: desIntermediateShardLocations){
                for(Map.Entry<Integer, Integer> shardLocation: intermediateShardsLocation.entrySet()){
                    ret.add(new Pair<>(shardLocation.getKey(), shardLocation.getValue()));
                }
            }
            results.setIntermediateLocations(ret);
            //List of Map<ShardID, dsID> to be converted in what is in the RelReadQueryResults
        }else {
            List<RelRow> ret = new ArrayList<>();
            for(ByteString serSubset: shardQueryResults){
                ret.addAll((List<RelRow>)Utilities.byteStringToObject(serSubset));
            }
            results.addData(ret);
            //List of RelRows lsis to be merged together in the structure
        }
        return results;
    }


    private ArrayList<RelRow> checkDistinct(ArrayList<RelRow> data){
        if(!isDistinct){
            return data;
        }
        List<RelRow> toBeRemoved = new ArrayList<>();
        for(int i = 0; i< data.size()-1; i++){
            RelRow r1 = data.get(i);
            for(int j = i+1; j< data.size(); j++){
                RelRow r2 = data.get(j);
                for(int k = 0; k<r1.getSize(); k++){
                    if(!r1.getField(k).equals(r2.getField(k))){
                        break;
                    }
                    toBeRemoved.add(r2);
                }
            }
        }
        data.removeAll(toBeRemoved);
        return data;
    }
    private List<RelRow> filter(List<RelRow> data, String sourceName, Map<String, ReadQueryResults> subqueriesResults){
        String predicate = filterPredicates.get(sourceName);
        if(predicate == null || predicate.isEmpty()){
            return data;
        }
        Map<String, RelReadQueryResults> relsubqRes = new HashMap<>();
        for(Map.Entry<String, ReadQueryResults> entry: subqueriesResults.entrySet()){
            relsubqRes.put(entry.getKey(), (RelReadQueryResults) entry.getValue());
        }
        ArrayList<RelRow> results = new ArrayList<>();
        for (RelRow row: data){
            if(evaluatePredicate(row, sourceName, relsubqRes)){
                results.add(row);
            }
        }
        return results;
    }
    private boolean evaluatePredicate(RelRow row, String sourceName, Map<String, RelReadQueryResults> subqueriesResults){
        Map<String, Object> values = new HashMap<>();
        List<String> sourceSchema = sourceSchemas.get(sourceName);
        String sourceAlias = "";
        String filterPredicate = filterPredicates.get(sourceName);

        for(Map.Entry<String, RelReadQueryResults> subqRes: subqueriesResults.entrySet()){
            if(filterPredicate.contains(subqRes.getKey())){
                values.put(subqRes.getKey(), subqRes.getValue().getData().get(0).getField(0));
            }
        }
        for(String attributeName: sourceSchema){
            Object val = row.getField(sourceSchema.indexOf(attributeName));
            String systemName = sourceName+"."+attributeName;
            if(filterPredicate.contains(systemName)){
                values.put(systemName, val);
            }
            if(!sourceAlias.isEmpty() && filterPredicate.contains(sourceAlias)){
                String aliasAttr = sourceAlias+"."+attributeName;
                if(filterPredicate.contains(aliasAttr)){
                    values.put(aliasAttr, val);
                }
            }
            if(filterPredicate.contains(attributeName)){
                values.put(attributeName, val);
            }
        }

        JexlEngine jexl = new JexlBuilder().create();
        JexlExpression expression = jexl.createExpression(filterPredicate);
        JexlContext context = new MapContext(values);
        Object result = expression.evaluate(context);
        try{
            if(!(result instanceof Boolean))
                return false;
            else {
                return (Boolean) result;
            }
        }catch (Exception e ){
            System.out.println(e.getMessage());
            return false;
        }
    }
}
