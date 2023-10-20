package edu.stanford.futuredata.uniserve.relationalapi;

import com.google.protobuf.ByteString;
import edu.stanford.futuredata.uniserve.interfaces.ReadQueryResults;
import edu.stanford.futuredata.uniserve.interfaces.ShuffleOnReadQueryPlan;
import edu.stanford.futuredata.uniserve.interfaces.SimpleWriteQueryPlan;
import edu.stanford.futuredata.uniserve.relational.RelReadQueryResults;
import edu.stanford.futuredata.uniserve.relational.RelRow;
import edu.stanford.futuredata.uniserve.relational.RelShard;
import edu.stanford.futuredata.uniserve.utilities.Utilities;
import org.apache.commons.jexl3.*;
import org.javatuples.Pair;
import org.mvel2.MVEL;
import org.mvel2.compiler.CompiledExpression;

import java.io.Serializable;
import java.util.*;
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
    private final  Map<String, Serializable> cachedFilterPredicates = new HashMap<>();
    private Map<String, ReadQuery> sourceSubqueries = new HashMap<>(); //map from subquery alias to subquery
    private boolean stored = false;
    private boolean isThisSubquery = false;
    private boolean isDistinct = false; //still necessary, if one source is a subquery without equal rows
    private Map<String, ReadQuery> predicateSubqueries = new HashMap<>();
    private String resultTableName = "";
    private WriteResultsPlan writeResultsPlan = null;
    private final List<Serializable> operations = new ArrayList<>();
    private List<Pair<String, Integer>> predicateVarToIndexesOne = new ArrayList<>();
    private List<Pair<String, Integer>> predicateVarToIndexesTwo = new ArrayList<>();

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
        for(Map.Entry<String,String> p: filterPredicates.entrySet()){
            if(p.getValue() != null && !p.getValue().isEmpty()){
                try {
                    cachedFilterPredicates.put(p.getKey(), MVEL.compileExpression(p.getValue()));
                    this.filterPredicates.put(p.getKey(), p.getValue());
                }catch (Exception e){
                    throw new RuntimeException("Impossible to compile predicate");
                }
            }
            return this;
        }
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
        this.isThisSubquery = isThisSubquery;
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
        Boolean[] keyStructure = new Boolean[resultSchema.size()];
        Arrays.fill(keyStructure, true);
        this.writeResultsPlan = new WriteResultsPlan(resultTableName, keyStructure);
        return this;
    }
    public JoinQuery setOperations(List<SerializablePredicate> operations) {
        this.operations.addAll(operations);
        return this;
    }
    public JoinQuery setPredicateVarToIndexesOne(List<Pair<String, Integer>> predicateVarToIndexes) {
        this.predicateVarToIndexesOne = predicateVarToIndexes;
        return this;
    }
    public JoinQuery setPredicateVarToIndexesTwo(List<Pair<String, Integer>> predicateVarToIndexes) {
        this.predicateVarToIndexesTwo = predicateVarToIndexes;
        return this;
    }

    public List<String> getPredicates(){
        return new ArrayList<>(filterPredicates.values());
    }
    public List<String> getSystemResultSchema() {
        return systemResultSchema;
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
    public WriteResultsPlan getWriteResultPlan() {
        return writeResultsPlan;
    }

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
        //List<RelRow> data = shard.getData();
        //if(data.isEmpty()){
        //    return new HashMap<>();
        //}
        //List<RelRow> filteredData = filter(data, sourceName, concreteSubqueriesResults);
        //if(filteredData.isEmpty()){
        //    return new HashMap<>();
        //}

        //List<RelRow> filteredData; //= filter(shardData, concreteSubqueriesResults);
        //String filterPredicate = filterPredicates.get(sourceName);
        Serializable cachedFilterPredicate = cachedFilterPredicates.get(sourceName);
        List<Pair<String, Integer>> predicateVarToIndexes = null;
        if(sourceName.equals(sourceOne)){
            predicateVarToIndexes = predicateVarToIndexesOne;
        } else if (sourceName.equals(sourceTwo)) {
            predicateVarToIndexes = predicateVarToIndexesTwo;
        }
        //if(!(filterPredicate == null || filterPredicate.isEmpty() || cachedFilterPredicate == null || cachedFilterPredicate.equals(""))){
        //    filteredData = shard.getFilteredData(cachedFilterPredicate, concreteSubqueriesResults, predicateVarToIndexes);
        //}else {
        //    filteredData = shard.getData();
        //}

        List<RelRow> filteredData = new ArrayList<>(
                shard.getData(
                        false,
                        false,
                        null,
                        cachedFilterPredicate,
                        concreteSubqueriesResults,
                        predicateVarToIndexes,
                        null
                )
        );

        Map<Integer, List<ByteString>> returnedAssignment = new HashMap<>();
        List<String> joinAttributes = sourcesJoinAttributes.get(sourceName);
        List<String> sourceSchema = sourceSchemas.get(sourceName);

        for(RelRow row: filteredData){
            int key = 0;
            for(String joinAttribute: joinAttributes){
                Object val = row.getField(sourceSchema.indexOf(joinAttribute));
                if(val == null) {
                    key += 1;
                }else{
                    if(val instanceof Number) {
                        val = ((Number) val).doubleValue();
                    }
                    key += val.hashCode();
                    if(key<0){
                        key = key * -1;
                    }
                }
            }
            if(numRepartitions == 0){
                numRepartitions += 1;
            }
            key = key % numRepartitions;
            returnedAssignment.computeIfAbsent(key, k->new ArrayList<>()).add(Utilities.objectToByteString(row));
        }
        return returnedAssignment;
    }
    @Override
    public ByteString gather(Map<String, List<ByteString>> ephemeralData, Map<String, RelShard> ephemeralShards) {
        //List<RelRow> rowsSourceOne = new ArrayList<>();
        //List<RelRow> rowsSourceTwo = new ArrayList<>();
        //if(ephemeralData.get(sourceOne) != null)
        //    rowsSourceOne = ephemeralData.get(sourceOne).stream().map(v -> (RelRow)Utilities.byteStringToObject(v)).collect(Collectors.toList());
        //if(ephemeralData.get(sourceTwo) != null)
        //    rowsSourceTwo = ephemeralData.get(sourceTwo).stream().map(v -> (RelRow)Utilities.byteStringToObject(v)).collect(Collectors.toList());
        List<RelRow> rowsSourceOne = ephemeralShards.get(sourceOne).getData();
        List<RelRow> rowsSourceTwo = ephemeralShards.get(sourceTwo).getData();
        if(rowsSourceOne == null || rowsSourceOne.isEmpty() || rowsSourceTwo == null || rowsSourceTwo.isEmpty())
            return  Utilities.objectToByteString(new ArrayList<>());
        //if(rowsSourceOne.isEmpty() || rowsSourceTwo.isEmpty()){
        //    return Utilities.objectToByteString(new ArrayList<>());
        //}
        List<String> schemaSourceOne = sourceSchemas.get(sourceOne);
        List<String> schemaSourceTwo = sourceSchemas.get(sourceTwo);
        List<String> joinAttributesOne = sourcesJoinAttributes.get(sourceOne);
        List<String> joinAttributesTwo = sourcesJoinAttributes.get(sourceTwo);
        ArrayList<RelRow> joinedRows = new ArrayList<>();
        for(RelRow rowOne: rowsSourceOne){
            for(RelRow rowTwo: rowsSourceTwo){
                boolean matching = true;

                for(int i = 0; i < joinAttributesOne.size(); i++){
                    Object rowOneVal = rowOne.getField(schemaSourceOne.indexOf(joinAttributesOne.get(i)));
                    Object rowTwoVal = rowTwo.getField(schemaSourceTwo.indexOf(joinAttributesTwo.get(i)));
                    if(rowOneVal == null && rowTwoVal == null){

                    } else if (rowOneVal == null || rowTwoVal == null) {
                        matching = false;
                        break;
                    }else if(!rowOneVal.equals(rowTwoVal)) {
                        if(rowOneVal instanceof Number && rowTwoVal instanceof Number &&
                                (((Number) rowOneVal).doubleValue() == ((Number) rowTwoVal).doubleValue())){
                        }else {
                            matching = false;
                            break;
                        }
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
                    if(operations.isEmpty()) {
                        joinedRows.add(new RelRow(rawNewRow.toArray()));
                    }else{
                        joinedRows.add(applyOperations(new RelRow(rawNewRow.toArray())));
                    }
                }
            }
        }
        ArrayList<RelRow> res;
        res = checkDistinct(joinedRows);
        return Utilities.objectToByteString(res);
    }
    @Override
    public boolean writeIntermediateShard(RelShard intermediateShard, ByteString gatherResults){
        List<RelRow> rows = (List<RelRow>) Utilities.byteStringToObject(gatherResults);
        return intermediateShard.insertRows(rows) && intermediateShard.committRows();
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
        }else{
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
        ArrayList<RelRow> nonDuplicateRows = new ArrayList<>();
        for(int i = 0; i < data.size(); i++){
            List<RelRow> sublist = data.subList(i+1, data.size());
            if(!sublist.contains(data.get(i))){
                nonDuplicateRows.add(data.get(i));
            }
        }
        return nonDuplicateRows;
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
        String filterPredicate = filterPredicates.get(sourceName);

        for(Map.Entry<String, RelReadQueryResults> subqRes: subqueriesResults.entrySet()){
            if(filterPredicate.contains(subqRes.getKey())){
                values.put(subqRes.getKey(), subqRes.getValue().getData().get(0).getField(0));
            }
        }
        /*
        for (String attributeName : sourceSchema) {
            Object val = row.getField(sourceSchema.indexOf(attributeName));
            String systemName = sourceName + "." + attributeName;
            if (filterPredicate.contains(systemName)) {
                filterPredicate = filterPredicate.replace(systemName, "'"+systemName+"'");
                values.put("'"+systemName+"'", val);
            }
            if (filterPredicate.contains(attributeName)) {
                values.put(attributeName, val);
            }
        }
        */


        if(sourceName.equals(sourceOne)) {
            for (Pair<String, Integer> nameToVar : predicateVarToIndexesOne) {
                Object val = row.getField(nameToVar.getValue1());
                if (val == null) {
                    return false;
                } else {
                    values.put(nameToVar.getValue0(), val);
                }
            }
        }else{
            for (Pair<String, Integer> nameToVar : predicateVarToIndexesTwo) {
                Object val = row.getField(nameToVar.getValue1());
                if (val == null) {
                    return false;
                } else {
                    values.put(nameToVar.getValue0(), val);
                }
            }
        }
        if(values.containsValue(null)){
            return false;
        }
        try{
            /*

            JexlEngine jexl = new JexlBuilder().create();
            JexlExpression expression = jexl.createExpression(filterPredicate);
            JexlContext context = new MapContext(values);
            Object result = expression.evaluate(context);
            if(!(result instanceof Boolean))
                return false;
            else {
                return (Boolean) result;
            }

            */

            //Serializable compiled = MVEL.compileExpression(filterPredicate);
            Serializable compiled = cachedFilterPredicates.get(sourceName);
            Object result = MVEL.executeExpression(compiled, values);
            if(!(result instanceof Boolean))
                return false;
            else
                return (Boolean) result;

        }catch (Exception e ){
            System.out.println("JQ.filter ----- "+e.getMessage());
            return false;
        }
    }
    private RelRow applyOperations(RelRow inputRow){
        List<Object> newRow = new ArrayList<>();
        for(int i = 0; i<inputRow.getSize(); i++){
            newRow.add(applyOperation(inputRow.getField(i), operations.get(i)));
        }
        return new RelRow(newRow.toArray());
    }

    private Object applyOperation(Object o, Serializable pred){
        SerializablePredicate predicate = (SerializablePredicate) pred;
        return predicate.run(o);
    }
}
