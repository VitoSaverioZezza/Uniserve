package edu.stanford.futuredata.uniserve.relationalapi;

import com.google.protobuf.ByteString;
import edu.stanford.futuredata.uniserve.broker.Broker;
import edu.stanford.futuredata.uniserve.interfaces.RetrieveAndCombineQueryPlan;
import edu.stanford.futuredata.uniserve.relational.RelReadQueryResults;
import edu.stanford.futuredata.uniserve.relational.RelRow;
import edu.stanford.futuredata.uniserve.relational.RelShard;
import edu.stanford.futuredata.uniserve.utilities.Utilities;
import org.javatuples.Pair;

import org.apache.commons.jexl3.*;
import java.util.*;

public class IntermediateQuery implements RetrieveAndCombineQueryPlan<RelShard, RelReadQueryResults> {
    private List<String> tableNames = new ArrayList<>();

    private Map<String, ReadQuery> subqueries = new HashMap<>();

    private List<Pair<String, String>> intermediateSchema;
    private Map<String, List<String>> srcInterProjectSchema;
    private Map<String, List<String>> cachedSourceSchema;

    private String resultName = String.valueOf(new Random().nextInt());

    private String selectionPredicate = null;

    public void setIntermediateSchema(List<Pair<String, String>> intermediateSchema) {
        this.intermediateSchema = intermediateSchema;
    }
    public void setTableNames(List<String> tableNames) {
        this.tableNames = tableNames;
    }
    public void setCachedSourceSchema(Map<String, List<String>> cachedSourceSchema) {
        this.cachedSourceSchema = cachedSourceSchema;
    }
    public void setSrcInterProjectSchema(Map<String, List<String>> srcInterProjectSchema) {
        this.srcInterProjectSchema = srcInterProjectSchema;
    }
    public void setSubqueries(Map<String, ReadQuery> subqueriesResults) {
        this.subqueries = subqueriesResults;
    }
    public void setSelectionPredicate(String selectionPredicate) {
        this.selectionPredicate = selectionPredicate;
    }

    public RelReadQueryResults run(Broker broker){
        RelReadQueryResults res;
        res = broker.retrieveAndCombineReadQuery(this);
        res.setName(resultName);
        List<String> intermediateFieldNames = new ArrayList<>();
        for(Pair<String, String> sourceField: intermediateSchema){
            intermediateFieldNames.add(sourceField.getValue0()+"."+sourceField.getValue1());
        }
        res.setFieldNames(intermediateFieldNames);
        return res;
    }

    @Override
    public List<String> getTableNames() {
        return tableNames;
    }
    @Override
    public Map<String, List<Integer>> keysForQuery() {
        Map<String, List<Integer>> returnedStructure = new HashMap<>();
        for(String tabName: tableNames)
            returnedStructure.put(tabName, List.of(-1));
        return returnedStructure;
    }

    @Override
    public ByteString retrieve(RelShard shard, String tableName) {
        List<RelRow> shardData = shard.getData();
        List<String> sourceSchema = cachedSourceSchema.get(tableName);
        List<String> tablesProjSchema = srcInterProjectSchema.get(tableName);
        List<List<Object>> projData = new ArrayList<>();
        for(RelRow row: shardData){
            List<Object> projRow = new ArrayList<>();
            for(String projAttr: tablesProjSchema){
                projRow.add(row.getField(sourceSchema.indexOf(projAttr)));
            }
            /*If the resulting rows of the whole query are extracted from a single table it is possible to evaluate the predicate
            * here since the row is fully built.
            * Other small optimizations are possible by parsing the predicate at query building time
            * */
            if(tableNames.size() == 1 && !checkPredicate(projRow)){
                continue;
            }
            projData.add(projRow);
        }
        Object[] projRowArray = projData.toArray();
        return Utilities.objectToByteString(projRowArray);
    }

    @Override
    public RelReadQueryResults combine(Map<String, List<ByteString>> retrieveResults) {
        Map<String, List<List<Object>>> tableToProjDataMap = new HashMap<>();
        for(String table: retrieveResults.keySet()){
            tableToProjDataMap.put(table, new ArrayList<>());
            List<ByteString> serProjRowArrays = retrieveResults.get(table);
            for(ByteString dataChunkSer : serProjRowArrays){
                Object[] desDataChunk = (Object[]) Utilities.byteStringToObject(dataChunkSer);
                for(Object o: desDataChunk){
                    List<Object> row = (List<Object>) o;
                    tableToProjDataMap.get(table).add(row);
                }
            }
        }

        //product
        List<String> sourcesList = new ArrayList<>(srcInterProjectSchema.keySet());
        List<List<Object>> resultRows = new ArrayList<>();
        Object[] currentPrototype = new Object[intermediateSchema.size()];
        int currentTableIndex = 0;
        cartesianProduct(currentTableIndex, resultRows, sourcesList, currentPrototype, tableToProjDataMap);
        RelReadQueryResults res = new RelReadQueryResults();
        List<RelRow> dataRows = new ArrayList<>();
        for(List<Object> row: resultRows) {
            dataRows.add(new RelRow(row.toArray()));
        }
        res.addData(dataRows);
        return res;
    }

    private void cartesianProduct(int currentTableIndex, List<List<Object>> resultRows, List<String> sourcesList, Object[] currentPrototype, Map<String, List<List<Object>>> retrievedData){
        String sourceName = sourcesList.get(currentTableIndex);
        List<String> intermediateProjSchema = srcInterProjectSchema.get(sourceName);
        List<List<Object>> retrievedTableData = retrievedData.get(sourceName);
        for(List<Object> row: retrievedTableData){
            for(int i = 0; i<intermediateSchema.size(); i++){
                Pair<String, String> intermediateAttributeDescription = intermediateSchema.get(i);
                if(intermediateAttributeDescription.getValue0().equals(sourceName)){
                    currentPrototype[i] = row.get(intermediateProjSchema.indexOf(intermediateAttributeDescription.getValue1()));
                }
            }
            if(currentTableIndex < sourcesList.size()-1){
                cartesianProduct(++currentTableIndex, resultRows, sourcesList, currentPrototype, retrievedData);
                currentTableIndex = currentTableIndex -1;
            }else{
                List<Object> r = new ArrayList<>(Arrays.asList(currentPrototype));
                if(checkPredicate(r))
                    resultRows.add(r);
            }
        }
    }
    @Override
    public boolean writeSubqueryResults(RelShard shard, String tableName, List<Object> data){
        List<RelRow> data1 = new ArrayList<>();
        for(Object o: data){
            data1.add((RelRow) o);
        }
        return shard.insertRows(data1) && shard.committRows();
    }

    private boolean checkPredicate(List<Object> row){
        if(selectionPredicate == null || selectionPredicate.isEmpty()){
            return true;
        }
        String predToTest = new String(selectionPredicate);

        Map<String, Object> values = new HashMap<>();
        for(Pair<String, String> attributePair: intermediateSchema){
            String attr = attributePair.getValue0()+"."+attributePair.getValue1();
            if(predToTest.contains(attr)){
                values.put(attr, row.get(intermediateSchema.indexOf(attributePair)));
            }
        }

        JexlEngine jexl = new JexlBuilder().create();
        JexlExpression expression = jexl.createExpression(predToTest);
        JexlContext context = new MapContext(values);
        Object result = expression.evaluate(context);
        try{
            if(!(result instanceof Boolean)) return false;
            else return (Boolean) result;
        }catch (Exception e ){
            System.out.println(e.getMessage());
            return false;
        }
    }

    @Override
    public Map<String, ReadQuery> getSubqueriesResults(){
        return subqueries;
    }
}
