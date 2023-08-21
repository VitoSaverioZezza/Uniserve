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

public class ProdSelProjQuery implements RetrieveAndCombineQueryPlan<RelShard, RelReadQueryResults> {
    private List<String> tableNames = new ArrayList<>();
    private Map<String, ReadQuery> subqueries = new HashMap<>();
    /**Mapping from table aliases to table names, needed for predicate resolution*/
    private Map<String, String> aliasToTableMap = new HashMap<>();

    /**Schema of the results*/
    private List<String> userFinalSchema = new ArrayList<>();
    /**Names of the attributes that made up the results as they are defined in the sources*/
    private List<String> systemFinalSchema = new ArrayList<>();
    /**Names of the attributes that made up the intermediate schema that includes the attributes needed for the final
     * schema and also for predicate evaluation. The names of these attributes are specified as they are defined
     * in the sources*/
    private List<String> systemCombineSchema = new ArrayList<>();
    /**Cache of the schemas of the sources*/
    private Map<String, List<String>> cachedSourcesSchema = new HashMap<>();
    /**Mapping from source names to names of the attributes that are needed in order to build the combine subschema*/
    private Map<String, List<String>> sourcesSubschemasForCombine = new HashMap<>();
    /**List of names needed for the intermediate schema, already splitted between source part and attribute name*/
    private List<Pair<String, String>> splitSystemCombineSchema = new ArrayList<>();

    private String selectionPredicate = "";

    private Boolean distinct = false;

    public ProdSelProjQuery setAliasToTableMap(Map<String, String> aliasToTableMap) {
        this.aliasToTableMap = aliasToTableMap;
        return this;
    }
    public ProdSelProjQuery setTableNames(List<String> tableNames) {
        this.tableNames = tableNames;
        return this;
    }
    public ProdSelProjQuery setSubqueries(Map<String, ReadQuery> subqueriesResults) {
        this.subqueries = subqueriesResults;
        return this;
    }
    public ProdSelProjQuery setSystemFinalSchema(List<String> systemFinalSchema){
        this.systemFinalSchema = systemFinalSchema;
        return this;
    }
    public ProdSelProjQuery setSystemCombineSchema(List<String> systemCombineSchema){
        this.systemCombineSchema = systemCombineSchema;
        for (String dotAttribute : systemCombineSchema){
            String[] split = dotAttribute.split("\\.");
            StringBuilder attrBuilder = new StringBuilder();
            for(int i = 1; i<split.length-1; i++){
                attrBuilder.append(split[i]);
                attrBuilder.append(".");
            }
            attrBuilder.append(split[split.length-1]);
            splitSystemCombineSchema.add(new Pair<>(split[0], attrBuilder.toString()));
        }
        return this;
    }
    public ProdSelProjQuery setSourcesSubschemasForCombine(Map<String, List<String>> sourcesSubschemasForCombine){
        this.sourcesSubschemasForCombine = sourcesSubschemasForCombine;
        return this;
    }
    public ProdSelProjQuery setCachedSourcesSchema(Map<String, List<String>> cachedSourcesSchema) {
        this.cachedSourcesSchema = cachedSourcesSchema;
        return this;
    }
    public ProdSelProjQuery setUserFinalSchema(List<String> userFinalSchema){
        this.userFinalSchema = userFinalSchema;
        return this;
    }
    public ProdSelProjQuery setSelectionPredicate(String selectionPredicate) {
        this.selectionPredicate = selectionPredicate;
        return this;
    }
    public ProdSelProjQuery setDistinct(){
        this.distinct = true;
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
        List<String> sourceSchema = cachedSourcesSchema.get(tableName);
        List<String> subschema = sourcesSubschemasForCombine.get(tableName);
        if(subschema == null){
            System.out.println("No entry for " + tableName + " in sourcesSubschemasForCombine");
            return Utilities.objectToByteString(new Object[0]);
        }
        List<List<Object>> dataToCombine = new ArrayList<>();
        for(RelRow row: shardData){
            List<Object> projRow = new ArrayList<>();
            for(String projAttr: subschema){
                projRow.add(row.getField(sourceSchema.indexOf(projAttr)));
            }
            /*If the resulting rows of the whole query are extracted from a single table/subquery it is possible to evaluate the predicate
            * here since the row is fully built.
            * Other small optimizations are possible by parsing the predicate at query building time
            * */
            if((tableNames.size()+subqueries.size()) == 1 && !checkPredicate(projRow)){
                continue;
            }
            dataToCombine.add(projRow);
        }
        Object[] projRowArray = dataToCombine.toArray();
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
        List<String> sourcesList = new ArrayList<>(cachedSourcesSchema.keySet());
        List<List<Object>> resultRows = new ArrayList<>();
        Object[] currentPrototype = new Object[systemCombineSchema.size()];
        int currentTableIndex = 0;
        cartesianProduct(currentTableIndex, resultRows, sourcesList, currentPrototype, tableToProjDataMap);
        RelReadQueryResults res = new RelReadQueryResults();
        List<RelRow> dataRows = new ArrayList<>();
        List<List<Object>> resRows = new ArrayList<>();
        for(List<Object> nonProjRow: resultRows) {
            List<Object> projRow = new ArrayList<>(systemFinalSchema.size());
            for(String finalAttribute: systemFinalSchema){
                int index = systemFinalSchema.indexOf(finalAttribute);
                projRow.add(index, nonProjRow.get(systemCombineSchema.indexOf(finalAttribute)));
            }
            if(distinct){
                if (!resRows.contains(projRow)){
                    resRows.add(projRow);
                    dataRows.add(new RelRow(projRow.toArray()));
                }
            }else{
                dataRows.add(new RelRow(projRow.toArray()));
            }
        }
        res.addData(dataRows);
        return res;
    }
    private void cartesianProduct(int currentTableIndex, List<List<Object>> resultRows, List<String> sourcesList, Object[] currentPrototype, Map<String, List<List<Object>>> retrievedData){
        String sourceName = sourcesList.get(currentTableIndex);
        List<String> intermediateProjSchema = sourcesSubschemasForCombine.get(sourceName);
        List<List<Object>> retrievedTableData = retrievedData.get(sourceName);
        for(List<Object> row: retrievedTableData){
            for(int i = 0; i<systemCombineSchema.size(); i++){
                Pair<String, String> intermediateAttributeDescription = splitSystemCombineSchema.get(i);
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

        //TODO: can be done better but it gave me enough trouble, I'll check it later.
        for(Map.Entry<String, String> aliasTableName: aliasToTableMap.entrySet()){
            List<String> sourceSchema = cachedSourcesSchema.get(aliasTableName.getValue());
            for(String attribute: sourceSchema){
                String systemName = aliasTableName.getValue() + "." + attribute;
                String aliasName = aliasTableName.getKey() + "." + attribute;
                if(predToTest.contains(aliasName)){
                    values.put(aliasName, row.get(systemCombineSchema.indexOf(systemName)));
                }
            }
        }
        for(String attrAlias: userFinalSchema){
            if (predToTest.contains(attrAlias)){
                values.put(attrAlias, row.get(systemCombineSchema.indexOf(systemFinalSchema.get(userFinalSchema.indexOf(attrAlias)))));
            }
        }

        for(Pair<String, String> attributePair: splitSystemCombineSchema){
            String attr = attributePair.getValue0()+"."+attributePair.getValue1();
            if(predToTest.contains(attr)){
                values.put(attr, row.get(splitSystemCombineSchema.indexOf(attributePair)));
            }
        }
        for(String finalSchemaName: systemFinalSchema){
            if(predToTest.contains(finalSchemaName)){
                values.put(finalSchemaName, row.get(systemFinalSchema.indexOf(finalSchemaName)));
            }
        }

        JexlEngine jexl = new JexlBuilder().create();
        JexlExpression expression = jexl.createExpression(predToTest);
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

    @Override
    public Map<String, ReadQuery> getSubqueriesResults(){
        return subqueries;
    }
}
