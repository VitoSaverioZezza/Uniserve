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

    private Map<String, String> filterPredicates;
    private String selectionPredicate = "";

    private Boolean distinct = false;

    public ProdSelProjQuery setFilterPredicates(Map<String, String> filterPredicates){
        this.filterPredicates = filterPredicates;
        return this;
    }
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
    public Map<String, ReadQuery> getSubqueriesResults(){
        return subqueries;
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
            if(filterPredicates.containsKey(tableName) && !checkFilterPredicate(filterPredicates.get(tableName), row, tableName)){
                continue;
            }
            List<Object> projRow = new ArrayList<>();
            for(String projAttr: subschema){
                projRow.add(row.getField(sourceSchema.indexOf(projAttr)));
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
        List<String> sourcesList = new ArrayList<>(cachedSourcesSchema.keySet());
        List<List<Object>> resultRows = new ArrayList<>();
        Object[] currentPrototype = new Object[systemCombineSchema.size()];
        int currentTableIndex = 0;
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
    @Override
    public boolean writeSubqueryResults(RelShard shard, String tableName, List<Object> data){
        List<RelRow> data1 = new ArrayList<>();
        for(Object o: data){
            data1.add((RelRow) o);
        }
        return shard.insertRows(data1) && shard.committRows();
    }

    private boolean checkFilterPredicate(String filterPredicate, RelRow row, String tableName){
        Map<String, Object> values = new HashMap<>();
        String tableAlias = "";
        List<String> tableSchema = cachedSourcesSchema.get(tableName);
        for(Map.Entry<String, String> aliasTable: aliasToTableMap.entrySet()){
            if(aliasTable.getValue().equals(tableName)){
                tableAlias = aliasTable.getKey();
                break;
            }
        }
        for(String attributeName: tableSchema){
            Object val = row.getField(tableSchema.indexOf(attributeName));
            String systemName = tableName+"."+attributeName;
            if(filterPredicate.contains(systemName)){
                values.put(systemName, val);
            }
            if(!tableAlias.isEmpty() && filterPredicate.contains(tableAlias)){
                String aliasAttr = tableAlias+"."+attributeName;
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
    public List<String> getSystemFinalSchema(){return systemFinalSchema;}
    public String getSelectionPredicate(){return selectionPredicate;}
}
