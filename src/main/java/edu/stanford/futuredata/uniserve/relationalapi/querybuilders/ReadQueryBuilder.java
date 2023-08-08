package edu.stanford.futuredata.uniserve.relationalapi.querybuilders;

import edu.stanford.futuredata.uniserve.broker.Broker;
import edu.stanford.futuredata.uniserve.relational.RelReadQueryResults;
import edu.stanford.futuredata.uniserve.relationalapi.ReadQuery;
import org.javatuples.Pair;

import java.util.*;

public class ReadQueryBuilder {
    public static final Integer AVG = 1;
    public static final Integer MIN = 2;
    public static final Integer MAX = 3;
    public static final Integer COUNT = 4;

    private final Broker broker;

    private List<String> rawSelectedFields = new ArrayList<>();
    private String[] fieldsAliases = null;

    private final List<Pair<Integer, String>> aggregates = new ArrayList<>();
    private String rawSelectionPredicate = null;

    private String[] rawGroupFields = null;
    private String rawHavingPredicate = null;
    private boolean distinct = false;
    private boolean isStored = false;

    private Map<String, String> tableNameToAlias = new HashMap<>(); //contains ALL tables, values for tableNames without aliases are set to null;
    private Map<String, String> aliasToTableName = new HashMap<>(); //contains ONLY aliases of the tables
    private Map<String, RelReadQueryResults> subqueriesAlias = new HashMap<>(); //contains ALL subqueries mappings

    private List<String> intermediateSchema = new ArrayList<>(); //ALL fields needed for the intermediate schema in dot notation
    private Map<String, List<String>> sourceToSelectArguments = new HashMap<>(); //ALL fields selected, without aggregates and without attributes needed for predicates
    private Map<String, List<Pair<Integer, String>>> sourceToAggregates = new HashMap<>(); //fields to be aggregate
    private Map<String, List<String>> sourceToSelectionPredicateArguments = new HashMap<>(); //attributes needed for predicate evaluation

    private Map<String, List<String>> cachedSourceSchemas = new HashMap<>(); //cache of source name (table or alias for subquery) and their schema


    public ReadQueryBuilder(Broker broker){
        this.broker = broker;
    }

    //ASSUMPTION: ALL ARGUMENTS USE DOT NOTATION WITH TABLENAMES OR ALIASES
    public ReadQueryBuilder select(){return this;}
    public ReadQueryBuilder select(String... selectedFields){
        this.rawSelectedFields.addAll(Arrays.asList(selectedFields));
        return this;
    }
    public ReadQueryBuilder alias(String... aliases){
        fieldsAliases = aliases;
        if(!noDuplicateInStringArray(aliases))
            throw new RuntimeException("Error in parameter alias definition: Multiple projection args have the same alias");
        if(fieldsAliases.length != rawSelectedFields.size()){
            throw new RuntimeException("Wrong number of aliases provided for selected fields");
        }
        return this;
    }
    public ReadQueryBuilder avg(String aggregatedField){
        this.aggregates.add(new Pair<>(AVG, aggregatedField));
        return this;
    }
    public ReadQueryBuilder min(String aggregatedField){
        this.aggregates.add(new Pair<>(MIN, aggregatedField));
        return this;
    }
    public ReadQueryBuilder max(String aggregatedField){
        this.aggregates.add(new Pair<>(MAX, aggregatedField));
        return this;
    }
    public ReadQueryBuilder count(String aggregatedField){
        this.aggregates.add(new Pair<>(COUNT, aggregatedField));
        return this;
    }

    public ReadQueryBuilder from(String tableName ){
        tableNameToAlias.put(tableName, null);
        return this;
    }
    public ReadQueryBuilder from(String tableName, String alias){
        tableNameToAlias.put(tableName, alias);
        aliasToTableName.put(alias, tableName);
        return this;
    }
    public ReadQueryBuilder from(ReadQuery subquery, String alias){
        if(alias == null){
            throw new RuntimeException("Subquery has not alias");
        }
        subqueriesAlias.put(alias, subquery.run());
        return this;
    }
    public ReadQueryBuilder from(ReadQueryBuilder subqueryToBuild, String alias){
        return from(subqueryToBuild.build(), alias);
    }

    public ReadQueryBuilder where(String selectionPredicate){
        this.rawSelectionPredicate = selectionPredicate;
        return this;
    }
    public ReadQueryBuilder group(String... groupFields){
        if(!noDuplicateInStringArray(groupFields)){
            throw new RuntimeException("Error: Duplicate elements in group clause");
        }
        this.rawGroupFields = groupFields;
        return this;
    }
    public ReadQueryBuilder having(String havingPredicate){
        this.rawHavingPredicate = havingPredicate;
        return this;
    }
    public ReadQueryBuilder distinct(){
        this.distinct = true;
        return this;
    }
    public ReadQueryBuilder store(){
        this.isStored = true;
        return this;
    }


    public ReadQuery build(){
        buildIntermediateSchemaAlternative();
        //(2) build result schema -> parse select arguments and aggregates, then find a correspondence between the elements
        //result schema and the source schema.
        //(2) solve select elements

        //(4) define queries
        //(4.) predicates, operations on tables with push of projection and selection
        return null;
    }

    /*
    //TODO: this is very, very bad...
    private void buildIntermediateSchema(){
        Map<String, List<Integer>> sourceSelectedIndexes = new HashMap<>(); //no aggregates, no needed intermediates
        //only arguments inside the select clause. The intermediate schema also needs predicate arguments and
        // arguments of aggregate functions
        if(rawSelectedFields.isEmpty()){
            selectAll();
            return;
        }

        Map<String, List<String>> splitSelected = new HashMap<>();
        List<String> notDotAttributeNames = new ArrayList<>();
        Map<String, List<Integer>> tableToReqAttributeIndexes = new HashMap<>();
        for(String rawSelectValue: rawSelectedFields){
            String[] split = rawSelectValue.split("\\.");
            if(split.length != 2){
                notDotAttributeNames.add(rawSelectValue);
            }else {
                splitSelected.computeIfAbsent(split[0],k->new ArrayList<>()).add(split[1]);
            }
        }
        for(Map.Entry<String, String> tableNameAlias : tableAliases.entrySet()){
            String tableName = tableNameAlias.getKey();
            List<String> sourceSchema = broker.getTableInfo(tableName).getAttributeNames();
            if(splitSelected.containsKey(tableName) || splitSelected.containsKey(tableNameAlias.getValue())){
                List<String> requestedAttributes = splitSelected.get(tableName);
                requestedAttributes.addAll(splitSelected.get(tableNameAlias.getValue()));
                for(String reqAttribute: requestedAttributes){
                    int index = sourceSchema.indexOf(reqAttribute);
                    if(index == -1){
                        throw new RuntimeException("Requested attribute " + reqAttribute + " not defined for table " + tableName);
                    }
                    tableToReqAttributeIndexes.computeIfAbsent(tableName, k->new ArrayList<>()).add(index);
                }
            }
            for(String notDotAttr: notDotAttributeNames){
                int index = sourceSchema.indexOf(notDotAttr);
                if(index != -1){
                    tableToReqAttributeIndexes.computeIfAbsent(tableName, k->new ArrayList<>()).add(index);
                    notDotAttributeNames.remove(notDotAttr);
                }
            }
        }
        for(Map.Entry<String, ReadQueryResults> aliasToResultMapEntry: subqueriesAlias.entrySet()){
            String aliasName = aliasToResultMapEntry.getKey();
            List<String> sourceSchema = aliasToResultMapEntry.getValue().getFieldNames();
            if(splitSelected.containsKey(aliasName)){
                List<String> requestedAttributes = splitSelected.get(aliasName);
                for(String reqAttribute: requestedAttributes){
                    int index = sourceSchema.indexOf(reqAttribute);
                    if(index == -1){
                        throw new RuntimeException("Requested attribute " + reqAttribute + " not defined for subquery " + aliasName);
                    }
                    tableToReqAttributeIndexes.computeIfAbsent(aliasName, k->new ArrayList<>()).add(index);
                }
            }
            for(String notDotAttr: notDotAttributeNames){
                int index = sourceSchema.indexOf(notDotAttr);
                if(index != -1){
                    tableToReqAttributeIndexes.computeIfAbsent(aliasName, k->new ArrayList<>()).add(index);
                    notDotAttributeNames.remove(notDotAttr);
                }
            }
        }

        Map<String, List<Integer>> wherePreddicateAttributes = new HashMap<>();
        if(rawSelectionPredicate!=null) {
            String[] splitRawSelectionArray = rawSelectionPredicate.split(" ");
            StringBuilder noSpaceBuilder = new StringBuilder();
            for (String s : splitRawSelectionArray) {
                noSpaceBuilder.append(s);
            }
            String noSpacePredicate = noSpaceBuilder.toString();
            //Only find the names of the values to be retrieved, then use the JavaScript
            for (Map.Entry<String, String> tableNameAlias : tableAliases.entrySet()) {
                // (table.name == "string" AND attribute != 23) OR table1.attribute < table2.atr) AND attribute < 12)
                List<String> tableSchema = broker.getTableInfo(tableNameAlias.getKey()).getAttributeNames();
                for (String attrName : tableSchema) {
                    if (noSpacePredicate.matches(tableNameAlias.getKey() + "\\." + attrName)) {
                        noSpacePredicate = noSpacePredicate.replace(tableNameAlias.getKey() + "\\." + attrName, "");
                        wherePreddicateAttributes.computeIfAbsent(tableNameAlias.getKey(), k -> new ArrayList<>()).add(tableSchema.indexOf(attrName));
                    }
                    if (noSpacePredicate.matches(tableNameAlias.getValue() + "\\." + attrName)) {
                        noSpacePredicate = noSpacePredicate.replace(tableNameAlias.getKey() + "\\." + attrName, "");
                        wherePreddicateAttributes.computeIfAbsent(tableNameAlias.getKey(), k -> new ArrayList<>()).add(tableSchema.indexOf(attrName));
                    }
                }
            }
            for (Map.Entry<String, ReadQueryResults> aliasToRQR : subqueriesAlias.entrySet()) {
                List<String> subquerySchema = aliasToRQR.getValue().getFieldNames();
                for (String attrName : subquerySchema) {
                    if (noSpacePredicate.matches(aliasToRQR.getKey() + "\\." + attrName)) {
                        noSpacePredicate = noSpacePredicate.replace(aliasToRQR.getKey() + "\\." + attrName, "");
                        wherePreddicateAttributes.computeIfAbsent(aliasToRQR.getKey(), k -> new ArrayList<>()).add(subquerySchema.indexOf(attrName));
                    }
                }
            }
        }

        Map<String, List<Integer>> tableToAggregatedFieldIndexes = new HashMap<>();
        Map<String,List<String>> rawAggregatedAttributes = new HashMap<>();
        for(Pair<Integer, String> aggregate: aggregates){
            String[] split = aggregate.getValue1().split("\\.");
            rawAggregatedAttributes.computeIfAbsent(split[0],k->new ArrayList<>()).add(split[1]);
        }

        for(Map.Entry<String, String> tableNameAlias: tableAliases.entrySet()){
            List<String> aggregatedAttributes = rawAggregatedAttributes.get(tableNameAlias.getKey());
            aggregatedAttributes.addAll(rawAggregatedAttributes.get(tableNameAlias.getValue()));
            if(!aggregatedAttributes.isEmpty()){
                List<String> tableSchema = broker.getTableInfo(tableNameAlias.getKey()).getAttributeNames();
                List<Integer> aggregateIndexes = new ArrayList<>();
                for(String s: aggregatedAttributes){
                    aggregateIndexes.add(tableSchema.indexOf(s));
                }
                if(aggregateIndexes.contains(-1)){
                    throw new RuntimeException("Attribute to be aggregate is not an attribute of given table");
                }
                tableToAggregatedFieldIndexes.put(tableNameAlias.getKey(), aggregateIndexes);
            }
        }
        for(Map.Entry<String, ReadQueryResults> aliasSubq : subqueriesAlias.entrySet()){
            List<String> aggregatedAttributes = rawAggregatedAttributes.get(aliasSubq.getKey());
            if(!aggregatedAttributes.isEmpty()){
                List<String> tableSchema = aliasSubq.getValue().getFieldNames();
                List<Integer> aggregateIndexes = new ArrayList<>();
                for(String s: aggregatedAttributes){
                    aggregateIndexes.add(tableSchema.indexOf(s));
                }
                if(aggregateIndexes.contains(-1)){
                    throw new RuntimeException("Attribute to be aggregate is not an attribute of given subquery");
                }
                tableToAggregatedFieldIndexes.put(aliasSubq.getKey(), aggregateIndexes);
            }
        }
    }
*/
    private void buildIntermediateSchemaAlternative(){
        if(rawSelectedFields.isEmpty()){
            for(String tableName: tableNameToAlias.keySet()){
                List<String> tableSchema = broker.getTableInfo(tableName).getAttributeNames();
                cachedSourceSchemas.put(tableName, tableSchema);
                sourceToSelectArguments.put(tableName, tableSchema);
            }
            for(String subqueryAlias: subqueriesAlias.keySet()){
                List<String> subquerySchema = subqueriesAlias.get(subqueryAlias).getFieldNames();
                cachedSourceSchemas.put(subqueryAlias, subquerySchema);
                sourceToSelectArguments.put(subqueryAlias, subquerySchema);
            }
        }else{
            for (String selectAttribute : rawSelectedFields) {
                String[] split = selectAttribute.split("\\.");
                String sourceName = split[0];
                String attributeName = split[1];
                List<String> sourceSchema = cachedSourceSchemas.get(sourceName);
                String solvedSourceName;
                if (sourceSchema == null) {
                    if (tableNameToAlias.containsKey(sourceName)) {
                        sourceSchema = broker.getTableInfo(sourceName).getAttributeNames();
                        solvedSourceName = sourceName;
                        cachedSourceSchemas.put(solvedSourceName, sourceSchema);
                    } else if (aliasToTableName.containsKey(sourceName)) {
                        sourceSchema = broker.getTableInfo(aliasToTableName.get(sourceName)).getAttributeNames();
                        solvedSourceName = aliasToTableName.get(sourceName);
                        cachedSourceSchemas.put(solvedSourceName, sourceSchema);
                    } else {
                        sourceSchema = subqueriesAlias.get(sourceName).getFieldNames();
                        solvedSourceName = sourceName;
                        cachedSourceSchemas.put(solvedSourceName, sourceSchema);
                    }
                    if (sourceSchema == null) {
                        throw new RuntimeException("Impossible to solve source " + sourceName);
                    }
                } else {
                    solvedSourceName = sourceName;
                }
                int index = sourceSchema.indexOf(attributeName);
                if (index == -1) {
                    throw new RuntimeException("Attribute " + attributeName + " is not defined for source " + solvedSourceName);
                } else {
                    sourceToSelectArguments.computeIfAbsent(solvedSourceName, k -> new ArrayList<>()).add(attributeName);
                }
            }
        }
        for(Pair<Integer, String> aggregate: aggregates){
            String rawAggregateName = aggregate.getValue1();
            intermediateSchema.add(rawAggregateName);
            Integer aggregateOp = aggregate.getValue0();
            String [] split = rawAggregateName.split("\\.");
            String rawSourceName = split[0];
            String attributeName = split[1];
            String solvedSourceName;
            List<String> sourceSchema = cachedSourceSchemas.get(rawSourceName);
            if(sourceSchema == null) {
                if (tableNameToAlias.containsKey(rawSourceName)) {
                    sourceSchema = broker.getTableInfo(rawSourceName).getAttributeNames();
                    solvedSourceName = rawSourceName;
                    cachedSourceSchemas.put(solvedSourceName, sourceSchema);
                } else if (aliasToTableName.containsKey(rawSourceName)) {
                    sourceSchema = broker.getTableInfo(aliasToTableName.get(rawSourceName)).getAttributeNames();
                    solvedSourceName = aliasToTableName.get(rawSourceName);
                    cachedSourceSchemas.put(solvedSourceName, sourceSchema);
                } else {
                    sourceSchema = subqueriesAlias.get(rawSourceName).getFieldNames();
                    solvedSourceName = rawSourceName;
                    cachedSourceSchemas.put(solvedSourceName, sourceSchema);
                }
                if (sourceSchema == null) {
                    throw new RuntimeException("Impossible to solve source " + rawSourceName);
                }
            }else{
                solvedSourceName = rawSourceName;
            }
            int index = sourceSchema.indexOf(attributeName);
            if(index == -1){
                throw new RuntimeException("Attribute " + attributeName + " is not defined for source " + rawSourceName);
            }else{
                sourceToAggregates.computeIfAbsent(solvedSourceName, k->new ArrayList<>()).add(new Pair<>(aggregateOp, attributeName));
            }
        }
        for(String tableName: tableNameToAlias.keySet()){
            List<String> tableSchema = cachedSourceSchemas.get(tableName);
            if(tableSchema == null){
                tableSchema = broker.getTableInfo(tableName).getAttributeNames();
                cachedSourceSchemas.put(tableName, tableSchema);
            }
            for(String attrName: tableSchema){
                String dotNotation = tableName + "." + attrName;
                if(rawSelectionPredicate.contains(dotNotation)){
                    sourceToSelectionPredicateArguments.computeIfAbsent(tableName, k->new ArrayList<>()).add(attrName);
                    intermediateSchema.add(dotNotation);
                }
            }
        }
        for(Map.Entry<String,String> aliasToStringEntry: aliasToTableName.entrySet()){
            List<String> tableSchema = cachedSourceSchemas.get(aliasToStringEntry.getValue());
            String tableName = aliasToStringEntry.getValue();
            if(tableSchema == null){
                tableSchema = broker.getTableInfo(tableName).getAttributeNames();
                cachedSourceSchemas.put(tableName, tableSchema);
            }
            for(String attrName: tableSchema){
                String dotNotation = tableName + "." + attrName;
                if(rawSelectionPredicate.contains(dotNotation)){
                    sourceToSelectionPredicateArguments.computeIfAbsent(tableName, k->new ArrayList<>()).add(attrName);
                    intermediateSchema.add(dotNotation);
                }
            }
        }
        for(Map.Entry<String, RelReadQueryResults> subqueryEntry: subqueriesAlias.entrySet()){
            String alias = subqueryEntry.getKey();
            List<String> sourceSchema = subqueryEntry.getValue().getFieldNames();
            for(String attrName: sourceSchema){
                String dotNotation = alias + "." + attrName;
                if(rawSelectionPredicate.contains(dotNotation)){
                    sourceToSelectionPredicateArguments.computeIfAbsent(alias, k->new ArrayList<>()).add(attrName);
                    intermediateSchema.add(dotNotation);
                }
            }
        }
    }



/*
    private void selectAll(){
        for(Map.Entry<String, String> tableAlias: tableAliases.entrySet()){
            String tableName = tableAlias.getKey();
            List<String> fieldNames = broker.getTableInfo(tableName).getAttributeNames();
            List<Integer> allIndexes = new ArrayList<>();
            for(int i = 0; i < fieldNames.size(); i++){
                allIndexes.add(i);
            }
            projectionIntermediateTable.put(tableName, allIndexes);
        }
        for (Map.Entry<String, ReadQueryResults> aliasResults : subqueriesAlias.entrySet()){
            String queryName = aliasResults.getKey();
            List<String> fieldNames = aliasResults.getValue().getFieldNames();
            List<Integer> allIndexes = new ArrayList<>();
            for(int i = 0; i < fieldNames.size(); i++){
                allIndexes.add(i);
            }
            projectionIntermediateTable.put(queryName, allIndexes);
        }
    }

 */

    private boolean noDuplicateInStringArray(String[] array){
        for(int i = 0; i<array.length; i++){
            for(int j = i+1; j < array.length; j++){
                if(array[i] != null && array[j] != null && array[i].equals(array[j])){
                    return false;
                }
            }
        }
        return true;
    }
}
