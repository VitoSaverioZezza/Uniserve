package edu.stanford.futuredata.uniserve.relationalapi.querybuilders;

import edu.stanford.futuredata.uniserve.broker.Broker;
import edu.stanford.futuredata.uniserve.interfaces.ReadQueryResults;
import edu.stanford.futuredata.uniserve.relational.RelReadQueryResults;
import edu.stanford.futuredata.uniserve.relationalapi.IntermediateQuery;
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
    private String rawSelectionPredicate = "";

    private String[] rawGroupFields = null;
    private String rawHavingPredicate = null;
    private boolean distinct = false;
    private boolean isStored = false;

    private Map<String, String> tableNameToAlias = new HashMap<>(); //contains ALL tables, values for tableNames without aliases are set to null;
    private Map<String, String> aliasToTableName = new HashMap<>(); //contains ONLY aliases of the tables
    private Map<String, ReadQuery> subqueriesAlias = new HashMap<>(); //contains ALL subqueries mappings

    private Map<String, List<Pair<Integer, String>>> sourceToAggregates = new HashMap<>(); //fields to be aggregate in simple notation
    private Map<String, List<String>> sourceToSelectionPredicateArguments = new HashMap<>(); //attributes needed for predicate evaluation in simple notation
    private Map<String, List<String>> sourceToSelectArguments = new HashMap<>(); //ALL fields selected, without aggregates and without attributes needed

    private List<Pair<String, String>> intermediateSchema = new ArrayList<>(); //ALL fields needed for the intermediate schema in dot notation, first the select,
    private Map<String, List<String>> srcInterProjectSchema = new HashMap<>();
    private String intermediateResID = String.valueOf(new Random().nextInt());

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
        subqueriesAlias.put(alias, subquery);
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
        ReadQuery ret = new ReadQuery();
        ret.setIntermediateQuery(buildIntermediateQuery());
        List<String> resSchema = new ArrayList<>();
        for(Pair<String, String> entry: intermediateSchema){
            resSchema.add(entry.getValue0()+"."+entry.getValue1());
        }
        ret.setResultSchema(resSchema);
        return ret;
    }

    private IntermediateQuery buildIntermediateQuery(){
        Map<String, List<String>> sourceToAggregatedFields = new HashMap<>();

        /*
        * If no selection attribute is specified, iterate through all tables and subqueries in order to select all their
        * attributes.
        * */
        if(rawSelectedFields.isEmpty()){
            for(String tableName: tableNameToAlias.keySet()){
                List<String> tableSchema = broker.getTableInfo(tableName).getAttributeNames();
                cachedSourceSchemas.put(tableName, tableSchema);
                sourceToSelectArguments.put(tableName, tableSchema);
                for(String attributeName: tableSchema){
                    intermediateSchema.add(new Pair<>(tableName, attributeName));
                }
                srcInterProjectSchema.put(tableName, tableSchema);
            }
            for(String subqueryAlias: subqueriesAlias.keySet()){
                List<String> subquerySchema = subqueriesAlias.get(subqueryAlias).getResultSchema();
                cachedSourceSchemas.put(subqueryAlias, subquerySchema);
                sourceToSelectArguments.put(subqueryAlias, subquerySchema);
                for(String attributeName: subquerySchema){
                    intermediateSchema.add(new Pair<>(subqueryAlias, attributeName));
                }
                srcInterProjectSchema.put(subqueryAlias, subquerySchema);
            }
        }
        else{
            /*Filling the sourceToSelectArguments with the arguments of the select operator
            * First split the raw attributes "Table.Attribute" in the two parts, then solve the first one since it
            * can be an alias for a table or a subquery. Once that is solved, check whether the attribute is part of that
            * schema and if so add it to the intermediate schema
            * */
            for (String selectAttribute : rawSelectedFields) {
                String[] split = selectAttribute.split("\\.");
                String sourceName = split[0];
                StringBuilder builder = new StringBuilder();
                for(int i = 1; i < split.length-1; i++) {
                    builder.append(split[i]);
                    builder.append(".");
                }
                builder.append(split[split.length-1]);
                String attributeName = builder.toString();
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
                        ReadQuery subqueryRes = subqueriesAlias.get(sourceName);
                        sourceSchema = subqueryRes.getResultSchema();


                        //sourceSchema = subqueriesAlias.get(sourceName).getFieldNames();
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
                    Pair<String, String> intermediateSchemaEntry = new Pair<>(solvedSourceName, attributeName);
                    if(!intermediateSchema.contains(intermediateSchemaEntry))
                        intermediateSchema.add(intermediateSchemaEntry);
                    if(!srcInterProjectSchema.computeIfAbsent(solvedSourceName, k->new ArrayList<>()).contains(attributeName)){
                        srcInterProjectSchema.get(solvedSourceName).add(attributeName);
                    }
                }
            }
        }
        /*Filling sourceToAggregates, attributes not in the dot notation, stored in pairs with the operation code*/
        for(Pair<Integer, String> aggregate: aggregates){
            String rawAggregateName = aggregate.getValue1();
            Integer aggregateOp = aggregate.getValue0();
            String [] split = rawAggregateName.split("\\.");
            String rawSourceName = split[0];
            StringBuilder builder = new StringBuilder();
            for(int i = 1; i < split.length-1; i++) {
                builder.append(split[i]);
                builder.append(".");
            }
            builder.append(split[split.length-1]);
            String attributeName = builder.toString();
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
                    ReadQuery subqueryResult = subqueriesAlias.get(rawSourceName);
                    sourceSchema = subqueryResult.getResultSchema();

                    //sourceSchema = subqueriesAlias.get(rawSourceName).getFieldNames();
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
                sourceToAggregatedFields.computeIfAbsent(solvedSourceName, k->new ArrayList<>());
                if(!sourceToAggregatedFields.get(solvedSourceName).contains(attributeName)){
                    sourceToAggregatedFields.get(solvedSourceName).add(attributeName);
                }
                Pair<String, String> intermediateSchemaEntry = new Pair<>(solvedSourceName, attributeName);
                if(!intermediateSchema.contains(intermediateSchemaEntry))
                    intermediateSchema.add(intermediateSchemaEntry);
                if(!srcInterProjectSchema.computeIfAbsent(solvedSourceName, k->new ArrayList<>()).contains(attributeName)){
                    srcInterProjectSchema.get(solvedSourceName).add(attributeName);
                }
            }
        }

        /*Check all source names and their schema for matches in the predicate*/
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
                    Pair<String, String> intermediateSchemaEntry = new Pair<>(tableName, attrName);
                    if(!intermediateSchema.contains(intermediateSchemaEntry))
                        intermediateSchema.add(intermediateSchemaEntry);
                    if(!srcInterProjectSchema.computeIfAbsent(tableName, k->new ArrayList<>()).contains(attrName)){
                        srcInterProjectSchema.get(tableName).add(attrName);
                    }
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


                    Pair<String, String> intermediateSchemaEntry = new Pair<>(tableName, attrName);
                    if(!intermediateSchema.contains(intermediateSchemaEntry))
                        intermediateSchema.add(intermediateSchemaEntry);
                    if(!srcInterProjectSchema.computeIfAbsent(tableName, k->new ArrayList<>()).contains(attrName)){
                        srcInterProjectSchema.get(tableName).add(attrName);
                    }

                }
            }
        }
        //for(Map.Entry<String, RelReadQueryResults> subqueryEntry: subqueriesAlias.entrySet()){
        for(Map.Entry<String, ReadQuery> subqueryEntry: subqueriesAlias.entrySet()){
            String alias = subqueryEntry.getKey();

            ReadQuery subqueryResult = subqueryEntry.getValue();
            List<String> sourceSchema = subqueryResult.getResultSchema();
            //List<String> sourceSchema = subqueryEntry.getValue().getFieldNames();
            for(String attrName: sourceSchema){
                String dotNotation = alias + "." + attrName;
                if(rawSelectionPredicate.contains(dotNotation)){
                    sourceToSelectionPredicateArguments.computeIfAbsent(alias, k->new ArrayList<>()).add(attrName);
                    Pair<String, String> intermediateSchemaEntry = new Pair<>(alias, attrName);
                    if(!intermediateSchema.contains(intermediateSchemaEntry))
                        intermediateSchema.add(intermediateSchemaEntry);
                    if(!srcInterProjectSchema.computeIfAbsent(alias, k->new ArrayList<>()).contains(attrName)){
                        srcInterProjectSchema.get(alias).add(attrName);
                    }
                }
            }
        }
        IntermediateQuery intermediateQuery = new IntermediateQuery();
        intermediateQuery.setIntermediateSchema(intermediateSchema);
        List<String> tableNames = new ArrayList<>(tableNameToAlias.keySet());
        intermediateQuery.setTableNames(tableNames);
        intermediateQuery.setSrcInterProjectSchema(srcInterProjectSchema);
        intermediateQuery.setCachedSourceSchema(cachedSourceSchemas);
        intermediateQuery.setSubqueries(subqueriesAlias);
        return intermediateQuery;
    }

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
