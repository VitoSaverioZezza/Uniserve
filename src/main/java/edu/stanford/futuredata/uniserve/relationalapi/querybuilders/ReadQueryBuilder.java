package edu.stanford.futuredata.uniserve.relationalapi.querybuilders;

import edu.stanford.futuredata.uniserve.broker.Broker;
import edu.stanford.futuredata.uniserve.relationalapi.AggregateQuery;
import edu.stanford.futuredata.uniserve.relationalapi.ProdSelProjQuery;
import edu.stanford.futuredata.uniserve.relationalapi.ReadQuery;
import org.javatuples.Pair;

import java.util.*;

public class ReadQueryBuilder {
    public static final Integer AVG = 1;
    public static final Integer MIN = 2;
    public static final Integer MAX = 3;
    public static final Integer COUNT = 4;
    public static final Integer SUM = 5;

    private final Broker broker;

    private List<String> rawSelectedFields = new ArrayList<>();
    private List<String> fieldsAliases = new ArrayList<>();
    private List<String> aggregateAttributesNames = new ArrayList<>();

    private final List<Pair<Integer, String>> aggregates = new ArrayList<>();
    private String rawSelectionPredicate = "";

    private String rawHavingPredicate = "";
    private boolean distinct = false;
    private boolean isStored = false;

    private Map<String, String> tableNameToAlias = new HashMap<>(); //contains ALL tables, values for tableNames without aliases are set to null;
    private Map<String, String> aliasToTableName = new HashMap<>(); //contains ONLY aliases of the tables
    private Map<String, ReadQuery> subqueriesAlias = new HashMap<>(); //contains ALL subqueries mappings


    //TODO: Second part of the query for aggregates
    //TODO: General cleanup
    //TODO: Stored queries management

    public ReadQueryBuilder(Broker broker){
        this.broker = broker;
    }

    //ASSUMPTION: ALL ARGUMENTS USE DOT NOTATION WITH TABLENAMES OR ALIASES
    public ReadQueryBuilder select(){
        if(fieldsAliases != null){
            throw new RuntimeException("Cannot select new attributes after aliases definition");
        }
        return this;
    }
    public ReadQueryBuilder select(String... selectedFields){
        if(!fieldsAliases.isEmpty()){
            throw new RuntimeException("Cannot select new attributes after aliases definition");
        }
        this.rawSelectedFields.addAll(Arrays.asList(selectedFields));
        return this;
    }
    public ReadQueryBuilder alias(String... aliases){
        if(rawSelectedFields.isEmpty()){
            throw new RuntimeException("Alias specified for non-explicitly declared selection attributes");
        }
        if(!fieldsAliases.isEmpty()){
            throw new RuntimeException("Cannot select new attributes after aliases definition");
        }
        fieldsAliases = Arrays.asList(aliases);
        if(!noDuplicateInStringArray(aliases))
            throw new RuntimeException("Error in parameter alias definition: Multiple projection args have the same alias");
        if((fieldsAliases.size() + aggregateAttributesNames.size())!= (rawSelectedFields.size() + aggregates.size())){
            throw new RuntimeException("Wrong number of aliases provided for selected fields");
        }
        return this;
    }
    public ReadQueryBuilder avg(String aggregatedField, String alias){
        if(!fieldsAliases.isEmpty()){
            throw new RuntimeException("Cannot select new aggregates after aliases definition");
        }
        this.aggregates.add(new Pair<>(AVG, aggregatedField));
        this.aggregateAttributesNames.add(alias);
        return this;
    }
    public ReadQueryBuilder min(String aggregatedField, String alias){
        if(!fieldsAliases.isEmpty()){
            throw new RuntimeException("Cannot select new aggregates after aliases definition");
        }
        this.aggregates.add(new Pair<>(MIN, aggregatedField));
        this.aggregateAttributesNames.add(alias);
        return this;
    }
    public ReadQueryBuilder max(String aggregatedField, String alias){
        if(!fieldsAliases.isEmpty()){
            throw new RuntimeException("Cannot select new aggregates after aliases definition");
        }
        this.aggregates.add(new Pair<>(MAX, aggregatedField));
        this.aggregateAttributesNames.add(alias);
        return this;
    }
    public ReadQueryBuilder count(String aggregatedField, String alias){
        if(!fieldsAliases.isEmpty()){
            throw new RuntimeException("Cannot select new aggregates after aliases definition");
        }
        this.aggregates.add(new Pair<>(COUNT, aggregatedField));
        this.aggregateAttributesNames.add(alias);
        return this;
    }
    public ReadQueryBuilder sum(String aggregatedField, String alias){
        if(!fieldsAliases.isEmpty()){
            throw new RuntimeException("Cannot select new aggregates after aliases definition");
        }
        aggregates.add(new Pair<>(SUM, aggregatedField));
        this.aggregateAttributesNames.add(alias);
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
        if(aggregates.isEmpty()){
            return buildSimpleQuery();
        }else{
            return buildAggregateQuery();
        }
    }


    private ReadQuery buildSimpleQuery(){
        String selectionPredicate = rawSelectionPredicate;

        List<String> userFinalSchema = new ArrayList<>();
        List<String> systemFinalSchema = new ArrayList<>();
        List<String> systemCombineSchema = new ArrayList<>();
        Map<String, List<String>> sourcesSubschemasForCombine = new HashMap<>();
        Map<String, List<String>> sourcesSchema = new HashMap<>();

        if(rawSelectedFields.isEmpty()){
            //selectAll
            for(Map.Entry<String, String> tableAlias: tableNameToAlias.entrySet()){
                String tableName = tableAlias.getKey();
                List<String> sourceTableSchema = broker.getTableInfo(tableName).getAttributeNames();
                sourcesSchema.put(tableName, sourceTableSchema);
                sourcesSubschemasForCombine.put(tableName, sourceTableSchema);
                for(String attributeName: sourceTableSchema){
                    userFinalSchema.add(tableName+"."+attributeName);
                    systemFinalSchema.add(tableName+"."+attributeName);
                    systemCombineSchema.add(tableName+"."+attributeName);
                }
            }
            for(Map.Entry<String, ReadQuery> subquery: subqueriesAlias.entrySet()){
                String subqueryAlias = subquery.getKey();
                List<String> subquerySchema = subquery.getValue().getResultSchema();
                sourcesSchema.put(subqueryAlias, subquerySchema);
                sourcesSubschemasForCombine.put(subqueryAlias, subquerySchema);
                for(String attributeName: subquerySchema){
                    userFinalSchema.add(subqueryAlias+"."+attributeName);
                    systemFinalSchema.add(subqueryAlias+"."+attributeName);
                    systemCombineSchema.add(subqueryAlias+"."+attributeName);
                }
            }
        }
        else{
            for(String rawSelectedAttributeName: rawSelectedFields){
                //all attributes in the select clause are in dotted notation, but the table name can be specified with alias
                Pair<String, String> splitRaw = splitRawAttribute(rawSelectedAttributeName);
                String systemAttributeName = splitRaw.getValue0()+"."+splitRaw.getValue1();

                systemFinalSchema.add(systemAttributeName);
                systemCombineSchema.add(systemAttributeName);
                sourcesSubschemasForCombine.computeIfAbsent(splitRaw.getValue0(), k->new ArrayList<>()).add(splitRaw.getValue1());
                if(subqueriesAlias.containsKey(splitRaw.getValue0()) && !sourcesSchema.containsKey(splitRaw.getValue0())){
                    sourcesSchema.put(splitRaw.getValue0(), subqueriesAlias.get(splitRaw.getValue0()).getResultSchema());
                }else {
                    sourcesSchema.computeIfAbsent(splitRaw.getValue0(), k -> broker.getTableInfo(splitRaw.getValue0()).getAttributeNames());
                }
                if(!fieldsAliases.isEmpty() && !(fieldsAliases.get(rawSelectedFields.indexOf(rawSelectedAttributeName)).isEmpty())){
                    userFinalSchema.add(fieldsAliases.get(rawSelectedFields.indexOf(rawSelectedAttributeName)));
                }else{
                    userFinalSchema.add(rawSelectedAttributeName);
                }
            }
            if(!selectionPredicate.isEmpty()){
                for (Map.Entry<String,String> tableNameAndAlias: tableNameToAlias.entrySet()){
                    String tableName = tableNameAndAlias.getKey();
                    if(!selectionPredicate.contains(tableName)){
                        continue;
                    }
                    List<String> sourceSchema = broker.getTableInfo(tableName).getAttributeNames();
                    for(String attribute: sourceSchema){
                        if(selectionPredicate.contains(tableName+"."+attribute)){
                            sourcesSchema.put(tableName, sourceSchema);
                            sourcesSubschemasForCombine.computeIfAbsent(tableName, k->new ArrayList<>()).add(attribute);
                            systemCombineSchema.add(tableName+"."+attribute);
                        }
                    }
                }
                for(Map.Entry<String,String> aliasAndTableName: aliasToTableName.entrySet()){
                    String alias = aliasAndTableName.getKey();
                    if(!selectionPredicate.contains(alias)){
                        continue;
                    }
                    String tableName = aliasAndTableName.getValue();
                    List<String> sourceSchema = broker.getTableInfo(tableName).getAttributeNames();
                    for(String attribute: sourceSchema){
                        if(selectionPredicate.contains(alias+"."+attribute)){
                            sourcesSchema.put(tableName, sourceSchema);
                            sourcesSubschemasForCombine.computeIfAbsent(tableName, k->new ArrayList<>()).add(attribute);
                            systemCombineSchema.add(tableName+"."+attribute);
                        }
                    }
                }
                for (Map.Entry<String,ReadQuery> aliasAndReadQuery: subqueriesAlias.entrySet()){
                    String alias = aliasAndReadQuery.getKey();
                    if(!selectionPredicate.contains(alias)){
                        continue;
                    }
                    List<String> sourceSchema = aliasAndReadQuery.getValue().getResultSchema();
                    for(String attribute: sourceSchema){
                        if(selectionPredicate.contains(alias+"."+attribute)){
                            sourcesSchema.put(alias, sourceSchema);
                            sourcesSubschemasForCombine.computeIfAbsent(alias, k->new ArrayList<>()).add(attribute);
                            systemCombineSchema.add(alias+"."+attribute);
                        }
                    }
                }
            }
        }
        ProdSelProjQuery simpleQuery = new ProdSelProjQuery()
                .setSystemFinalSchema(systemFinalSchema)
                .setSystemCombineSchema(systemCombineSchema)
                .setSourcesSubschemasForCombine(sourcesSubschemasForCombine)
                .setCachedSourcesSchema(sourcesSchema)
                .setUserFinalSchema(userFinalSchema)
                .setSelectionPredicate(selectionPredicate)
                .setTableNames(new ArrayList<>(tableNameToAlias.keySet()))
                .setSubqueries(subqueriesAlias)
                .setAliasToTableMap(aliasToTableName);
        ReadQuery res = new ReadQuery()
                .setSimpleQuery(simpleQuery)
                .setResultSchema(userFinalSchema);
        return res;
    }
    private ReadQuery buildAggregateQuery(){
        String selectionPredicate = rawSelectionPredicate;
        String parsedHavingPredicate = rawHavingPredicate;

        List<String> userFinalSchema = new ArrayList<>();   //result schema of the whole query
        List<Pair<Integer, String>> systemAggregateSubschema = new ArrayList<>(); //aggregate part of the schema going as input of the final combine operation
        List<String> systemGroupSubschema = new ArrayList<>(); //group part of the schema going as input of the final combine operation
        List<String> systemGatherSchema = new ArrayList<>(); //schema of the input of the gather operation, it includes the group attributes and attributes to be aggregated



        List<String> systemIntermediateSchema = new ArrayList<>();  //result schema of the subquery that will be input of the main one, this includes group attributes
                                                                    //and attributes that need to be aggregated
        List<String> systemIntermediateCombineSchema = new ArrayList<>(); //schema that includes group attributes, attributes to be aggregated and attributes that
                                                                            //are argument of the selection predicate
        Map<String, List<String>> intermediateSourcesSchemas = new HashMap<>();
        Map<String, List<String>> intermediateSourcesSubschemasForCombine = new HashMap<>();

        if(rawSelectedFields.isEmpty()){
            throw new RuntimeException("No group attributes specified");
        }
        for(String rawSelectedAttributeName: rawSelectedFields){
            //these attributes will be used as arguments for the group operation in the scatter and gather.

            Pair<String, String> splitRaw = splitRawAttribute(rawSelectedAttributeName);
            String systemAttributeName = splitRaw.getValue0()+"."+splitRaw.getValue1();

            systemIntermediateSchema.add(systemAttributeName);
            systemIntermediateCombineSchema.add(systemAttributeName);
            intermediateSourcesSubschemasForCombine.computeIfAbsent(splitRaw.getValue0(), k->new ArrayList<>()).add(splitRaw.getValue1());
            if(subqueriesAlias.containsKey(splitRaw.getValue0()) && !intermediateSourcesSchemas.containsKey(splitRaw.getValue0())){
                intermediateSourcesSchemas.put(splitRaw.getValue0(), subqueriesAlias.get(splitRaw.getValue0()).getResultSchema());
            }else {
                intermediateSourcesSchemas.computeIfAbsent(splitRaw.getValue0(), k -> broker.getTableInfo(splitRaw.getValue0()).getAttributeNames());
            }
            systemGroupSubschema.add(systemAttributeName);
            systemGatherSchema.add(systemAttributeName);
            if(!fieldsAliases.isEmpty() && !fieldsAliases.get(rawSelectedFields.indexOf(rawSelectedAttributeName)).isEmpty()){
                userFinalSchema.add(fieldsAliases.get(rawSelectedFields.indexOf(rawSelectedAttributeName)));
            }else{
                userFinalSchema.add(rawSelectedAttributeName);
            }
        }
        for(Pair<Integer, String> aggregate: aggregates){
            String rawAggregatedAttribute = aggregate.getValue1();
            Pair<String, String> splitRaw = splitRawAttribute(rawAggregatedAttribute);
            String systemAttributeName = splitRaw.getValue0() + "." + splitRaw.getValue1();

            systemIntermediateSchema.add(systemAttributeName);
            systemIntermediateCombineSchema.add(systemAttributeName);
            intermediateSourcesSubschemasForCombine.computeIfAbsent(splitRaw.getValue0(), k->new ArrayList<>()).add(splitRaw.getValue1());
            if(subqueriesAlias.containsKey(splitRaw.getValue0()) && !intermediateSourcesSchemas.containsKey(splitRaw.getValue0())){
                intermediateSourcesSchemas.put(splitRaw.getValue0(), subqueriesAlias.get(splitRaw.getValue0()).getResultSchema());
            }else {
                intermediateSourcesSchemas.computeIfAbsent(splitRaw.getValue0(), k -> broker.getTableInfo(splitRaw.getValue0()).getAttributeNames());
            }
            systemGatherSchema.add(systemAttributeName);
            systemAggregateSubschema.add(new Pair<>(aggregate.getValue0(), systemAttributeName));
            userFinalSchema.add(aggregateAttributesNames.get(aggregates.indexOf(aggregate)));
        }

        if(!selectionPredicate.isEmpty()){
            for (Map.Entry<String,String> tableNameAndAlias: tableNameToAlias.entrySet()){
                String tableName = tableNameAndAlias.getKey();
                if(!selectionPredicate.contains(tableName)){
                    continue;
                }
                List<String> sourceSchema = broker.getTableInfo(tableName).getAttributeNames();
                for(String attribute: sourceSchema){
                    if(selectionPredicate.contains(tableName+"."+attribute)){
                        intermediateSourcesSchemas.put(tableName, sourceSchema);
                        intermediateSourcesSubschemasForCombine.computeIfAbsent(tableName, k->new ArrayList<>()).add(attribute);
                        systemIntermediateCombineSchema.add(tableName+"."+attribute);
                    }
                }
            }
            for(Map.Entry<String,String> aliasAndTableName: aliasToTableName.entrySet()){
                String alias = aliasAndTableName.getKey();
                if(!selectionPredicate.contains(alias)){
                    continue;
                }
                String tableName = aliasAndTableName.getValue();
                List<String> sourceSchema = broker.getTableInfo(tableName).getAttributeNames();
                for(String attribute: sourceSchema){
                    if(selectionPredicate.contains(tableName+"."+attribute)){
                        intermediateSourcesSchemas.put(tableName, sourceSchema);
                        intermediateSourcesSubschemasForCombine.computeIfAbsent(tableName, k->new ArrayList<>()).add(attribute);
                        systemIntermediateCombineSchema.add(tableName+"."+attribute);
                    }
                }
            }
            for (Map.Entry<String,ReadQuery> aliasAndReadQuery: subqueriesAlias.entrySet()){
                String alias = aliasAndReadQuery.getKey();
                if(!selectionPredicate.contains(alias)){
                    continue;
                }
                List<String> sourceSchema = aliasAndReadQuery.getValue().getResultSchema();
                for(String attribute: sourceSchema){
                    if(selectionPredicate.contains(alias+"."+attribute)){
                        intermediateSourcesSchemas.put(alias, sourceSchema);
                        intermediateSourcesSubschemasForCombine.computeIfAbsent(alias, k->new ArrayList<>()).add(attribute);
                        systemIntermediateCombineSchema.add(alias+"."+attribute);
                    }
                }
            }
        }
        ProdSelProjQuery intermediateQuery = new ProdSelProjQuery()
                .setCachedSourcesSchema(intermediateSourcesSchemas)
                .setSelectionPredicate(selectionPredicate)
                .setSubqueries(subqueriesAlias)
                .setTableNames(new ArrayList<>(tableNameToAlias.keySet()))
                .setSystemCombineSchema(systemIntermediateCombineSchema)
                .setSourcesSubschemasForCombine(intermediateSourcesSubschemasForCombine)
                .setSystemFinalSchema(systemIntermediateSchema)
                .setUserFinalSchema(systemIntermediateSchema);
        String intermediateQueryStringID = Integer.toString(new Random().nextInt());
        ReadQuery intermediateQueryWrapper = new ReadQuery().setSimpleQuery(intermediateQuery).setResultSchema(systemIntermediateSchema);
        AggregateQuery aggregateQuery = new AggregateQuery()
                .setIntermediateQuery(intermediateQueryStringID, intermediateQueryWrapper)
                .setAggregatesSubschema(systemAggregateSubschema)
                .setGatherInputRowsSchema(systemGatherSchema)
                .setGroupAttributesSubschema(systemGroupSubschema)
                .setFinalSchema(userFinalSchema);
        return new ReadQuery().setResultSchema(userFinalSchema).setAggregateQuery(aggregateQuery);
    }


    private Pair<String, String> splitRawAttribute(String rawAttribute){
        String[] split = rawAttribute.split("\\.");
        String sourceName = split[0];
        if(aliasToTableName.get(sourceName) != null){
            sourceName = aliasToTableName.get(sourceName);
        }
        StringBuilder attributeNameBuilder = new StringBuilder();
        for(int i = 1; i<split.length-1; i++){
            attributeNameBuilder.append(split[i]);
            attributeNameBuilder.append(".");
        }
        attributeNameBuilder.append(split[split.length-1]);
        String attrName = attributeNameBuilder.toString();
        return new Pair<>(sourceName, attrName);
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