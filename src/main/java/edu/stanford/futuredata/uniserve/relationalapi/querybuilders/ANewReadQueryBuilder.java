package edu.stanford.futuredata.uniserve.relationalapi.querybuilders;

import edu.stanford.futuredata.uniserve.broker.Broker;
import edu.stanford.futuredata.uniserve.relationalapi.AnotherAggregateQuery;
import edu.stanford.futuredata.uniserve.relationalapi.AnotherSimpleAggregateQuery;
import edu.stanford.futuredata.uniserve.relationalapi.FilterAndProjectionQuery;
import edu.stanford.futuredata.uniserve.relationalapi.ReadQuery;
import edu.stanford.futuredata.uniserve.utilities.TableInfo;
import org.javatuples.Pair;

import java.util.*;

public class ANewReadQueryBuilder {
    private Broker broker;

    public static final Integer AVG = 1;
    public static final Integer MIN = 2;
    public static final Integer MAX = 3;
    public static final Integer COUNT = 4;
    public static final Integer SUM = 5;

    private List<String> projectionAttributes = new ArrayList<>();
    private List<Pair<Integer, String>> aggregates = new ArrayList<>();

    private List<String> userResultsSchema = new ArrayList<>();
    private List<String> aggregateUserAttributesNames = new ArrayList<>();

    private String sourceName = "";
    private boolean sourceIsTable = true;
    private String sourceFilter = "";
    private Map<String, ReadQuery> subquery = new HashMap<>();
    private List<String> sourceSchema = new ArrayList<>();

    private String rawHavingPredicate = "";
    private boolean distinct = false;

    private boolean isStored = false;

    private Map<String, ReadQuery> predicateSubqueries = new HashMap<>();

    public ANewReadQueryBuilder(Broker broker){
        this.broker = broker;
    }

    //TODO: INPUT SAFETY CHECKS

    public ANewReadQueryBuilder select(){
        return this;
    }
    public ANewReadQueryBuilder select(String... selectedFields){
        if(!noDuplicateInStringArray(selectedFields)){
            throw new RuntimeException("Duplicate elements in select clause arguments");
        }
        this.projectionAttributes.addAll(Arrays.asList(selectedFields));

        return this;
    }
    public ANewReadQueryBuilder alias(String... aliases){
        if(projectionAttributes.isEmpty()){
            throw new RuntimeException("Alias specified for non-explicitly declared selection attributes");
        }
        userResultsSchema = Arrays.asList(aliases);
        if(!noDuplicateInStringArray(aliases))
            throw new RuntimeException("Error in parameter alias definition: Multiple projection args have the same alias");
        if((userResultsSchema.size() + aggregateUserAttributesNames.size()) != (projectionAttributes.size() + aggregates.size())){
            throw new RuntimeException("Wrong number of aliases provided for selected fields");
        }
        return this;
    }
    public ANewReadQueryBuilder avg(String aggregatedField, String alias){
        this.aggregates.add(new Pair<>(AVG, aggregatedField));
        this.aggregateUserAttributesNames.add(alias);
        return this;
    }
    public ANewReadQueryBuilder min(String aggregatedField, String alias){
        this.aggregates.add(new Pair<>(MIN, aggregatedField));
        this.aggregateUserAttributesNames.add(alias);
        return this;
    }
    public ANewReadQueryBuilder max(String aggregatedField, String alias){
        this.aggregates.add(new Pair<>(MAX, aggregatedField));
        this.aggregateUserAttributesNames.add(alias);
        return this;
    }
    public ANewReadQueryBuilder count(String aggregatedField, String alias){
        this.aggregates.add(new Pair<>(COUNT, aggregatedField));
        this.aggregateUserAttributesNames.add(alias);
        return this;
    }
    public ANewReadQueryBuilder sum(String aggregatedField, String alias){
        aggregates.add(new Pair<>(SUM, aggregatedField));
        this.aggregateUserAttributesNames.add(alias);
        return this;
    }

    public ANewReadQueryBuilder from(String tableName){
        if(tableName == null || tableName.isEmpty()){
            throw new RuntimeException("Invalid tableName, missing");
        }
        TableInfo t = broker.getTableInfo(tableName);
        if(t.equals(Broker.NIL_TABLE_INFO)){
            throw new RuntimeException(tableName+" does not match any allocated table");
        }
        sourceSchema = t.getAttributeNames();
        sourceName = tableName;
        return this;
    }
    public ANewReadQueryBuilder from(ReadQuery subquery, String alias){
        if(subquery == null){
            throw new RuntimeException("Subquery is null");
        }
        if(alias == null || alias.isEmpty()){
            throw new RuntimeException("Missing alias for subquery");
        }
        subquery.setIsThisSubquery(true);
        sourceName = alias;
        this.subquery.put(alias, subquery);
        this.sourceIsTable = false;
        sourceSchema = subquery.getResultSchema();
        return this;
    }
    public ANewReadQueryBuilder from(ReadQueryBuilder subqueryToBuild, String alias){
        if(subqueryToBuild == null){
            throw new RuntimeException("Subquery is null");
        }
        return from(subqueryToBuild.build(), alias);
    }

    public ANewReadQueryBuilder fromFilter(String tableName, String filter){
        if(filter == null || filter.isEmpty()){
            throw new RuntimeException("Filter is empty or null");
        }
        this.sourceFilter = filter;
        return from(tableName);
    }
    public ANewReadQueryBuilder fromFilter(ReadQuery subquery, String alias, String filter){
        if(filter == null || filter.isEmpty()){
            throw new RuntimeException("Filter is empty or null");
        }
        if(subquery == null){
            throw new RuntimeException("Subquery is null");
        }
        subquery.setIsThisSubquery(true);
        this.sourceFilter = filter;
        return from(subquery, alias);
    }
    public ANewReadQueryBuilder fromFilter(ReadQueryBuilder subqueryToBuild, String alias, String filter){
        if(filter == null || filter.isEmpty()){
            throw new RuntimeException("Filter is empty or null");
        }
        if(subqueryToBuild == null){
            throw new RuntimeException("Subquery is null");
        }
        return fromFilter(subqueryToBuild.build(), alias, filter);
    }

    public ANewReadQueryBuilder predicateSubquery(String alias, ReadQuery subquery){
        predicateSubqueries.put(alias, subquery);
        return this;
    }

    public ANewReadQueryBuilder having(String havingPredicate){
        this.rawHavingPredicate = havingPredicate;
        return this;
    }
    public ANewReadQueryBuilder distinct(){
        this.distinct = true;
        return this;
    }
    public ANewReadQueryBuilder store(){
        this.isStored = true;
        return this;
    }

    public ReadQuery build(){
        for(Map.Entry<String, ReadQuery> entry: subquery.entrySet()){
            entry.getValue().setIsThisSubquery(true);
        }
        ReadQuery readQuery;
        if(!aggregates.isEmpty() && !projectionAttributes.isEmpty()){
            readQuery = buildAggregateQuery();
        } else if(!aggregates.isEmpty()){
            readQuery = buildSimpleAggregateQuery();
        }else{
            readQuery = buildSimpleQuery();
        }
        return readQuery;
    }

    private ReadQuery buildSimpleQuery(){
        List<String> resultSchema = new ArrayList<>();
        if(projectionAttributes.isEmpty()){
            projectionAttributes = this.sourceSchema;
        }
        for(String userAttribute: this.userResultsSchema){
            if(userAttribute == null || userAttribute.isEmpty()){
                resultSchema.add(userResultsSchema.indexOf(userAttribute),
                        projectionAttributes.get(userResultsSchema.indexOf(userAttribute)));
            }else{
                resultSchema.add(userResultsSchema.indexOf(userAttribute), userAttribute);
            }
        }
        if(resultSchema.size() < this.projectionAttributes.size()){
            for(int i = resultSchema.size(); i< projectionAttributes.size(); i++){
                resultSchema.add(i, projectionAttributes.get(i));
            }
        }
        if(resultSchema.isEmpty()){
            resultSchema = this.projectionAttributes;
        }
        FilterAndProjectionQuery query = new FilterAndProjectionQuery()
                .setSourceName          (this.sourceName)
                .setSourceSchema        (this.sourceSchema)
                .setResultSchema        (resultSchema)
                .setSystemResultSchema  (this.projectionAttributes)
                .setFilterPredicate     (this.sourceFilter)
                .setIsThisSubquery      (false)
                .setSourceSubqueries    (this.subquery)
                .setPredicateSubqueries (this.predicateSubqueries)
                ;
        if(this.distinct){
            query = query.setDistinct();
        }
        if(this.isStored){
            query = query.setStored();
        }
        ReadQuery readQuery = new ReadQuery().setFilterAndProjectionQuery(query).setResultSchema(resultSchema);
        if(this.isStored){
            readQuery = readQuery.setStored();
        }
        return readQuery;
    }
    private ReadQuery buildAggregateQuery(){
        List<String> systemSelectFields = projectionAttributes;
        for(String name: systemSelectFields){
            if(!sourceSchema.contains(name)){
                throw new RuntimeException("Undefined attribute in select clause: " + name);
            }
        }
        //same thing, but for arguments of the aggregate operators
        for(Pair<Integer, String> aggregate: aggregates){
            String name = aggregate.getValue1();
            if(!sourceSchema.contains(name)){
                throw new RuntimeException("Undefined attribute in select clause: " + name);
            }
        }
        List<String> finalUserSchema = new ArrayList<>();
        List<String> finalUserSchemaSelect = new ArrayList<>();

        if(userResultsSchema.size()<systemSelectFields.size()){
            userResultsSchema.addAll(systemSelectFields.subList(userResultsSchema.size(), systemSelectFields.size()));
        }


        for(String userFinalName: userResultsSchema){
            if(userFinalName == null || userFinalName.isEmpty()){
                finalUserSchemaSelect.add(systemSelectFields.get(userResultsSchema.indexOf(userFinalName)));
            }else{
                finalUserSchemaSelect.add(userFinalName);
            }
        }
        finalUserSchema.addAll(finalUserSchemaSelect);
        finalUserSchema.addAll(aggregateUserAttributesNames);
        AnotherAggregateQuery query = new AnotherAggregateQuery()
                .setSourceName(this.sourceName)
                .setSourceIsTable(this.sourceIsTable)
                .setSourceSchema(this.sourceSchema)
                .setResultSchema(finalUserSchema)
                .setSystemSelectedFields(systemSelectFields)
                .setAggregatesSpecification(this.aggregates)
                .setFilterPredicate(this.sourceFilter)
                .setSourceSubqueries(this.subquery)
                .setIsThisSubquery(false)
                .setHavingPredicate(this.rawHavingPredicate)
                .setPredicateSubqueries(this.predicateSubqueries)
                ;
        if(this.isStored){
            query = query.setStored();
        }
        ReadQuery readQuery = new ReadQuery().setAnotherAggregateQuery(query).setResultSchema(finalUserSchema);
        if(this.isStored){
            readQuery = readQuery.setStored();
        }
        return readQuery;
    }
    private ReadQuery buildSimpleAggregateQuery(){
        AnotherSimpleAggregateQuery query = new AnotherSimpleAggregateQuery()
                .setSourceName(this.sourceName)
                .setSourceIsTable(this.sourceIsTable)
                .setSourceSchema(this.sourceSchema)
                .setResultsSchema(aggregateUserAttributesNames)
                .setAggregatesSpecification(this.aggregates)
                .setFilterPredicate(this.sourceFilter)
                .setSourceSubqueries(this.subquery)
                .setIsThisSubquery(false)
                .setPredicateSubqueries(this.predicateSubqueries)
                ;
        if(this.isStored){
            query = query.setStored();
        }
        ReadQuery readQuery = new ReadQuery().setResultSchema(aggregateUserAttributesNames).setAnotherSimpleAggregateQuery(query);
        if(this.isStored){
            readQuery = readQuery.setStored();
        }
        return readQuery;
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