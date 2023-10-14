package edu.stanford.futuredata.uniserve.relationalapi.querybuilders;

import edu.stanford.futuredata.uniserve.broker.Broker;
import edu.stanford.futuredata.uniserve.relationalapi.*;
import edu.stanford.futuredata.uniserve.utilities.TableInfo;
import org.javatuples.Pair;

import java.util.*;

public class RelReadQueryBuilder {
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
    private List<SerializablePredicate> operations = new ArrayList<>();


    public RelReadQueryBuilder(Broker broker){
        this.broker = broker;
    }

    //TODO: INPUT SAFETY CHECKS

    public RelReadQueryBuilder select(){
        return this;
    }
    public RelReadQueryBuilder select(String... selectedFields){
        this.projectionAttributes.addAll(Arrays.asList(selectedFields));
        return this;
    }
    public RelReadQueryBuilder alias(String... aliases){
        userResultsSchema = Arrays.asList(aliases);
        if(!noDuplicateInStringArray(aliases))
            throw new RuntimeException("Error in parameter alias definition: Multiple projection args have the same alias");
        /*
        if((userResultsSchema.size() + aggregateUserAttributesNames.size()) != (projectionAttributes.size() + aggregates.size())){
            throw new RuntimeException("Wrong number of aliases provided for selected fields");
        }
        */
        return this;
    }
    public RelReadQueryBuilder avg(String aggregatedField, String alias){
        this.aggregates.add(new Pair<>(AVG, aggregatedField));
        this.aggregateUserAttributesNames.add(alias);
        return this;
    }
    public RelReadQueryBuilder min(String aggregatedField, String alias){
        this.aggregates.add(new Pair<>(MIN, aggregatedField));
        this.aggregateUserAttributesNames.add(alias);
        return this;
    }
    public RelReadQueryBuilder max(String aggregatedField, String alias){
        this.aggregates.add(new Pair<>(MAX, aggregatedField));
        this.aggregateUserAttributesNames.add(alias);
        return this;
    }
    public RelReadQueryBuilder count(String aggregatedField, String alias){
        this.aggregates.add(new Pair<>(COUNT, aggregatedField));
        this.aggregateUserAttributesNames.add(alias);
        return this;
    }
    public RelReadQueryBuilder sum(String aggregatedField, String alias){
        aggregates.add(new Pair<>(SUM, aggregatedField));
        this.aggregateUserAttributesNames.add(alias);
        return this;
    }

    public RelReadQueryBuilder apply(SerializablePredicate... operations){
        for(SerializablePredicate operation: operations){
            if(operation == null){
                this.operations.add(o->o);
            }else {
                this.operations.add(operation);
            }
        }
        return this;
    }

    public RelReadQueryBuilder from(String tableName){
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
    public RelReadQueryBuilder from(ReadQuery subquery, String alias){
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
    public RelReadQueryBuilder from(RelReadQueryBuilder subqueryToBuild, String alias){
        if(subqueryToBuild == null){
            throw new RuntimeException("Subquery is null");
        }
        return from(subqueryToBuild.build(), alias);
    }

    public RelReadQueryBuilder fromFilter(String tableName, String filter){
        if(filter == null || filter.isEmpty()){
            throw new RuntimeException("Filter is empty or null");
        }
        this.sourceFilter = filter;
        return from(tableName);
    }
    public RelReadQueryBuilder fromFilter(ReadQuery subquery, String alias, String filter){
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
    public RelReadQueryBuilder fromFilter(RelReadQueryBuilder subqueryToBuild, String alias, String filter){
        if(filter == null || filter.isEmpty()){
            throw new RuntimeException("Filter is empty or null");
        }
        if(subqueryToBuild == null){
            throw new RuntimeException("Subquery is null");
        }
        return fromFilter(subqueryToBuild.build(), alias, filter);
    }
    public RelReadQueryBuilder fromFilter(JoinQueryBuilder subqueryToBuild, String alias, String filter){
        if(filter == null || filter.isEmpty()){
            throw new RuntimeException("Filter is empty or null");
        }
        if(subqueryToBuild == null){
            throw new RuntimeException("Subquery is null");
        }
        return fromFilter(subqueryToBuild.build(), alias, filter);
    }

    public RelReadQueryBuilder predicateSubquery(String alias, ReadQuery subquery){
        predicateSubqueries.put(alias, subquery);
        return this;
    }

    public RelReadQueryBuilder having(String havingPredicate){
        this.rawHavingPredicate = havingPredicate;
        return this;
    }
    public RelReadQueryBuilder distinct(){
        this.distinct = true;
        return this;
    }
    public RelReadQueryBuilder store(){
        this.isStored = true;
        return this;
    }

    public ReadQuery build(){
        if((sourceName == null || sourceName.isEmpty()) && subquery.isEmpty()){
            throw new RuntimeException("Unspecified source");
        }

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
        if(!userResultsSchema.isEmpty() && userResultsSchema.size() != projectionAttributes.size()){
            throw new RuntimeException("Wrong number of aliases provided for selected fields");
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

        if(operations.size() < resultSchema.size()){
            for(int i = operations.size(); i<resultSchema.size(); i++){
                operations.add(o -> o);
            }
        }

        List<Integer> indexesOfResultAttributes = new ArrayList<>();
        for(String resultSystemAttribute: this.projectionAttributes){
            int index = this.sourceSchema.indexOf(resultSystemAttribute);
            if(index == -1){
                throw new RuntimeException("Selected attribute " + resultSystemAttribute+" not in source schema");
            }else{
                indexesOfResultAttributes.add(index);
            }
        }


        List<Pair<String, Integer>> predVarToIndex = new ArrayList<>();
        if(sourceFilter != null && !sourceFilter.isEmpty()){
            for(String attrName: sourceSchema){
                if(sourceFilter.contains(sourceName+"."+attrName)){
                    sourceFilter = sourceFilter.replace(sourceName+"."+attrName, sourceName+(attrName.replace(".", "")));
                    predVarToIndex.add(new Pair<>((sourceName+(attrName.replace(".", ""))), sourceName.indexOf(attrName)));
                }
                if(sourceFilter.contains(attrName)){
                    if(attrName.contains(".")){
                        sourceFilter = sourceFilter.replace(attrName, attrName.replace(".", ""));
                        predVarToIndex.add(new Pair<>(attrName.replace(".", ""), sourceSchema.indexOf(attrName)));
                    }else{
                        predVarToIndex.add(new Pair<>(attrName, sourceSchema.indexOf(attrName)));
                    }
                }
            }
            for(String userAlias: resultSchema){
                if (sourceFilter.contains(userAlias)){
                    int index = this.sourceSchema.indexOf(this.projectionAttributes.get(resultSchema.indexOf(userAlias)));
                    if(index == -1){
                        throw new RuntimeException("ERROR: FATAL");
                    }
                    predVarToIndex.add(new Pair<>(userAlias, index));
                }
            }
        }

        FilterAndProjectionQuery query = new FilterAndProjectionQuery()
                .setSourceName          (this.sourceName)
                //.setSourceSchema        (this.sourceSchema)
                .setResultSchema        (resultSchema)
                .setSystemResultSchema  (this.projectionAttributes)
                .setFilterPredicate     (this.sourceFilter)
                .setIsThisSubquery      (false)
                .setSourceSubqueries    (this.subquery)
                .setPredicateSubqueries (this.predicateSubqueries)
                .setOperations(this.operations)
                .setResultSourceIndexes(indexesOfResultAttributes)
                .setPredicateVarToIndexes(predVarToIndex)
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

        if(operations.size() < finalUserSchema.size()){
            for(int i = operations.size(); i<finalUserSchema.size(); i++){
                operations.add(o -> o);
            }
        }

        List<Integer> groupAttributesIndexes = new ArrayList<>();
        for(String groupAttribute: systemSelectFields){
            int index = sourceSchema.indexOf(groupAttribute);
            if(index != -1){
                groupAttributesIndexes.add(index);
            }else{
                throw new RuntimeException("Group attribute " + groupAttribute + " is not in the source's schema");
            }
        }
        List<Pair<Integer, Integer>> aggregatesOPsToIndexes = new ArrayList<>();
        for(Pair<Integer, String> aggregate: this.aggregates){
            int OP = aggregate.getValue0();
            String name = aggregate.getValue1();
            int index = sourceSchema.indexOf(name);
            if(index != -1){
                aggregatesOPsToIndexes.add(new Pair<>(OP, index));
            }else{
                throw new RuntimeException("Group attribute " + name + " is not in the source's schema");
            }
        }


        List<Pair<String, Integer>> predicateVarToIndexes = new ArrayList<>();
        if(sourceFilter != null && !sourceFilter.isEmpty()){
            for(String attrName: sourceSchema){
                if(sourceFilter.contains(sourceName+"."+attrName)){
                    sourceFilter = sourceFilter.replace(sourceName+"."+attrName, sourceName+(attrName.replace(".", "")));
                    predicateVarToIndexes.add(new Pair<>((sourceName+(attrName.replace(".", ""))), sourceName.indexOf(attrName)));
                }
                if(sourceFilter.contains(attrName)){
                    if(attrName.contains(".")){
                        sourceFilter = sourceFilter.replace(attrName, attrName.replace(".", ""));
                        predicateVarToIndexes.add(new Pair<>(attrName.replace(".", ""), sourceSchema.indexOf(attrName)));
                    }else{
                        predicateVarToIndexes.add(new Pair<>(attrName, sourceSchema.indexOf(attrName)));
                    }
                }
            }
            for(String userAlias: userResultsSchema){
                if (sourceFilter.contains(userAlias)){
                    int index = this.sourceSchema.indexOf(this.projectionAttributes.get(userResultsSchema.indexOf(userAlias)));
                    if(index == -1){
                        throw new RuntimeException("ERROR: FATAL");
                    }
                    predicateVarToIndexes.add(new Pair<>(userAlias, index));
                }
            }
        }


        AggregateQuery query = new AggregateQuery()
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
                .setOperations(this.operations)
                .setAggregatesOPToIndex(aggregatesOPsToIndexes)
                .setGroupAttributesIndexes(groupAttributesIndexes)
                .setPredicateVarToIndexes(predicateVarToIndexes)
                ;
        if(this.isStored){
            query = query.setStored();
        }
        ReadQuery readQuery = new ReadQuery().setAggregateQuery(query).setResultSchema(finalUserSchema);
        if(this.isStored){
            readQuery = readQuery.setStored();
        }
        return readQuery;
    }
    private ReadQuery buildSimpleAggregateQuery(){
        if(operations.size() < aggregateUserAttributesNames.size()){
            for(int i = operations.size(); i<aggregateUserAttributesNames.size(); i++){
                operations.add(o -> o);
            }
        }

        List<Pair<Integer, Integer>> aggregatesOPsToIndexes = new ArrayList<>();
        for(Pair<Integer, String> operation: this.aggregates){
            Integer op = operation.getValue0();
            String attributeName = operation.getValue1();
            int index = this.sourceSchema.indexOf(attributeName);
            if(index != -1){
                aggregatesOPsToIndexes.add(new Pair<>(op, index));
            }else{
                throw new RuntimeException("Attribute " + attributeName + " is not part of the schema of the source");
            }
        }

        List<Pair<String, Integer>> predicateVarToIndexes = new ArrayList<>();
        if(sourceFilter != null && !sourceFilter.isEmpty()){
            for(String attrName: sourceSchema){
                if(sourceFilter.contains(sourceName+"."+attrName)){
                    sourceFilter = sourceFilter.replace(sourceName+"."+attrName, sourceName+(attrName.replace(".", "")));
                    predicateVarToIndexes.add(new Pair<>((sourceName+(attrName.replace(".", ""))), sourceName.indexOf(attrName)));
                }
                if(sourceFilter.contains(attrName)){
                    if(attrName.contains(".")){
                        sourceFilter = sourceFilter.replace(attrName, attrName.replace(".", ""));
                        predicateVarToIndexes.add(new Pair<>(attrName.replace(".", ""), sourceSchema.indexOf(attrName)));
                    }else{
                        predicateVarToIndexes.add(new Pair<>(attrName, sourceSchema.indexOf(attrName)));
                    }
                }
            }
            for(String userAlias: userResultsSchema){
                if (sourceFilter.contains(userAlias)){
                    int index = this.sourceSchema.indexOf(this.projectionAttributes.get(userResultsSchema.indexOf(userAlias)));
                    if(index == -1){
                        throw new RuntimeException("ERROR: FATAL");
                    }
                    predicateVarToIndexes.add(new Pair<>(userAlias, index));
                }
            }
        }

        SimpleAggregateQuery query = new SimpleAggregateQuery()
                .setSourceName(this.sourceName)
                .setSourceIsTable(this.sourceIsTable)
                //.setSourceSchema(this.sourceSchema)
                .setResultsSchema(aggregateUserAttributesNames)
                .setAggregatesSpecification(this.aggregates)
                .setFilterPredicate(this.sourceFilter)
                .setSourceSubqueries(this.subquery)
                .setIsThisSubquery(false)
                .setPredicateSubqueries(this.predicateSubqueries)
                .setOperations(this.operations)
                .setAggregatesOPsToIndexes(aggregatesOPsToIndexes)
                .setPredicateVarToIndexes(predicateVarToIndexes)
                ;
        if(this.isStored){
            query = query.setStored();
        }
        ReadQuery readQuery = new ReadQuery().setResultSchema(aggregateUserAttributesNames).setSimpleAggregateQuery(query);
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