package edu.stanford.futuredata.uniserve.relationalapi.querybuilders;

import edu.stanford.futuredata.uniserve.broker.Broker;
import edu.stanford.futuredata.uniserve.relationalapi.JoinQuery;
import edu.stanford.futuredata.uniserve.relationalapi.ReadQuery;
import edu.stanford.futuredata.uniserve.utilities.TableInfo;

import java.util.*;

public class JoinQueryBuilder {
    private Broker broker;

    private String sourceOne = "";
    private String sourceTwo = "";
    private Map<String, ReadQuery> subqueries = new HashMap<>();
    private boolean sourceOneTable = true;
    private boolean sourceTwoTable = true;
    private Map<String, List<String>> sourcesSchemas = new HashMap<>();
    private Map<String, List<String>> joinAttributes = new HashMap<>();
    private Map<String, String> filterPredicates = new HashMap<>();

    private List<String> resultUserSchema = new ArrayList<>();
    private List<String> resultSystemSchema = new ArrayList<>();

    private boolean distinct = false;
    private boolean isStored = false;

    private Map<String, ReadQuery> predicateSubqueries = new HashMap<>();

    public JoinQueryBuilder(Broker broker){
        this.broker = broker;
    }

    public JoinQueryBuilder sources(String tableOne, String tableTwo, String filterOne, String filterTwo, List<String> joinAttributesOne, List<String> joinAttributesTwo){
        sourceOneTable = true;
        sourceTwoTable = true;
        if(tableOne == null || tableTwo == null){
            throw new RuntimeException("Table name is null");
        }
        TableInfo t1 = broker.getTableInfo(tableOne);
        TableInfo t2 = broker.getTableInfo(tableTwo);
        if(t1.equals(Broker.NIL_TABLE_INFO) || t2.equals(Broker.NIL_TABLE_INFO)){
            throw new RuntimeException("Join request for non-existing table");
        }
        sourceOne = tableOne;
        sourceTwo = tableTwo;
        List<String> schemaOne = t1.getAttributeNames();
        List<String> schemaTwo = t2.getAttributeNames();
        for(String joinAttr1: joinAttributesOne){
            if(!schemaOne.contains(joinAttr1)){
                throw new RuntimeException("No match for attribute " + joinAttr1 + " in table " + tableOne);
            }
        }
        for(String joinAttr1: joinAttributesTwo){
            if(!schemaTwo.contains(joinAttr1)){
                throw new RuntimeException("No match for attribute " + joinAttr1 + " in table " + tableTwo);
            }
        }
        sourcesSchemas.put(tableOne, schemaOne);
        sourcesSchemas.put(tableTwo, schemaTwo);
        joinAttributes.put(tableOne, joinAttributesOne);
        joinAttributes.put(tableTwo, joinAttributesTwo);
        if(filterOne != null && !filterOne.isEmpty()){
            filterPredicates.put(tableOne, filterOne);
        }
        if(filterTwo != null && !filterTwo.isEmpty()){
            filterPredicates.put(tableTwo, filterTwo);
        }
        return this;
    }
    public JoinQueryBuilder sources(ReadQuery subqueryOne, String tableTwo, String aliasOne, String filterOne, String filterTwo, List<String> joinAttributesOne, List<String> joinAttributesTwo){
        sourceOneTable = false;
        sourceTwoTable = true;
        if(subqueryOne == null){
            throw new RuntimeException("subquery is null");
        }
        if(tableTwo == null){
            throw new RuntimeException("Table name is null");
        }
        if(aliasOne == null || aliasOne.isEmpty()){
            throw new RuntimeException("Alias for subquery is not specified");
        }
        TableInfo t2 = broker.getTableInfo(tableTwo);
        if(t2.equals(Broker.NIL_TABLE_INFO)){
            throw new RuntimeException("Table " +tableTwo+" is not registered");
        }
        sourceOne = aliasOne;
        sourceTwo = tableTwo;
        List<String> schemaOne = subqueryOne.getResultSchema();
        List<String> schemaTwo = t2.getAttributeNames();

        for(String joinAttr1: joinAttributesOne){
            if(!schemaOne.contains(joinAttr1)){
                throw new RuntimeException("No match for attribute " + joinAttr1 + " in subquery " + aliasOne);
            }
        }
        for(String joinAttr1: joinAttributesTwo){
            if(!schemaTwo.contains(joinAttr1)){
                throw new RuntimeException("No match for attribute " + joinAttr1 + " in table " + tableTwo);
            }
        }
        sourcesSchemas.put(aliasOne, schemaOne);
        sourcesSchemas.put(tableTwo, schemaTwo);
        joinAttributes.put(aliasOne, joinAttributesOne);
        joinAttributes.put(tableTwo, joinAttributesTwo);
        if(filterOne != null && !filterOne.isEmpty()){
            filterPredicates.put(aliasOne, filterOne);
        }
        if(filterTwo != null && !filterTwo.isEmpty()){
            filterPredicates.put(tableTwo, filterTwo);
        }
        subqueries.put(aliasOne, subqueryOne);
        return this;
    }
    public JoinQueryBuilder sources(String tableOne, ReadQuery subqueryTwo, String aliasTwo, String filterOne, String filterTwo, List<String> joinAttributesOne, List<String> joinAttributesTwo){
        sourceOneTable = true;
        sourceTwoTable = false;
        if(subqueryTwo == null){
            throw new RuntimeException("subquery is null");
        }
        if(tableOne == null || tableOne.isEmpty()){
            throw new RuntimeException("Table name is null");
        }
        if(aliasTwo == null || aliasTwo.isEmpty()){
            throw new RuntimeException("Alias for subquery is not specified");
        }
        TableInfo t1 = broker.getTableInfo(tableOne);
        if(t1.equals(Broker.NIL_TABLE_INFO)){
            throw new RuntimeException("Table " + tableOne + " is not registered");
        }
        sourceOne = tableOne;
        sourceTwo = aliasTwo;
        List<String> schemaOne = t1.getAttributeNames();
        List<String> schemaTwo = subqueryTwo.getResultSchema();

        for(String joinAttr1: joinAttributesOne){
            if(!schemaOne.contains(joinAttr1)){
                throw new RuntimeException("No match for attribute " + joinAttr1 + " in table " + tableOne);
            }
        }
        for(String joinAttr1: joinAttributesTwo){
            if(!schemaTwo.contains(joinAttr1)){
                throw new RuntimeException("No match for attribute " + joinAttr1 + " in subquery " + aliasTwo + "\nSubquery schema: " + schemaTwo);
            }
        }
        sourcesSchemas.put(tableOne, schemaOne);
        sourcesSchemas.put(aliasTwo, schemaTwo);
        joinAttributes.put(tableOne, joinAttributesOne);
        joinAttributes.put(aliasTwo, joinAttributesTwo);
        if(filterOne != null && !filterOne.isEmpty()){
            filterPredicates.put(tableOne, filterOne);
        }
        if(filterTwo != null && !filterTwo.isEmpty()){
            filterPredicates.put(aliasTwo, filterTwo);
        }
        subqueries.put(aliasTwo, subqueryTwo);
        return this;
    }
    public JoinQueryBuilder sources(ReadQuery subqueryOne, ReadQuery subqueryTwo, String aliasOne, String aliasTwo, String filterOne, String filterTwo, List<String> joinAttributesOne, List<String> joinAttributesTwo){
        sourceOneTable = false;
        sourceTwoTable = false;
        if(subqueryTwo == null || subqueryOne == null){
            throw new RuntimeException("subquery is null");
        }
        if(aliasTwo == null || aliasTwo.isEmpty() || aliasOne == null || aliasOne.isEmpty()){
            throw new RuntimeException("Alias for subquery is not specified");
        }
        sourceOne = aliasOne;
        sourceTwo = aliasTwo;
        List<String> schemaOne = subqueryOne.getResultSchema();
        List<String> schemaTwo = subqueryTwo.getResultSchema();

        for(String joinAttr1: joinAttributesOne){
            if(!schemaOne.contains(joinAttr1)){
                throw new RuntimeException("No match for attribute " + joinAttr1 + " in subquery " + aliasOne);
            }
        }
        for(String joinAttr1: joinAttributesTwo){
            if(!schemaTwo.contains(joinAttr1)){
                throw new RuntimeException("No match for attribute " + joinAttr1 + " in table " + aliasTwo);
            }
        }
        sourcesSchemas.put(aliasOne, schemaOne);
        sourcesSchemas.put(aliasTwo, schemaTwo);
        joinAttributes.put(aliasOne, joinAttributesOne);
        joinAttributes.put(aliasTwo, joinAttributesTwo);
        if(filterOne != null && !filterOne.isEmpty()){
            filterPredicates.put(aliasOne, filterOne);
        }
        if(filterTwo != null && !filterTwo.isEmpty()){
            filterPredicates.put(aliasTwo, filterTwo);
        }
        if(aliasTwo.equals(aliasOne)){
            throw new RuntimeException("Subqueries cannot have the same alias");
        }
        subqueries.put(aliasOne, subqueryOne);
        subqueries.put(aliasTwo, subqueryTwo);
        return this;
    }

    public JoinQueryBuilder predicateSubquery(String alias, ReadQuery subquery){
        if(alias == null || alias.isEmpty() || subqueries.containsKey(alias) || sourceOne.equals(alias) || sourceTwo.equals(alias)){
            throw new RuntimeException("Invalid alias for predicate subquery");
        }
        predicateSubqueries.put(alias, subquery);
        return this;
    }

    public JoinQueryBuilder alias(String... resultSchema){
        resultUserSchema.addAll(Arrays.asList(resultSchema));
        for(int i=0; i<resultUserSchema.size()-1;i++){
            for(int j=i+1; j<resultUserSchema.size(); j++){
                if(resultUserSchema.get(i).equals(resultUserSchema.get(j))){
                    throw new RuntimeException("Duplicate aliases for different attributes");
                }
            }
        }
        return this;
    }
    public JoinQueryBuilder distinct(){
        this.distinct = true;
        return this;
    }
    public JoinQueryBuilder stored(){
        this.isStored = true;
        return this;
    }

    public ReadQuery build(){
        List<String> systemSourceOneSchema = new ArrayList<>();
        List<String> systemSourceTwoSchema = new ArrayList<>();
        for(String att: sourcesSchemas.get(sourceOne)){
            systemSourceOneSchema.add(sourceOne+"."+att);
        }for(String att: sourcesSchemas.get(sourceTwo)){
            systemSourceTwoSchema.add(sourceTwo+"."+att);
        }

        for(Map.Entry<String, ReadQuery> entry: subqueries.entrySet()){
            entry.getValue().setIsThisSubquery(true);
        }

        resultSystemSchema.addAll(systemSourceOneSchema);
        resultSystemSchema.addAll(systemSourceTwoSchema);
        if(resultUserSchema.size() < resultSystemSchema.size()){
            for(int i = resultUserSchema.size(); i < resultSystemSchema.size(); i++){
                resultUserSchema.add(resultSystemSchema.get(i));
            }
        } else if (resultUserSchema.size() > resultSystemSchema.size()) {
            resultUserSchema.subList(resultSystemSchema.size(), resultUserSchema.size()).clear();
        }

        JoinQuery query = new JoinQuery()
                .setSourceOne(sourceOne)
                .setSourceTwo(sourceTwo)
                .setTableFlags(sourceOneTable, sourceTwoTable)
                .setSourceSchemas(sourcesSchemas)
                .setResultSchema(resultUserSchema)
                .setSystemResultSchema(resultSystemSchema)
                .setSourcesJoinAttributes(joinAttributes)
                .setFilterPredicates(filterPredicates)
                .setSourceSubqueries(subqueries)
                .setIsThisSubquery(false)
                .setPredicateSubqueries(predicateSubqueries)
                ;
        if(this.distinct){
            query = query.setDistinct();
        }
        if(this.isStored){
            query = query.setStored();
        }
        ReadQuery readQuery =  new ReadQuery().setJoinQuery(query).setResultSchema(resultUserSchema);
        if(this.isStored){
            readQuery = readQuery.setStored();
        }
        return readQuery;
    }
}
