package edu.stanford.futuredata.uniserve.relationalapi.querybuilders;

import edu.stanford.futuredata.uniserve.broker.Broker;
import edu.stanford.futuredata.uniserve.relationalapi.JoinQuery;
import edu.stanford.futuredata.uniserve.relationalapi.ReadQuery;
import edu.stanford.futuredata.uniserve.relationalapi.SerializablePredicate;
import edu.stanford.futuredata.uniserve.utilities.TableInfo;
import org.javatuples.Pair;

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

    private List<String> rawSelectArguments = new ArrayList<>();

    private List<String> resultUserSchema = new ArrayList<>();
    private List<String> resultSystemSchema = new ArrayList<>();

    private boolean distinct = false;
    private boolean isStored = false;

    private Map<String, ReadQuery> predicateSubqueries = new HashMap<>();

    private List<SerializablePredicate> operations = new ArrayList<>();

    public JoinQueryBuilder(Broker broker){
        this.broker = broker;
    }


    public JoinQueryBuilder select(String... selectArguments){
        rawSelectArguments.addAll(Arrays.asList(selectArguments));
        return this;
    }

    public JoinQueryBuilder sources(String tableOne, String tableTwo, List<String> joinAttributesOne, List<String> joinAttributesTwo){
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
        return this;
    }
    public JoinQueryBuilder sources(ReadQuery subqueryOne, String tableTwo, String aliasOne, List<String> joinAttributesOne, List<String> joinAttributesTwo){
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
        subqueries.put(aliasOne, subqueryOne);
        return this;
    }
    public JoinQueryBuilder sources(String tableOne, ReadQuery subqueryTwo, String aliasTwo, List<String> joinAttributesOne, List<String> joinAttributesTwo){
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
        subqueries.put(aliasTwo, subqueryTwo);
        return this;
    }
    public JoinQueryBuilder sources(ReadQuery subqueryOne, ReadQuery subqueryTwo, String aliasOne, String aliasTwo, List<String> joinAttributesOne, List<String> joinAttributesTwo){
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
        if(aliasTwo.equals(aliasOne)){
            throw new RuntimeException("Subqueries cannot have the same alias");
        }
        subqueries.put(aliasOne, subqueryOne);
        subqueries.put(aliasTwo, subqueryTwo);
        return this;
    }

    public JoinQueryBuilder filters(String filterOne, String filterTwo){
        if(sourceOne == null || sourceTwo == null || sourceOne.isEmpty() || sourceTwo.isEmpty()){
            throw new RuntimeException("Sources are not well defined");
        }
        if(filterOne != null && !filterOne.isEmpty()){
            filterPredicates.put(sourceOne, filterOne);
        }
        if(filterTwo != null && !filterTwo.isEmpty()){
            filterPredicates.put(sourceTwo, filterTwo);
        }
        return this;
    }

    public JoinQueryBuilder predicateSubquery(String alias, ReadQuery subquery){
        if(alias == null || alias.isEmpty() || subqueries.containsKey(alias) || sourceOne.equals(alias) || sourceTwo.equals(alias)){
            throw new RuntimeException("Invalid alias for predicate subquery");
        }
        predicateSubqueries.put(alias, subquery);
        return this;
    }

    public JoinQueryBuilder apply(SerializablePredicate... operations){
        for(SerializablePredicate operation: operations){
            if(operation == null){
                this.operations.add(o->o);
            }else {
                this.operations.add(operation);
            }
        }
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
        if(rawSelectArguments.isEmpty()) {
            List<String> systemSourceOneSchema = new ArrayList<>();
            List<String> systemSourceTwoSchema = new ArrayList<>();
            for (String att : sourcesSchemas.get(sourceOne)) {
                systemSourceOneSchema.add(sourceOne + "." + att);
            }
            for (String att : sourcesSchemas.get(sourceTwo)) {
                systemSourceTwoSchema.add(sourceTwo + "." + att);
            }

            for (Map.Entry<String, ReadQuery> entry : subqueries.entrySet()) {
                entry.getValue().setIsThisSubquery(true);
            }

            resultSystemSchema.addAll(systemSourceOneSchema);
            resultSystemSchema.addAll(systemSourceTwoSchema);
        }else{
            //check validity of user-defined system result schema
            for (String rawSelectArgument : rawSelectArguments) {
                String[] split = rawSelectArgument.split("\\.");
                if (split.length < 2) {
                    throw new RuntimeException("Invalid select argument specification in join query");
                }
                String source = split[0];
                if (source == null || source.isEmpty()) {
                    throw new RuntimeException("Invalid select argument, unspecified source in dotted notation");
                }
                if (!(source.equals(sourceOne) || source.equals(sourceTwo))) {
                    throw new RuntimeException("Specified source " + source + " is not a source in the join.");
                }
                StringBuilder stringBuilder = new StringBuilder();
                for (int i = 1; i < split.length - 1; i++) {
                    if (split[i] == null || split[i].isEmpty()) {
                        throw new RuntimeException("Invalid select argument, malformed dotted notation");
                    }
                    stringBuilder.append(split[i]);
                    stringBuilder.append(".");
                }
                stringBuilder.append(split[split.length - 1]);
                String attributeName = stringBuilder.toString();
                List<String> sourceSchema = sourcesSchemas.get(source);
                if (sourceSchema == null) {
                    throw new RuntimeException("Unavailable schema for source " + source);
                }
                if (!sourceSchema.contains(attributeName)) {
                    throw new RuntimeException(attributeName + " is not an attribute of source " + source);
                }
                resultSystemSchema.add(source+"."+attributeName);
            }
        }
        if (resultUserSchema.size() < resultSystemSchema.size()) {
            for (int i = resultUserSchema.size(); i < resultSystemSchema.size(); i++) {
                resultUserSchema.add(resultSystemSchema.get(i));
            }
        } else if (resultUserSchema.size() > resultSystemSchema.size()) {
            resultUserSchema.subList(resultSystemSchema.size(), resultUserSchema.size()).clear();
        }
        if(operations.size() < resultUserSchema.size()){
            for(int i = operations.size(); i<resultUserSchema.size(); i++){
                operations.add(o -> o);
            }
        }

        List<Pair<String, Integer>> predVarToIndexOne = new ArrayList<>();
        List<Pair<String, Integer>> predVarToIndexTwo = new ArrayList<>();

        String predOne = filterPredicates.get(sourceOne);
        String predTwo = filterPredicates.get(sourceTwo);

        if(predOne != null && !predOne.isEmpty()){
            List<String> schemaOne = sourcesSchemas.get(sourceOne);
            for(String attrName: schemaOne){
                if(predOne.contains(sourceOne+"."+attrName)){
                    predOne = predOne.replace(sourceOne+"."+attrName, sourceOne+(attrName.replace(".", "")));
                    predVarToIndexOne.add(new Pair<>((sourceOne+(attrName.replace(".", ""))), schemaOne.indexOf(attrName)));
                }
                if(predOne.contains(attrName)){
                    if(attrName.contains(".")){
                        predOne = predOne.replace(attrName, attrName.replace(".", ""));
                        predVarToIndexOne.add(new Pair<>(attrName.replace(".", ""), schemaOne.indexOf(attrName)));
                    }else{
                        predVarToIndexOne.add(new Pair<>(attrName, schemaOne.indexOf(attrName)));
                    }
                }
            }
            for(String userAlias: resultUserSchema){
                if(predOne.contains(userAlias)){
                    String correspondingSystemName = resultSystemSchema.get(resultUserSchema.indexOf(userAlias));
                    String[] split = correspondingSystemName.split("\\.");
                    StringBuilder sb = new StringBuilder();
                    for(int i = 1; i<split.length-1;i++){
                        sb.append(split[i]);
                        sb.append(".");
                    }
                    String correspondingAttributeName = sb.toString() + split[split.length-1];
                    int index = sourcesSchemas.get(sourceOne).indexOf(correspondingAttributeName);
                    if(!split[0].equals(sourceOne) || index == -1){
                        throw new RuntimeException("filter predicate of source one contains alias of " +
                                "an attribute that doesn't match source one in the prefix of the corresponding" +
                                "attribute name");
                    }
                    predVarToIndexOne.add(new Pair<>(userAlias, index));
                }
            }
            filterPredicates.put(sourceOne, predOne);
        }
        if(predTwo != null && !predTwo.isEmpty()){
            List<String> schemaTwo = sourcesSchemas.get(sourceTwo);
            for(String attrName: schemaTwo){
                if(predTwo.contains(sourceTwo+"."+attrName)){
                    predTwo = predTwo.replace(sourceTwo+"."+attrName, sourceTwo+(attrName.replace(".", "")));
                    predVarToIndexTwo.add(new Pair<>((sourceTwo+(attrName.replace(".", ""))), schemaTwo.indexOf(attrName)));
                }
                if(predTwo.contains(attrName)){
                    if(attrName.contains(".")){
                        predTwo = predTwo.replace(attrName, attrName.replace(".", ""));
                        predVarToIndexTwo.add(new Pair<>(attrName.replace(".", ""), schemaTwo.indexOf(attrName)));
                    }else{
                        predVarToIndexTwo.add(new Pair<>(attrName, schemaTwo.indexOf(attrName)));
                    }
                }
            }
            for(String userAlias: resultUserSchema){
                if(predTwo.contains(userAlias)){
                    String correspondingSystemName = resultSystemSchema.get(resultUserSchema.indexOf(userAlias));
                    String[] split = correspondingSystemName.split("\\.");
                    StringBuilder sb = new StringBuilder();
                    for(int i = 1; i<split.length-1;i++){
                        sb.append(split[i]);
                        sb.append(".");
                    }
                    String correspondingAttributeName = sb.toString() + split[split.length-1];
                    int index = sourcesSchemas.get(sourceTwo).indexOf(correspondingAttributeName);
                    if(!split[0].equals(sourceTwo) || index == -1){
                        throw new RuntimeException("filter predicate of source two contains alias of " +
                                "an attribute that doesn't match source one in the prefix of the corresponding" +
                                "attribute name");
                    }
                    predVarToIndexTwo.add(new Pair<>(userAlias, index));
                }
            }
            filterPredicates.put(sourceTwo, predTwo);
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
                .setPredicateVarToIndexesOne(predVarToIndexOne)
                .setPredicateVarToIndexesTwo(predVarToIndexTwo)
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
