package edu.stanford.futuredata.uniserve.relationalapi.querybuilders;

import edu.stanford.futuredata.uniserve.broker.Broker;
import edu.stanford.futuredata.uniserve.relational.RelReadQueryResults;
import edu.stanford.futuredata.uniserve.relationalapi.ReadQuery;
import edu.stanford.futuredata.uniserve.relationalapi.SerializablePredicate;
import edu.stanford.futuredata.uniserve.relationalapi.UnionQuery;
import edu.stanford.futuredata.uniserve.utilities.TableInfo;

import java.lang.reflect.Array;
import java.util.*;

public class UnionQueryBuilder {
    Broker broker;

    private String sourceOne = "";
    private String sourceTwo = "";
    private boolean sourceOneTable = true;
    private boolean sourceTwoTable = true;
    private final HashMap<String, ReadQuery> subqueries = new HashMap<>();
    private String filterPredicateOne = "";
    private String filterPredicateTwo = "";
    private ArrayList<String> finalSystemSchema = new ArrayList<>();
    private ArrayList<String> finalUserSchema = new ArrayList<>();
    private boolean stored = false;
    private boolean distinct = false;
    private ArrayList<SerializablePredicate> operations = new ArrayList<>();
    private Map<String, ReadQuery> predicateSubqueries = new HashMap<>();

    public UnionQueryBuilder sources(String tableOne, String tableTwo){
        this.sourceOneTable = true;
        this.sourceTwoTable = true;
        this.sourceOne = tableOne;
        this.sourceTwo = tableTwo;
        return this;
    }
    public UnionQueryBuilder sources(ReadQuery subqueryOne, String tableTwo, String aliasSubqueryOne){
        this.sourceOneTable = false;
        this.sourceTwoTable = true;
        this.sourceOne = aliasSubqueryOne;
        this.sourceTwo = tableTwo;
        subqueryOne.setIsThisSubquery(true);
        subqueries.put(aliasSubqueryOne, subqueryOne);
        return this;
    }
    public UnionQueryBuilder sources(String tableOne, ReadQuery subqueryTwo, String aliasSubqueryTwo){
        this.sourceOneTable = true;
        this.sourceTwoTable = false;
        this.sourceOne = tableOne;
        this.sourceTwo = aliasSubqueryTwo;
        subqueryTwo.setIsThisSubquery(true);
        subqueries.put(aliasSubqueryTwo, subqueryTwo);

        return this;
    }
    public UnionQueryBuilder sources(ReadQuery subqueryOne, ReadQuery subqueryTwo, String aliasSubqueryOne, String aliasSubqueryTwo){
        this.sourceOneTable = false;
        this.sourceTwoTable = false;
        this.sourceOne = aliasSubqueryOne;
        this.sourceTwo = aliasSubqueryTwo;
        subqueryOne.setIsThisSubquery(true);
        subqueryTwo.setIsThisSubquery(true);
        subqueries.put(aliasSubqueryOne, subqueryOne);
        subqueries.put(aliasSubqueryTwo, subqueryTwo);
        return this;
    }
    public UnionQueryBuilder filter(String filterSrcOne, String filterSrcTwo){
        this.filterPredicateOne = filterSrcOne;
        this.filterPredicateTwo = filterSrcTwo;
        return this;
    }
    public UnionQueryBuilder alias(String... finalSchema){
        this.finalUserSchema.addAll(Arrays.asList(finalSchema));
        return this;
    }
    public UnionQueryBuilder distinct(Boolean distinct){
        this.distinct = distinct;
        return this;
    }
    public UnionQueryBuilder stored(Boolean stored){
        this.stored = stored;
        return this;
    }
    public UnionQueryBuilder apply(SerializablePredicate... operations){
        this.operations.addAll(Arrays.asList(operations));
        return this;
    }

    public UnionQueryBuilder predicateSubquery(String alias, ReadQuery subquery){
        if(alias == null || alias.isEmpty() || subqueries.containsKey(alias) || sourceOne.equals(alias) || sourceTwo.equals(alias)){
            throw new RuntimeException("Invalid alias for predicate subquery");
        }
        predicateSubqueries.put(alias, subquery);
        return this;
    }

    public UnionQueryBuilder(Broker broker){
        this.broker = broker;
    }


    public ReadQuery build(){
        List<String> schemaSourceOne = sourceOneTable ?
                broker.getTableInfo(sourceOne).getAttributeNames() :
                subqueries.get(sourceOne).getResultSchema();
        List<String> schemaSourceTwo = sourceTwoTable ?
                broker.getTableInfo(sourceTwo).getAttributeNames() :
                subqueries.get(sourceTwo).getResultSchema();

        if(schemaSourceTwo == null || schemaSourceOne == null){
            throw new RuntimeException("ERROR, sources' schema is not well defined");
        }
        if(schemaSourceTwo.size() != schemaSourceOne.size()){
            throw new RuntimeException("Incompatible schema, sources schemas have different sizes");
        }

        List<String> parsedUserFinalSchema = new ArrayList<>();
        if(finalUserSchema.isEmpty()){
            parsedUserFinalSchema.addAll(schemaSourceOne);
        }else {
            for (int i = 0; i < schemaSourceOne.size(); i++) {
                if (i < finalUserSchema.size()) {
                    String userName = finalUserSchema.get(i);
                    if (userName != null && !userName.isEmpty()) {
                        parsedUserFinalSchema.add(userName);
                    } else {
                        parsedUserFinalSchema.add(schemaSourceOne.get(i));
                    }
                } else {
                    parsedUserFinalSchema.addAll(schemaSourceOne.subList(i, schemaSourceOne.size()));
                }
            }
        }

        List<SerializablePredicate> parsedOperations = new ArrayList<>();
        if(!operations.isEmpty()){
            if(operations.size() > parsedUserFinalSchema.size()){
                for(int i = 0; i<parsedUserFinalSchema.size(); i++){
                    parsedOperations.add(operations.get(i));
                }
            } else if (operations.size() < parsedUserFinalSchema.size()) {
                parsedOperations.addAll(operations);
                for(int i = parsedOperations.size(); i<parsedUserFinalSchema.size(); i++){
                    parsedOperations.add(o->o);
                }
            }else{
                parsedOperations.addAll(operations);
            }
        }




        UnionQuery resultQuery = new UnionQuery()
                .setSourceOne(sourceOne)
                .setSourceTwo(sourceTwo)
                .setTableFlags(sourceOneTable, sourceTwoTable)
                .setSourceSchemas(Map.of(sourceOne, schemaSourceOne, sourceTwo, schemaSourceTwo))
                .setResultSchema(parsedUserFinalSchema)
                .setSystemResultSchema(schemaSourceOne)
                .setFilterPredicates(Map.of(sourceOne, filterPredicateOne, sourceTwo, filterPredicateTwo))
                .setSourceSubqueries(subqueries)
                .setIsThisSubquery(false)
                .setPredicateSubqueries(predicateSubqueries)
                .setOperations(parsedOperations);

        if(distinct) {
            resultQuery.setDistinct();
        }
        if(stored) {
            resultQuery.setStored();
        }
        ReadQuery readQuery =  new ReadQuery().setUnionQuery(resultQuery).setResultSchema(parsedUserFinalSchema);
        if(this.stored){
            readQuery = readQuery.setStored();
        }
        return readQuery;

    }

}
