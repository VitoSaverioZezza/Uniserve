package edu.stanford.futuredata.uniserve.relationalapi;

import edu.stanford.futuredata.uniserve.broker.Broker;
import edu.stanford.futuredata.uniserve.relational.RelReadQueryResults;
import edu.stanford.futuredata.uniserve.relationalapi.querybuilders.ANewReadQueryBuilder;
import edu.stanford.futuredata.uniserve.utilities.TableInfo;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;

public class ReadQuery implements Serializable {
    private static final Logger logger = LoggerFactory.getLogger(ReadQuery.class);

    private FilterAndProjectionQuery filterAndProjectionQuery = null;
    private AnotherSimpleAggregateQuery anotherSimpleAggregateQuery = null;
    private JoinQuery joinQuery = null;
    private AnotherAggregateQuery anotherAggregateQuery = null;

    private List<String> resultSchema = new ArrayList<>();

    private boolean stored = false;
    private String resultTableName = "";
    private Boolean[] keyStructure;


    public ReadQuery setFilterAndProjectionQuery(FilterAndProjectionQuery filterAndProjectionQuery) {
        this.filterAndProjectionQuery = filterAndProjectionQuery;
        return this;
    }
    public ReadQuery setAnotherAggregateQuery(AnotherAggregateQuery aggregateQuery) {
        this.anotherAggregateQuery = aggregateQuery;
        return this;
    }
    public ReadQuery setJoinQuery(JoinQuery query){
        this.joinQuery = query;
        return this;
    }
    public ReadQuery setAnotherSimpleAggregateQuery(AnotherSimpleAggregateQuery anotherSimpleAggregateQuery) {
        this.anotherSimpleAggregateQuery = anotherSimpleAggregateQuery;
        return this;
    }

    public ReadQuery setStored(){
        this.stored = true;
        return this;
    }
    public ReadQuery setResultTableName(String resultTableName) {
        this.resultTableName = resultTableName;
        return this;
    }

    public ReadQuery setIsThisSubquery(boolean isThisSubquery){
        if(filterAndProjectionQuery != null){
            filterAndProjectionQuery.setIsThisSubquery(isThisSubquery);
        } else if (anotherSimpleAggregateQuery != null) {
            anotherSimpleAggregateQuery.setIsThisSubquery(isThisSubquery);
        } else if (anotherAggregateQuery != null) {
            anotherAggregateQuery.setIsThisSubquery(isThisSubquery);
        } else if (joinQuery != null){
            joinQuery.setIsThisSubquery(isThisSubquery);
        } else {
            throw new RuntimeException("No valid query is defined");
        }
        return this;
    }
    public ReadQuery setResultSchema(List<String> resultSchema) {
        this.resultSchema = resultSchema;
        return this;
    }

    public List<String> getResultSchema() {
        return resultSchema;
    }
    public String getResultTableName() {
        return resultTableName;
    }
    public Boolean[] getKeyStructure() {
        return keyStructure;
    }

    public RelReadQueryResults run(Broker broker){
        if(filterAndProjectionQuery == null && joinQuery == null && anotherAggregateQuery == null && anotherSimpleAggregateQuery == null){
            throw new RuntimeException("No valid query is defined");
        }
        RelReadQueryResults results = new RelReadQueryResults();
        String registeredTableResults = queryMatch(broker);
        if(!registeredTableResults.isEmpty()){
            logger.info("query matches an already registered query");
            ReadQuery rq = new ANewReadQueryBuilder(broker).select().from(registeredTableResults).build();
            if(filterAndProjectionQuery != null && filterAndProjectionQuery.isThisSubquery()){
                rq.setIsThisSubquery(true);
            } else if (anotherSimpleAggregateQuery != null && anotherSimpleAggregateQuery.isThisSubquery()) {
                rq.setIsThisSubquery(true);
            } else if (anotherAggregateQuery != null && anotherAggregateQuery.isThisSubquery()) {
                rq.setIsThisSubquery(true);
            } else if (joinQuery.isThisSubquery()) {
                rq.setIsThisSubquery(true);
            }
            results = rq.run(broker);
        }else {

            if(stored){
                logger.info("Query needs to be registered");
                keyStructure = new Boolean[resultSchema.size()];
                Arrays.fill(keyStructure, true);
                //What does this method do? Creates the table and registers the query to all sources
                this.resultTableName = broker.registerQuery(this);
            }

            if (filterAndProjectionQuery != null) {
                results = broker.retrieveAndCombineReadQuery(filterAndProjectionQuery);
            } else if (anotherSimpleAggregateQuery != null) {
                results = broker.shuffleReadQuery(anotherSimpleAggregateQuery);
            } else if (anotherAggregateQuery != null) {
                results = broker.shuffleReadQuery(anotherAggregateQuery);
            } else {
                results = broker.shuffleReadQuery(joinQuery);
            }
        }
        results.setFieldNames(resultSchema);
        return results;
    }
    public RelReadQueryResults updateStoredResults(Broker broker){

        return null;
    }

    @Override
    public boolean equals(Object obj){
        System.out.println("RQ.equals ----- this should not be called, like, never");
        if(!(obj instanceof ReadQuery)){
            return false;
        }
        ReadQuery readQuery = (ReadQuery) obj;
        for(String source: this.getSourceTables()){
            if(!readQuery.getSourceTables().contains(source)){
                return false;
            }
        }
        List<ReadQuery> concreteSubqueriesInput = new ArrayList<>(readQuery.getConcreteSubqueries().values());
        List<ReadQuery> concreteSubqueriesThis = new ArrayList<>(this.getConcreteSubqueries().values());
        if(concreteSubqueriesInput.size() != concreteSubqueriesThis.size())
            return false;
        for(int i = 0; i<concreteSubqueriesInput.size(); i++){
            ReadQuery subqInput = concreteSubqueriesInput.get(i);
            boolean match = false;
            for(int j = 0; j<concreteSubqueriesThis.size() && !match; j++){
                ReadQuery subqThis = concreteSubqueriesThis.get(j);
                if(subqThis.equals(subqInput)){
                    match = true;
                }
            }
            if(!match){
                return false;
            }
        }
        List<ReadQuery> volatileSubqueriesInput = new ArrayList<>(readQuery.getVolatileSubqueries().values());
        List<ReadQuery> volatileSubqueriesThis = new ArrayList<>(this.getVolatileSubqueries().values());
        if(volatileSubqueriesInput.size() != concreteSubqueriesThis.size())
            return false;
        for(int i = 0; i<volatileSubqueriesInput.size(); i++){
            ReadQuery subqInput = volatileSubqueriesInput.get(i);
            boolean match = false;
            for(int j = 0; j<volatileSubqueriesThis.size() && !match; j++){
                ReadQuery subqThis = volatileSubqueriesThis.get(j);
                if(subqThis.equals(subqInput)){
                    match = true;
                }
            }
            if(!match){
                return false;
            }
        }
        List<Pair<Integer, String>> aggregatesThis = this.getAggregates();
        List<Pair<Integer, String>> aggregatesInput = readQuery.getAggregates();
        if(aggregatesInput.size() != aggregatesThis.size()){
            return false;
        }
        for(int i = 0; i<aggregatesThis.size(); i++){
            if(!aggregatesThis.get(i).equals(aggregatesInput.get(i))){
                return false;
            }
        }
        List<String> systemResultSchemaInput =  readQuery.getSystemFinalSchema();
        List<String> systemResultSchemaThis = this.getSystemFinalSchema();
        if(!systemResultSchemaThis.equals(systemResultSchemaInput)){
            return false;
        }
        List<String> thisPredicates = this.getPredicate();
        List<String> inputPredicates = readQuery.getPredicate();
        if(thisPredicates.size() != inputPredicates.size()){
            return false;
        }
        for(int i = 0; i<thisPredicates.size(); i++){
            if(!thisPredicates.get(i).equals(inputPredicates.get(i))){
                return false;
            }
        }
        return true;
    }
    private String queryMatch(Broker broker){
        TableInfo aSourceTableInfo = broker.getTableInfo(new ArrayList<>(this.getSourceTables()).get(0));
        ArrayList<ReadQuery> alreadyRegisteredQueries = aSourceTableInfo.getRegisteredQueries();
        for(ReadQuery registeredQuery: alreadyRegisteredQueries){
            if(this.equals(registeredQuery)){
                return registeredQuery.getResultTableName();
            }
        }
        return "";
    }

    public Set<String> getSourceTables(){
        if(filterAndProjectionQuery != null){
            Set<String> sourceTables = new HashSet<>(filterAndProjectionQuery.getTableNames());
            Map<String, ReadQuery> volatileSubqueries = filterAndProjectionQuery.getVolatileSubqueries();
            Map<String, ReadQuery> concreteSubqueries = filterAndProjectionQuery.getConcreteSubqueries();

            for(ReadQuery subquery: volatileSubqueries.values()){
                sourceTables.addAll(subquery.getSourceTables());
            }
            for(ReadQuery subquery: concreteSubqueries.values()){
                sourceTables.addAll(subquery.getSourceTables());
            }
            return sourceTables;
        }else if(anotherSimpleAggregateQuery != null) {
            Set<String> sourceTables = new HashSet<>(anotherSimpleAggregateQuery.getQueriedTables());
            Map<String, ReadQuery> volatileSubqueries = anotherSimpleAggregateQuery.getVolatileSubqueries();
            Map<String, ReadQuery> concreteSubqueries = anotherSimpleAggregateQuery.getConcreteSubqueries();

            for(ReadQuery subquery: volatileSubqueries.values()){
                sourceTables.addAll(subquery.getSourceTables());
            }
            for(ReadQuery subquery: concreteSubqueries.values()){
                sourceTables.addAll(subquery.getSourceTables());
            }
            return sourceTables;
        } else if (joinQuery != null) {
            Set<String> sourceTables = new HashSet<>(joinQuery.getQueriedTables());
            Map<String, ReadQuery> volatileSubqueries = joinQuery.getVolatileSubqueries();
            Map<String, ReadQuery> concreteSubqueries = joinQuery.getConcreteSubqueries();

            for(ReadQuery subquery: volatileSubqueries.values()){
                sourceTables.addAll(subquery.getSourceTables());
            }
            for(ReadQuery subquery: concreteSubqueries.values()){
                sourceTables.addAll(subquery.getSourceTables());
            }
            return sourceTables;
        } else {
            Set<String> sourceTables = new HashSet<>(anotherAggregateQuery.getQueriedTables());
            Map<String, ReadQuery> volatileSubqueries = anotherAggregateQuery.getVolatileSubqueries();
            Map<String, ReadQuery> concreteSubqueries = anotherAggregateQuery.getConcreteSubqueries();

            for(ReadQuery subquery: volatileSubqueries.values()){
                sourceTables.addAll(subquery.getSourceTables());
            }
            for(ReadQuery subquery: concreteSubqueries.values()){
                sourceTables.addAll(subquery.getSourceTables());
            }
            return sourceTables;
        }
    }
    public Map<String, ReadQuery> getVolatileSubqueries(){
        Map<String, ReadQuery> subqueries = new HashMap<>();
        if(filterAndProjectionQuery != null){
            subqueries.putAll(filterAndProjectionQuery.getVolatileSubqueries());
        }else if(anotherSimpleAggregateQuery != null){
            subqueries.putAll(anotherSimpleAggregateQuery.getVolatileSubqueries());
        }else if(anotherAggregateQuery != null){
            subqueries.putAll(anotherAggregateQuery.getVolatileSubqueries());
        }else{
            subqueries.putAll(joinQuery.getVolatileSubqueries());
        }
        return subqueries;
    }
    public Map<String, ReadQuery> getConcreteSubqueries(){
        Map<String, ReadQuery> subqueries = new HashMap<>();
        if(filterAndProjectionQuery != null){
            subqueries.putAll(filterAndProjectionQuery.getConcreteSubqueries());
        }else if(anotherSimpleAggregateQuery != null){
            subqueries.putAll(anotherSimpleAggregateQuery.getConcreteSubqueries());
        }else if(anotherAggregateQuery != null){
            subqueries.putAll(anotherAggregateQuery.getConcreteSubqueries());
        }else{
            subqueries.putAll(joinQuery.getConcreteSubqueries());
        }
        return subqueries;
    }
    public List<Pair<Integer, String>> getAggregates(){
        if(filterAndProjectionQuery != null || joinQuery != null){
            return new ArrayList<>();
        }else if(anotherSimpleAggregateQuery != null){
            return anotherSimpleAggregateQuery.getAggregatesSpecification();
        }else{
            return anotherAggregateQuery.getAggregatesSpecification();
        }
    }
    public List<String> getSystemFinalSchema(){
        if(filterAndProjectionQuery != null){
            return filterAndProjectionQuery.getSystemResultSchema();
        } else if (anotherSimpleAggregateQuery!=null) {
            return new ArrayList<>();
        }else{
            return anotherAggregateQuery.getSystemSelectedFields();
        }
    }
    public List<String> getPredicate(){
        if(filterAndProjectionQuery != null){
            return filterAndProjectionQuery.getPredicates();
        } else if (anotherSimpleAggregateQuery!=null) {
            return anotherSimpleAggregateQuery.getPredicates();
        }else if(anotherAggregateQuery != null){
            return anotherAggregateQuery.getPredicates();
        }else{
            return joinQuery.getPredicates();
        }
    }


    public ReadQuery setSimpleQuery(ProdSelProjQuery simpleQuery) {

        return this;
    }
    public ReadQuery setAggregateQuery(AggregateQuery aggregateQuery) {

        return this;
    }
    public ReadQuery setSimpleAggregateQuery(SimpleAggregateQuery simpleAggregateQuery) {

        return this;
    }
}
