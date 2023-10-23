package edu.stanford.futuredata.uniserve.relationalapi;

import edu.stanford.futuredata.uniserve.broker.Broker;
import edu.stanford.futuredata.uniserve.relational.RelReadQueryResults;
import edu.stanford.futuredata.uniserve.relationalapi.querybuilders.RelReadQueryBuilder;
import edu.stanford.futuredata.uniserve.utilities.TableInfo;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;

public class ReadQuery implements Serializable {
    private static final Logger logger = LoggerFactory.getLogger(ReadQuery.class);

    private FilterAndProjectionQuery filterAndProjectionQuery = null;
    private SimpleAggregateQuery simpleAggregateQuery = null;
    private JoinQuery joinQuery = null;
    private AggregateQuery aggregateQuery = null;
    private UnionQuery unionQuery = null;

    private List<String> resultSchema = new ArrayList<>();

    private boolean stored = false;
    private String resultTableName = "";
    private Boolean[] keyStructure;


    public ReadQuery setFilterAndProjectionQuery(FilterAndProjectionQuery filterAndProjectionQuery) {
        this.filterAndProjectionQuery = filterAndProjectionQuery;
        return this;
    }
    public ReadQuery setAggregateQuery(AggregateQuery aggregateQuery) {
        this.aggregateQuery = aggregateQuery;
        return this;
    }
    public ReadQuery setJoinQuery(JoinQuery query){
        this.joinQuery = query;
        return this;
    }
    public ReadQuery setSimpleAggregateQuery(SimpleAggregateQuery simpleAggregateQuery) {
        this.simpleAggregateQuery = simpleAggregateQuery;
        return this;
    }
    public ReadQuery setUnionQuery(UnionQuery unionQuery){
        this.unionQuery = unionQuery;
        return this;
    }
    public ReadQuery setStored(){
        this.stored = true;
        return this;
    }
    public ReadQuery setResultTableName(String resultTableName) {
        this.resultTableName = resultTableName;
        if (filterAndProjectionQuery != null) {
            filterAndProjectionQuery.setResultTableName(resultTableName);
        } else if (simpleAggregateQuery != null) {
            simpleAggregateQuery.setResultTableName(resultTableName);
        } else if (aggregateQuery != null) {
            aggregateQuery.setResultTableName(resultTableName);
        } else if(joinQuery != null) {
            joinQuery.setResultTableName(resultTableName);
        } else if (unionQuery != null) {
            unionQuery.setResultTableName(resultTableName);
        } else {
            throw new RuntimeException("No valid query is defined");
        }
        return this;
    }
    public ReadQuery setIsThisSubquery(boolean isThisSubquery){
        if(filterAndProjectionQuery != null){
            filterAndProjectionQuery.setIsThisSubquery(isThisSubquery);
        } else if (simpleAggregateQuery != null) {
            simpleAggregateQuery.setIsThisSubquery(isThisSubquery);
        } else if (aggregateQuery != null) {
            aggregateQuery.setIsThisSubquery(isThisSubquery);
        } else if (joinQuery != null){
            joinQuery.setIsThisSubquery(isThisSubquery);
        } else if (unionQuery != null) {
            unionQuery.setIsThisSubquery(isThisSubquery);
        }else{
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
    public boolean getIsThisSubquery(){
        if(simpleAggregateQuery != null) return simpleAggregateQuery.isThisSubquery();
        else if (aggregateQuery != null) return aggregateQuery.isThisSubquery();
        else if (filterAndProjectionQuery != null) return filterAndProjectionQuery.isThisSubquery();
        else if (joinQuery != null) return joinQuery.isThisSubquery();
        else if (unionQuery != null) return unionQuery.isThisSubquery();
        else throw new RuntimeException("No valid query is defined");
    }

    public RelReadQueryResults run(Broker broker){
        if(filterAndProjectionQuery == null && joinQuery == null && aggregateQuery == null && simpleAggregateQuery == null && unionQuery == null){
            throw new RuntimeException("No valid query is defined");
        }
        RelReadQueryResults results = new RelReadQueryResults();
        String registeredTableResults = queryMatch(broker);
        if(!registeredTableResults.isEmpty()){
            logger.info("Query is already stored, reading from result table {}", registeredTableResults);
            ReadQuery rq = new RelReadQueryBuilder(broker).select().from(registeredTableResults).build();
            if(filterAndProjectionQuery != null && filterAndProjectionQuery.isThisSubquery()){
                rq.setIsThisSubquery(true);
            } else if (simpleAggregateQuery != null && simpleAggregateQuery.isThisSubquery()) {
                rq.setIsThisSubquery(true);
            } else if (aggregateQuery != null && aggregateQuery.isThisSubquery()) {
                rq.setIsThisSubquery(true);
            } else if (joinQuery != null && joinQuery.isThisSubquery()) {
                rq.setIsThisSubquery(true);
            } else if (unionQuery != null && unionQuery.isThisSubquery()) {
                rq.setIsThisSubquery(true);
            }
            results = rq.run(broker);
        }else {
            if(stored){
                logger.info("Query needs to be registered");
                this.resultTableName = broker.registerQuery(this);
                if(this.filterAndProjectionQuery != null){
                    keyStructure = new Boolean[resultSchema.size()];
                    Arrays.fill(keyStructure, true);
                } else if (aggregateQuery != null ) {
                    keyStructure = new Boolean[resultSchema.size()];
                    Arrays.fill(keyStructure, 0, aggregateQuery.getSystemSelectedFields().size(), true);
                    Arrays.fill(keyStructure, aggregateQuery.getSystemSelectedFields().size(), resultSchema.size(), false);
                } else if (simpleAggregateQuery != null) {
                    keyStructure = new Boolean[resultSchema.size()];
                    Arrays.fill(keyStructure, true);
                } else if (joinQuery != null) {
                    keyStructure = new Boolean[resultSchema.size()];
                    Arrays.fill(keyStructure, true);
                } else if (unionQuery != null) {
                    keyStructure = new Boolean[resultSchema.size()];
                    Arrays.fill(keyStructure, false);
                    List<String> srcs = unionQuery.getTableNames();
                    for(String srcName: srcs){
                        Boolean[] srcKey = broker.getTableInfo(srcName).getKeyStructure();
                        for(int i = 0; i<srcKey.length && i<keyStructure.length; i++){
                            keyStructure[i] = keyStructure[i] || srcKey[i];
                        }
                    }
                }else{
                    throw new RuntimeException("No valid stored query is defined");
                }
            }
            if (filterAndProjectionQuery != null) {
                results = broker.retrieveAndCombineReadQuery(filterAndProjectionQuery);
            } else if (simpleAggregateQuery != null) {
                results = broker.shuffleReadQuery(simpleAggregateQuery);
            } else if (aggregateQuery != null) {
                results = broker.shuffleReadQuery(aggregateQuery);
            } else if(joinQuery != null){
                results = broker.shuffleReadQuery(joinQuery);
            } else if (unionQuery != null)  {
                results = broker.retrieveAndCombineReadQuery(unionQuery);
            }else{
                throw new RuntimeException("No valid query is defined for run");
            }
        }
        results.setFieldNames(resultSchema);
        logger.info("Query completed, result schema: " + resultSchema);
        return results;
    }
    public RelReadQueryResults updateStoredResults(Broker broker){
        if (filterAndProjectionQuery != null) {
            broker.retrieveAndCombineReadQuery(filterAndProjectionQuery);
        } else if (simpleAggregateQuery != null) {
            broker.shuffleReadQuery(simpleAggregateQuery);
        } else if (aggregateQuery != null) {
            broker.shuffleReadQuery(aggregateQuery);
        } else if(joinQuery != null){
            broker.shuffleReadQuery(joinQuery);
        } else if (unionQuery != null) {
            broker.retrieveAndCombineReadQuery(unionQuery);
        }else {
            throw new RuntimeException("No valid stored query is defined");
        }
        return null;
    }

    @Override
    public boolean equals(Object obj){
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
        if(concreteSubqueriesInput.size() != concreteSubqueriesThis.size()) {
            return false;
        }
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
        if(volatileSubqueriesInput.size() != volatileSubqueriesThis.size()) {
            return false;
        }
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
            Map<String, ReadQuery> volatileSubqueries = filterAndProjectionQuery.getSourceSubqueries();
            Map<String, ReadQuery> concreteSubqueries = filterAndProjectionQuery.getConcreteSubqueries();

            for(ReadQuery subquery: volatileSubqueries.values()){
                sourceTables.addAll(subquery.getSourceTables());
            }
            for(ReadQuery subquery: concreteSubqueries.values()){
                sourceTables.addAll(subquery.getSourceTables());
            }
            return sourceTables;
        }else if(simpleAggregateQuery != null) {

            Set<String> sourceTables = new HashSet<>(simpleAggregateQuery.getQueriedTables());
            Map<String, ReadQuery> volatileSubqueries = simpleAggregateQuery.getVolatileSubqueries();
            Map<String, ReadQuery> concreteSubqueries = simpleAggregateQuery.getConcreteSubqueries();

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
        } else if (unionQuery != null) {
            Set<String> sourceTables = new HashSet<>(unionQuery.getTableNames());
            Map<String, ReadQuery> volatileSubqueries = unionQuery.getSourceSubqueries();
            Map<String, ReadQuery> concreteSubqueries = unionQuery.getConcreteSubqueries();
            for(ReadQuery subquery: volatileSubqueries.values()){
                sourceTables.addAll(subquery.getSourceTables());
            }
            for(ReadQuery subquery: concreteSubqueries.values()){
                sourceTables.addAll(subquery.getSourceTables());
            }
            return sourceTables;
        } else if(aggregateQuery != null){

            Set<String> sourceTables = new HashSet<>(aggregateQuery.getQueriedTables());
            Map<String, ReadQuery> volatileSubqueries = aggregateQuery.getVolatileSubqueries();
            Map<String, ReadQuery> concreteSubqueries = aggregateQuery.getConcreteSubqueries();

            for(ReadQuery subquery: volatileSubqueries.values()){
                sourceTables.addAll(subquery.getSourceTables());
            }
            for(ReadQuery subquery: concreteSubqueries.values()){
                sourceTables.addAll(subquery.getSourceTables());
            }
            return sourceTables;
        }else{
            throw new RuntimeException("no valid query is defined");
        }
    }
    public Map<String, ReadQuery> getVolatileSubqueries(){
        Map<String, ReadQuery> subqueries = new HashMap<>();
        if(filterAndProjectionQuery != null){
            subqueries.putAll(filterAndProjectionQuery.getSourceSubqueries());
        }else if(simpleAggregateQuery != null){
            subqueries.putAll(simpleAggregateQuery.getVolatileSubqueries());
        }else if(aggregateQuery != null){
            subqueries.putAll(aggregateQuery.getVolatileSubqueries());
        }else if(joinQuery != null){
            subqueries.putAll(joinQuery.getVolatileSubqueries());
        } else if (unionQuery != null) {
            subqueries.putAll(unionQuery.getSourceSubqueries());
        }else {
            throw new RuntimeException("no valid query is defined");
        }
        return subqueries;
    }
    public Map<String, ReadQuery> getConcreteSubqueries(){
        Map<String, ReadQuery> subqueries = new HashMap<>();
        if(filterAndProjectionQuery != null){
            subqueries.putAll(filterAndProjectionQuery.getConcreteSubqueries());
        }else if(simpleAggregateQuery != null){
            subqueries.putAll(simpleAggregateQuery.getConcreteSubqueries());
        }else if(aggregateQuery != null){
            subqueries.putAll(aggregateQuery.getConcreteSubqueries());
        }else if(joinQuery != null){
            subqueries.putAll(joinQuery.getConcreteSubqueries());
        } else if (unionQuery != null) {
            subqueries.putAll(unionQuery.getConcreteSubqueries());
        }else {
            throw new RuntimeException("no valid query is defined");
        }
        return subqueries;
    }
    public List<Pair<Integer, String>> getAggregates(){
        if(unionQuery != null || filterAndProjectionQuery != null || joinQuery != null){
            return new ArrayList<>();
        }else if(simpleAggregateQuery != null){
            return simpleAggregateQuery.getAggregatesSpecification();
        }else{
            return aggregateQuery.getAggregatesSpecification();
        }
    }
    public List<String> getSystemFinalSchema(){
        if(filterAndProjectionQuery != null){
            return filterAndProjectionQuery.getSystemResultSchema();
        } else if (simpleAggregateQuery !=null) {
            return new ArrayList<>();
        }else if(aggregateQuery != null){
            return aggregateQuery.getSystemSelectedFields();
        } else if (joinQuery != null) {
            return joinQuery.getSystemResultSchema();
        } else if (unionQuery != null) {
            return unionQuery.getSystemResultSchema();
        } else{
            return new ArrayList<>();
        }
    }
    public List<String> getPredicate(){
        if(filterAndProjectionQuery != null){
            return filterAndProjectionQuery.getPredicates();
        } else if (simpleAggregateQuery !=null) {
            return simpleAggregateQuery.getPredicates();
        }else if(aggregateQuery != null){
            return aggregateQuery.getPredicates();
        }else if(joinQuery!=null){
            return joinQuery.getPredicates();
        }else if(unionQuery != null){
            return unionQuery.getPredicates();
        }else {
            throw new RuntimeException("no valid query is defined");
        }
    }
}
