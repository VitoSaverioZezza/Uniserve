package edu.stanford.futuredata.uniserve.relationalapi;

import edu.stanford.futuredata.uniserve.broker.Broker;
import edu.stanford.futuredata.uniserve.relational.RelReadQueryResults;
import edu.stanford.futuredata.uniserve.relationalapi.querybuilders.ReadQueryBuilder;
import edu.stanford.futuredata.uniserve.relationalapi.querybuilders.WriteQueryBuilder;
import edu.stanford.futuredata.uniserve.utilities.TableInfo;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;

public class ReadQuery implements Serializable {
    private static final Logger logger = LoggerFactory.getLogger(ReadQuery.class);

    private ProdSelProjQuery simpleQuery = null;
    private List<String> resultSchema = new ArrayList<>();
    private AggregateQuery aggregateQuery = null;
    private SimpleAggregateQuery simpleAggregateQuery = null;

    private boolean registered = false;
    private String resultTableName = "";
    private Boolean[] keyStructure;

    public RelReadQueryResults updateStoredResults(Broker broker, RelReadQueryResults results){
        assert (results != null);
        broker.simpleWriteQuery(new UpdateStoredResultsWrite(keyStructure, resultTableName), results.getData());
        return results;
    }
    public RelReadQueryResults updateStoredResults(Broker broker){
        logger.info("Updating stored query results");
        RelReadQueryResults results;
        if(simpleQuery != null){
            results =  simpleQuery.run(broker);
        }else if(simpleAggregateQuery != null){
            results =  simpleAggregateQuery.run(broker);
        }else{
            results = aggregateQuery.run(broker);
        }
        assert (results != null);
        broker.simpleWriteQuery(new UpdateStoredResultsWrite(keyStructure, resultTableName), results.getData());

        return results;
    }

    public RelReadQueryResults run(Broker broker){
        RelReadQueryResults results = null;
        //check if there's a matching registered query
        String registeredTableResults = queryMatch(broker);
        if(!registeredTableResults.isEmpty()){
            logger.info("Query matches a previously stored query");
            RelReadQueryResults readQueryResults = new ReadQueryBuilder(broker).select().from(registeredTableResults).build().run(broker);
            return readQueryResults;
        }else{
            if(simpleQuery != null){
                results =  simpleQuery.run(broker);
            }else if(simpleAggregateQuery != null){
                results =  simpleAggregateQuery.run(broker);
            }else{
                results = aggregateQuery.run(broker);
            }
        }
        //check if the query has to be registered
        //register if and only if the query has no matching registered query
        if(registered && registeredTableResults.isEmpty()){
            keyStructure = new Boolean[resultSchema.size()];
            if(aggregateQuery != null){
                Arrays.fill(keyStructure, 0, aggregateQuery.getGroupAttributesSubschema().size(), true);
                Arrays.fill(keyStructure, aggregateQuery.getGroupAttributesSubschema().size(), resultSchema.size(), false);
            }else {
                Arrays.fill(keyStructure, true);
            }
            broker.registerQuery(this);
            updateStoredResults(broker, results);
        }
        return results;
    }

    public Boolean[] getKeyStructure() {
        return keyStructure;
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


    public String getResultTableName() {
        return resultTableName;
    }

    public void setResultTableName(String resultTableName) {
        this.resultTableName = resultTableName;
    }

    public ReadQuery setRegistered(){
        this.registered = true;
        return this;
    }
    public ReadQuery setSimpleAggregateQuery(SimpleAggregateQuery simpleAggregateQuery){
        this.simpleAggregateQuery = simpleAggregateQuery;
        return this;
    }
    public ReadQuery setAggregateQuery(AggregateQuery aggregateQuery){
        this.aggregateQuery = aggregateQuery;
        return this;
    }
    public ReadQuery setSimpleQuery(ProdSelProjQuery simpleQuery) {
        this.simpleQuery = simpleQuery;
        return this;
    }

    public ReadQuery setResultSchema(List<String> resultSchema) {
        this.resultSchema = resultSchema;
        return this;
    }
    public List<String> getResultSchema() {
        return resultSchema;
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
        List<ReadQuery> subqueriesInput = readQuery.getSubqueries();
        List<ReadQuery> subqueriesThis = this.getSubqueries();
        if(subqueriesInput.size() != subqueriesThis.size())
            return false;
        for(int i = 0; i<subqueriesInput.size(); i++){
            ReadQuery subqInput = subqueriesInput.get(i);
            boolean match = false;
            for(int j = 0; j<subqueriesThis.size() && !match; j++){
                ReadQuery subqThis = subqueriesThis.get(j);
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
        if(!this.getPredicate().equals(readQuery.getPredicate())){
            return false;
        }
        return true;
    }


    public Set<String> getSourceTables(){
        if(simpleQuery != null){
            Set<String> sourceTables = new HashSet<>(simpleQuery.getTableNames());
            Map<String, ReadQuery> subqueries = simpleQuery.getSubqueriesResults();
            for(ReadQuery subquery: subqueries.values()){
                sourceTables.addAll(subquery.getSourceTables());
            }
            return sourceTables;
        }else if(simpleAggregateQuery != null){
            ReadQuery subquery = simpleAggregateQuery.getSubqueriesResults().get(new ArrayList<>(simpleAggregateQuery.getSubqueriesResults().keySet()).get(0));
            return subquery.getSourceTables();
        }else{
            return aggregateQuery.getIntermediateQuery().getSourceTables();
        }
    }
    public List<ReadQuery> getSubqueries(){
        if(simpleQuery != null){
            return new ArrayList<>(simpleQuery.getSubqueriesResults().values());
        }else if(simpleAggregateQuery != null){
            ReadQuery subquery = simpleAggregateQuery.getSubqueriesResults().get(new ArrayList<>(simpleAggregateQuery.getSubqueriesResults().keySet()).get(0));
            return new ArrayList<>(subquery.getSubqueries());
        }else{
            return aggregateQuery.getIntermediateQuery().getSubqueries();
        }
    }
    public List<Pair<Integer, String>> getAggregates(){
        if(simpleQuery != null){
            return new ArrayList<>();
        }else if(simpleAggregateQuery != null){
            return simpleAggregateQuery.getAggregatesSubschema();
        }else{
            return aggregateQuery.getAggregatesSubschema();
        }
    }
    public List<String> getSystemFinalSchema(){
        if(simpleQuery != null){
            return simpleQuery.getSystemFinalSchema();
        } else if (simpleAggregateQuery!=null) {
            return new ArrayList<>();
        }else{
            return aggregateQuery.getGroupAttributesSubschema();
        }
    }
    public String getPredicate(){
        if(simpleQuery != null){
            return simpleQuery.getSelectionPredicate();
        } else if (simpleAggregateQuery!=null) {
            return "";
        }else{
            return aggregateQuery.getHavingPredicate();
        }
    }
}
