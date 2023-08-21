package edu.stanford.futuredata.uniserve.relationalapi;

import edu.stanford.futuredata.uniserve.broker.Broker;
import edu.stanford.futuredata.uniserve.relational.RelReadQueryResults;
import edu.stanford.futuredata.uniserve.relationalapi.querybuilders.WriteQueryBuilder;
import edu.stanford.futuredata.uniserve.utilities.TableInfo;
import org.javatuples.Pair;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ReadQuery implements Serializable {
    private ProdSelProjQuery simpleQuery = null;
    private List<String> resultSchema = new ArrayList<>();
    private AggregateQuery aggregateQuery = null;
    private SimpleAggregateQuery simpleAggregateQuery = null;
    private String resultTableID = null;

    private Boolean stored = false;
    private Boolean isAlreadyStored = false;


    public RelReadQueryResults run(Broker broker){
        RelReadQueryResults results = null;
        if(simpleQuery != null){
            results =  simpleQuery.run(broker);
        }else if(simpleAggregateQuery != null){
            results =  simpleAggregateQuery.run(broker);
        }else{
            results = aggregateQuery.run(broker);
        }
        Boolean[] keyStructure = new Boolean[resultSchema.size()];
        if(aggregateQuery != null){
            Arrays.fill(keyStructure, 0, aggregateQuery.getGroupAttributesSubschema().size(), true);
            Arrays.fill(keyStructure, aggregateQuery.getGroupAttributesSubschema().size(), keyStructure.length, false);
        }else{
            Arrays.fill(keyStructure, true);
        }
        if(stored && !isAlreadyStored){
            //store the query in the TableInfo of all source tables, then set isAlreadyStored
            List<String> sourceTables = getSourceTables();
            broker.storeReadQuery(this);
            broker.createTable(resultTableID, Broker.SHARDS_PER_TABLE, resultSchema, keyStructure);
            isAlreadyStored = true;
        }
        if(stored){
            boolean wqRes = broker.simpleWriteQuery(
                    new UpdateStoredResultsWrite(keyStructure, resultTableID), results.getData()
            );
        }
        return results;
    }

    public ReadQuery setResultTableID(String resultTableID){
        this.resultTableID = resultTableID;
        return this;
    }
    public String getResultTableID(){
        return resultTableID;
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
        System.out.println("My equals!");
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


    public void setIsAlreadyStored(){
        isAlreadyStored = true;
    }
    public void setStored(){
        stored = true;
    }
    public List<String> getSourceTables(){
        if(simpleQuery != null){
            return simpleQuery.getTableNames();
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
