package edu.stanford.futuredata.uniserve.relationalapi.querybuilders;

import edu.stanford.futuredata.uniserve.broker.Broker;
import edu.stanford.futuredata.uniserve.relational.ReadQueryResults;
import edu.stanford.futuredata.uniserve.relationalapi.ReadQuery;
import org.javatuples.Pair;

import java.util.*;
import java.util.function.Predicate;

public class ReadQueryBuilder {
    public static final Integer AVG = 1;
    public static final Integer MIN = 2;
    public static final Integer MAX = 3;
    public static final Integer COUNT = 4;

    private final Broker broker;

    private List<String> rawSelectedFields = new ArrayList<>();
    private String[] fieldsAliases = null;

    private final List<Pair<Integer, String>> aggregates = new ArrayList<>();
    private boolean aggregateQuery = false;

    /*
    * A x B join(pred) C x D
    * (((A x RSQ1) join(pred) C) x D)
    * .from(A).product().from(B).join(predicate).from(C).product().from(D)
    * */
    private final List<AtomicSourceElement> sources = new ArrayList<>();

    private String[] sourceAliases = null;

    private String rawSelectionPredicate = null;

    private String[] rawGroupFields = null;

    private String rawHavingPredicate = null;
    private Predicate havingPredicate = null;

    private boolean distinct = false;

    private boolean isStored = false;


    public ReadQueryBuilder(Broker broker){
        this.broker = broker;
    }

    public ReadQueryBuilder select(){return this;}
    public ReadQueryBuilder select(String... selectedFields){
        this.rawSelectedFields.addAll(Arrays.asList(selectedFields));
        return this;
    }
    public ReadQueryBuilder alias(String... aliases){
        fieldsAliases = aliases;
        if(!noDuplicateInStringArray(aliases))
            throw new RuntimeException("Error in parameter alias definition: Multiple projection args have the same alias");
        if(fieldsAliases.length != rawSelectedFields.size()){
            throw new RuntimeException("Wrong number of aliases provided for selected fields");
        }
        return this;
    }
    public ReadQueryBuilder avg(String aggregatedField){
        this.aggregateQuery = true;
        this.aggregates.add(new Pair<>(AVG, aggregatedField));
        return this;
    }
    public ReadQueryBuilder min(String aggregatedField){
        this.aggregateQuery = true;
        this.aggregates.add(new Pair<>(MIN, aggregatedField));
        return this;
    }
    public ReadQueryBuilder max(String aggregatedField){
        this.aggregateQuery = true;
        this.aggregates.add(new Pair<>(MAX, aggregatedField));
        return this;
    }
    public ReadQueryBuilder count(String aggregatedField){
        this.aggregateQuery = true;
        this.aggregates.add(new Pair<>(COUNT, aggregatedField));
        return this;
    }

    public ReadQueryBuilder from(String tableName){
        if(sources.isEmpty()){
            sources.add(new AtomicSourceElement(tableName));
        }else{
            sources.add(new AtomicSourceElement(tableName));
        }
        return this;
    }
    public ReadQueryBuilder from(ReadQuery subquery){
        if(sources.isEmpty()){
            sources.add(new AtomicSourceElement(subquery));
        }else{
            sources.add(new AtomicSourceElement(subquery));
        }
        return this;
    }
    public ReadQueryBuilder from(ReadQueryBuilder subqueryToBuild){
        return from(subqueryToBuild.build());
    }
    public ReadQueryBuilder product(){
        sources.get(sources.size()-1).isJoinedWithNext = false;
        return this;
    }
    public ReadQueryBuilder join(String predicate){
        sources.get(sources.size()-1).isJoinedWithNext = true;
        sources.get(sources.size()-1).joinWithNextRawPredicate = predicate;
        return this;
    }
    public ReadQueryBuilder aliasSource(String... sourceAliases){
        if(sources.size() != sourceAliases.length){
            throw new RuntimeException("Wrong number of source aliases specified");
        }
        if(!noDuplicateInStringArray(sourceAliases)){
            throw new RuntimeException("Error in source aliases definition: Multiple sources have the same alias");
        }
        this.sourceAliases = sourceAliases;
        int index = 0;
        for(AtomicSourceElement sourceElement: sources){
            sourceElement.alias = sourceAliases[index];
            index++;
            if(sourceElement.alias == null && sourceElement.subquery == null){
                throw new RuntimeException("No alias defined for subquery. Source number " + index);
            }
        }
        return this;
    }

    public ReadQueryBuilder where(String selectionPredicate){
        this.rawSelectionPredicate = selectionPredicate;
        return this;
    }
    public ReadQueryBuilder group(String... groupFields){
        if(!noDuplicateInStringArray(groupFields)){
            throw new RuntimeException("Error: Duplicate elements in group clause");
        }
        this.rawGroupFields = groupFields;
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
        //(1) run subqueries:
        runSubqueries();
        //(2) solve select elements
        parseSelectArguments();
        //(3) parse aggregates, with groupby clause to check whether they can be precomputed
        parseAggregates();

        parsePredicates();
        //(4) define queries
        //(4.) predicates, operations on tables with push of projection and selection
        return null;
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

    private void runSubqueries(){
        for(AtomicSourceElement sourceElement: sources){
            if(sourceElement.subquery != null)
                sourceElement.subqueryResults = sourceElement.subquery.run();
            if(sourceElement.tableName != null){
                sourceElement.fieldNames = broker.getTableInfo(sourceElement.tableName).getAttributeNames();
            } else {
                sourceElement.fieldNames = sourceElement.subqueryResults.getFieldNames();
            }
        }
    }
    private void parseSelectArguments(){
        if(rawSelectedFields.isEmpty()){
            for(AtomicSourceElement sourceElement: sources){
                int index = 0;
                for(String sourceTableAttribute: sourceElement.fieldNames){
                    sourceElement.selectedFieldsIndexes.add(index);
                    index++;
                }
            }
        }else{
            List<String> notDottedFields = new ArrayList<>();
            Map<String, List<String>> nameToFieldsMap = new HashMap<>();
            for(String rawFieldName: rawSelectedFields){
                String[] splitted = rawFieldName.split("\\.");
                if(splitted.length == 2){
                    nameToFieldsMap.computeIfAbsent(splitted[0], k->new ArrayList<>()).add(splitted[1]);
                }else{
                    notDottedFields.add(rawFieldName);
                }
            }
            for(Map.Entry<String, List<String>> entry: nameToFieldsMap.entrySet()){
                for(AtomicSourceElement sourceElement: sources){
                    if(
                            //source is a table and entry matches the table name or source is either a table or a query and entry matches
                            //its alias
                            (sourceElement.tableName != null && sourceElement.tableName.equals(entry.getKey())) ||
                                    (sourceElement.alias != null && sourceElement.alias.equals(entry.getKey()))

                    ){
                        for(String selectedFieldName: entry.getValue()){
                            int index = sourceElement.fieldNames.indexOf(selectedFieldName);
                            if(index == -1){
                                String src = "";
                                if(sourceElement.tableName != null){
                                    src = src + "table " + sourceElement.tableName;
                                }else{
                                    src = src + "subquery with alias " + sourceElement.alias;
                                }
                                throw new RuntimeException("Field " + selectedFieldName + " is not defined for " + src);
                            }
                            sourceElement.selectedFieldsIndexes.add(index);
                        }
                    }

                    for(String notDottedField: notDottedFields){
                        int index = sourceElement.fieldNames.indexOf(notDottedField);
                        if(index != -1) {
                            sourceElement.selectedFieldsIndexes.add(index);
                            notDottedFields.remove(notDottedField);
                        }
                    }
                }
            }
            if(!notDottedFields.isEmpty()){
                String str = "";
                for(String missingField: notDottedFields)
                    str = str + " " + missingField;
                throw new RuntimeException("Fields " + str + " do not have match");
            }
        }
    }
    private void parseAggregates(){
        if(!aggregates.isEmpty()){
            List<String> rawGroupFields = new ArrayList<>(Arrays.asList(this.rawGroupFields));
            Map<String, List<String>> groupFieldsMap = new HashMap<>();
            Map<String, List<Pair<Integer, String>>> nameToAggMap = new HashMap<>();
            List<Pair<Integer, String>> rawAggList = new ArrayList<>();
            for(String rawGroupField: rawGroupFields){
                String[] splitted = rawGroupField.split("\\.");
                if(splitted.length == 2){
                    rawGroupFields.remove(rawGroupField);
                    groupFieldsMap.computeIfAbsent(splitted[0], k->new ArrayList<>()).add(splitted[1]);
                }
            }
            for(Pair<Integer, String> agg: aggregates){
                String[] splitted = agg.getValue1().split("\\.");
                if(splitted.length == 2){
                    nameToAggMap.computeIfAbsent(splitted[0], k->new ArrayList<>()).add(new Pair<>(agg.getValue0(), splitted[1]));
                }else{
                    rawAggList.add(agg);
                }
            }
            for(AtomicSourceElement sourceElement: sources){
                if(nameToAggMap.isEmpty() && rawAggList.isEmpty() && groupFieldsMap.isEmpty() && rawGroupFields.isEmpty())
                    return;

                List<Pair<Integer, String>> aggregatesList = new ArrayList<>();
                List<Pair<Integer, String>> partial = null;

                List<String> groupAttributes = new ArrayList<>();
                List<String> partialGroup = null;


                if(sourceElement.tableName != null){
                    partial = nameToAggMap.get(sourceElement.tableName);
                    if(partial != null){
                        aggregatesList.addAll(partial);
                        nameToAggMap.remove(sourceElement.tableName);
                    }
                    partialGroup = groupFieldsMap.get(sourceElement.tableName);
                    if(partialGroup != null){
                        groupAttributes.addAll(partialGroup);
                        groupFieldsMap.remove(sourceElement.tableName);
                    }
                }

                if(sourceElement.alias != null) {
                    partial = nameToAggMap.get(sourceElement.alias);
                    if (partial != null) {
                        aggregatesList.addAll(partial);
                        nameToAggMap.remove(sourceElement.alias);
                    }
                    partialGroup = groupFieldsMap.get(sourceElement.alias);
                    if(partialGroup != null){
                        groupAttributes.addAll(partialGroup);
                        groupFieldsMap.remove(sourceElement.alias);
                    }
                }


                if(!aggregatesList.isEmpty()) {
                    for (Pair<Integer, String> aggr: aggregatesList) {
                        int index = sourceElement.fieldNames.indexOf(aggr.getValue1());
                        if(index == -1){
                            throw new RuntimeException("Wrong definition of aggregate field " + aggr.getValue1() + " no match for the attribute in the" +
                                    " specified table");
                        }
                        sourceElement.aggregateFieldsIndexes.add(sourceElement.fieldNames.indexOf(aggr.getValue1()));
                        sourceElement.aggregateOperation.add(aggr.getValue0());
                    }
                }
                if(!rawAggList.isEmpty()) {
                    for (Pair<Integer, String> rawAggr : rawAggList) {
                        int index = sourceElement.fieldNames.indexOf(rawAggr.getValue1());
                        if(index != -1){
                            sourceElement.aggregateFieldsIndexes.add(sourceElement.fieldNames.indexOf(rawAggr.getValue1()));
                            sourceElement.aggregateOperation.add(rawAggr.getValue0());
                            rawAggList.remove(rawAggr);
                        }
                    }
                }
                if(!rawGroupFields.isEmpty()){
                    for(String rawGroupField: rawGroupFields){
                        int index = sourceElement.fieldNames.indexOf(rawGroupField);
                        if(index != -1){
                            sourceElement.groupFields.add(index);
                            rawGroupFields.remove(rawGroupField);
                        }
                    }
                }
            }
            if(!rawAggList.isEmpty()){
                String str = "";
                for(Pair<Integer, String> missingField: rawAggList){
                    str = str + " " + missingField;
                }
                throw new RuntimeException("Aggregated fields {"+str+"} have no match in sources");
            }
            if(!rawGroupFields.isEmpty()){
                String str = "";
                for(String missingField: rawGroupFields){
                    str = str + " " + missingField;
                }
                throw new RuntimeException("Missing fields {" + str +"} for group clause");
            }
        }
    }
    private void parsePredicates(){
    }

    private class AtomicSourceElement{
        public String tableName = null;
        public ReadQuery subquery = null;
        public Boolean isJoinedWithNext = null;
        public String joinWithNextRawPredicate = null;

        public List<String> fieldNames = null;

        public ReadQueryResults subqueryResults= null;

        public String alias = null;

        public List<Integer> selectedFieldsIndexes = new ArrayList<>();
        public List<Integer> aggregateFieldsIndexes = new ArrayList<>();
        public List<Integer> aggregateOperation = new ArrayList<>();
        public List<Integer> groupFields = new ArrayList<>();


        public AtomicSourceElement(ReadQuery readQuery){
            this.subquery = readQuery;
        }
        public AtomicSourceElement(String tableName){
            this.tableName = tableName;
        }
    }
}
