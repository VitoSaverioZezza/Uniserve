package edu.stanford.futuredata.uniserve.relational;

import com.google.protobuf.ByteString;
import edu.stanford.futuredata.uniserve.interfaces.ReadQueryResults;
import edu.stanford.futuredata.uniserve.interfaces.Shard;
import edu.stanford.futuredata.uniserve.relationalapi.SerializablePredicate;
import edu.stanford.futuredata.uniserve.relationalapi.querybuilders.RelReadQueryBuilder;
import edu.stanford.futuredata.uniserve.utilities.Utilities;
import org.javatuples.Pair;
import org.mvel2.MVEL;

import java.io.*;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

public class RelShard implements Shard {
    private final LinkedHashSet<RelRow> data;
    private final String shardPath;

    private final LinkedHashSet<RelRow> uncommittedRows = new LinkedHashSet<>();
    private LinkedHashSet<RelRow> rowsToRemove = new LinkedHashSet<>();



    public RelShard(Path shardPath, boolean shardExists) throws IOException, ClassNotFoundException {
        if (shardExists) {
            Path mapFile = Path.of(shardPath.toString(), "map.obj");
            FileInputStream f = new FileInputStream(mapFile.toFile());
            ObjectInputStream o = new ObjectInputStream(f);
            this.data = (LinkedHashSet<RelRow>) o.readObject();
            o.close();
            f.close();
        } else {
            this.data = new LinkedHashSet<>();
        }
        this.shardPath = shardPath.toString();
    }

    @Override
    public int getMemoryUsage() {
        return Utilities.objectToByteString(this).size();
    }

    @Override
    public void destroy() {
        data.clear();
        uncommittedRows.clear();
        rowsToRemove.clear();
    }

    @Override
    public Optional<Path> shardToData() {
        Path mapFile = Path.of(shardPath, "map.obj");
        try {
            FileOutputStream f = new FileOutputStream(mapFile.toFile());
            ObjectOutputStream o = new ObjectOutputStream(f);
            o.writeObject(data);
            o.close();
            f.close();
        } catch (IOException e) {
            return Optional.empty();
        }
        return Optional.of(Path.of(shardPath));
    }
    public List<RelRow> getData(){
        return new ArrayList<>(data);
    }
    public boolean insertRows(List/*<RelRow>*/ rows){
        uncommittedRows.addAll(rows);
        return true;
    }
    public boolean committRows(){
        for(RelRow row: rowsToRemove){
            data.remove(row);
        }
        data.addAll(uncommittedRows);
        rowsToRemove.clear();
        uncommittedRows.clear();
        return true;
    }
    public boolean abortTransactions(){
        uncommittedRows.clear();
        rowsToRemove.clear();
        return true;
    }
    public void clear(){
        this.rowsToRemove = new LinkedHashSet<>(data);
    }
    public void removeRows(List<RelRow> rowsToRemove){
        this.rowsToRemove = new LinkedHashSet<>(rowsToRemove);
    }


    @Override
    public boolean writeIntermediateShard(ByteString gatherResults){
        List<RelRow> rows = (List<RelRow>) Utilities.byteStringToObject(gatherResults);
    //public boolean writeIntermediateShard(List<ByteString> gatherResults){
    //    List<RelRow> rows = gatherResults.stream().map(v->(RelRow)Utilities.byteStringToObject(v)).collect(Collectors.toList());
        return this.insertRows(rows) && this.committRows();
    }
    @Override
    public boolean writeEphemeralShard(List<ByteString> scatterResults){
        List rows = (List) scatterResults.stream().map(v->(RelRow)Utilities.byteStringToObject(v)).collect(Collectors.toList());
        return this.insertRows(rows) && this.committRows();
    }

    public List<RelRow> getData(boolean distinct,
                                boolean proj,
                                List<Integer> projIndex,
                                Serializable compFilterP,
                                Map<String, ReadQueryResults> subqRes,
                                List<Pair<String, Integer>> predVarToIndexes,
                                List<Serializable> operations)
    {
        LinkedHashSet<RelRow> output = new LinkedHashSet<>(data);
        if(compFilterP != null && !compFilterP.equals("")) {
            output = filter(null, compFilterP, subqRes, predVarToIndexes);
        }
        if(proj && projIndex != null && !projIndex.isEmpty()){
            output = project(output, projIndex, operations);
        }
        if(distinct){
            return output.stream().distinct().collect(Collectors.toList());
        }
        return new ArrayList<>(output);
    }

    public Map<Integer, List<ByteString>> getGroups(Serializable compiledPredicate,
                                                    Map<String, ReadQueryResults> subqResults,
                                                    List<Pair<String, Integer>> predVarToIndexes,
                                                    List<Integer> groupAttributeIndexes,
                                                    int numRepartitions)
    {
        List<RelRow> dataToAggregate = getData(false, false, null,
                compiledPredicate, subqResults, predVarToIndexes, null);
        Map<Integer, List<ByteString>> ret = new HashMap<>();
        for(RelRow row: dataToAggregate) {
            int key = 0;
            for (Integer index : groupAttributeIndexes) {
                Object val = row.getField(index);
                if (val == null) {
                    key += 1;
                } else {
                    if(val instanceof Number) {
                        val = ((Number) val).doubleValue();
                    }
                    key += val.hashCode();
                }
            }
            key = key % numRepartitions;
            if (key < 0) {
                key = key * -1;
            }
            ret.computeIfAbsent(key, k->new ArrayList<>()).add(Utilities.objectToByteString(row));
        }
        return ret;
    }

    public RelRow getAggregate(Serializable compiledPredicate, Map<String, ReadQueryResults> subqResults,
                               List<Pair<String, Integer>> predVarToIndexes,
                               List<Pair<Integer, Integer>> aggOpToIndexes)
    {
        List<RelRow> filteredData = this.getData(
                false,
                false,
                null,
                compiledPredicate,
                subqResults,
                predVarToIndexes,
                null
        );
        return computePartialResults(filteredData, aggOpToIndexes);
    }

    private boolean equalGroups(List<Object> g1, List<Object> g2){
        boolean equal = true;
        if(g1 == null && g2 == null)
            return true;
        if(g1 == null || g2 == null)
            return false;
        if(g1.size() != g2.size())
            return false;
        for(int i = 0; i<g1.size(); i++){
            Object v1 = g1.get(i);
            Object v2 = g2.get(i);
            if(v1 == null && v2 == null)
                continue;
            if(v1 == null || v2 == null)
                return false;
            if(v1.equals(v2))
                continue;
            if(v1 instanceof Number && v2 instanceof Number){
                if(((Number)v1).doubleValue() == ((Number)v2).doubleValue()){
                    continue;
                }else{
                    return false;
                }
            }
        }
        return true;
    }

    public List<RelRow> join(List<RelRow> rowsSourceTwo,
                             List<String> schemaSourceOne,
                             List<String> schemaSourceTwo,
                             List<String> joinAttributesOne,
                             List<String> joinAttributesTwo,
                             List<String> systemResultSchema,
                             String sourceOne,
                             List<Serializable> operations,
                             boolean distinct,
                             Integer[] resultSchemaSystemIndexes)
    {
        LinkedHashSet<RelRow> joinedRows = new LinkedHashSet<>();
        LinkedHashSet<RelRow> rowsSourceOne = new LinkedHashSet<>(data);

        if(rowsSourceTwo == null || rowsSourceTwo.isEmpty() || this.data == null || this.data.isEmpty()){
            return new ArrayList<>();
        }
        /*
        Map<List<Object>, List<RelRow>> groupsOne = new HashMap<>();
        Map<List<Object>, List<RelRow>> groupsTwo = new HashMap<>();
        for(RelRow row: rowsSourceOne){
            List<Object> rowGroup = new ArrayList<>();
            for(int i = 0; i<joinAttributesOne.size(); i++){
                rowGroup.add(row.getField(schemaSourceOne.indexOf(joinAttributesOne.get(i))));
            }
            boolean alreadyListed = false;
            for(List<Object> mapGroup: groupsOne.keySet()){
                if(equalGroups(mapGroup, rowGroup)){
                    alreadyListed = true;
                    groupsOne.get(mapGroup).add(row);
                }
            }
            if(!alreadyListed){
                groupsOne.put(rowGroup, new ArrayList<>());
                groupsOne.get(rowGroup).add(row);
            }
        }
        for(RelRow row: rowsSourceTwo){
            List<Object> rowGroup = new ArrayList<>();
            for(int i = 0; i<joinAttributesTwo.size(); i++){
                rowGroup.add(row.getField(schemaSourceTwo.indexOf(joinAttributesTwo.get(i))));
            }
            boolean alreadyListed = false;
            for(List<Object> mapGroup: groupsTwo.keySet()){
                if(equalGroups(mapGroup, rowGroup)){
                    alreadyListed = true;
                    groupsTwo.get(mapGroup).add(row);
                }
            }
            if(!alreadyListed){
                groupsTwo.put(rowGroup, new ArrayList<>());
                groupsTwo.get(rowGroup).add(row);
            }
        }
        for(Map.Entry<List<Object>, List<RelRow>> groupEntryOne: groupsOne.entrySet()){
            boolean found = false;
            List<Object> gr2 = null;
            for(Map.Entry<List<Object>, List<RelRow>> groupEntryTwo: groupsTwo.entrySet()){
                if(equalGroups(groupEntryOne.getKey(), groupEntryTwo.getKey())){
                    found = true;
                    gr2 = groupEntryTwo.getKey();
                    List<RelRow> rowsOne = groupEntryOne.getValue();
                    List<RelRow> rowsTwo = groupEntryTwo.getValue();
                    for(RelRow rowOne: rowsOne){
                        for(RelRow rowTwo: rowsTwo){
                            List<Object> rawNewRow = new ArrayList<>(systemResultSchema.size());
                            for(String systemAttribute: systemResultSchema) {
                                String[] split = systemAttribute.split("\\.");
                                String source = split[0];
                                StringBuilder stringBuilder = new StringBuilder();
                                for (int j = 1; j < split.length - 1; j++) {
                                    stringBuilder.append(split[j]);
                                    stringBuilder.append(".");
                                }
                                stringBuilder.append(split[split.length - 1]);
                                String attribute = stringBuilder.toString();

                                if (source.equals(sourceOne)) {
                                    rawNewRow.add(systemResultSchema.indexOf(systemAttribute),
                                            rowOne.getField(schemaSourceOne.indexOf(attribute))
                                    );
                                } else {
                                    rawNewRow.add(systemResultSchema.indexOf(systemAttribute),
                                            rowTwo.getField(schemaSourceTwo.indexOf(attribute))
                                    );
                                }
                            }
                            if(operations == null || operations.isEmpty()) {
                                joinedRows.add(new RelRow(rawNewRow.toArray()));
                            }else{
                                joinedRows.add(applyOperations(new RelRow(rawNewRow.toArray()), operations));
                            }
                        }
                    }
                }
            }
            if(found){
                groupsTwo.remove(gr2);
            }
        }
        */

        for(RelRow rowOne: rowsSourceOne){
            for(RelRow rowTwo: rowsSourceTwo){
                boolean matching = true;
                for(int i = 0; i < joinAttributesOne.size(); i++){
                    Object rowOneVal = rowOne.getField(schemaSourceOne.indexOf(joinAttributesOne.get(i)));
                    Object rowTwoVal = rowTwo.getField(schemaSourceTwo.indexOf(joinAttributesTwo.get(i)));
                    if(rowOneVal == null && rowTwoVal == null){

                    } else if (rowOneVal == null || rowTwoVal == null) {
                        matching = false;
                        break;
                    }else if(!rowOneVal.equals(rowTwoVal)) {
                        if(rowOneVal instanceof Number && rowTwoVal instanceof Number &&
                                (((Number) rowOneVal).doubleValue() == ((Number) rowTwoVal).doubleValue())){
                        }else {
                            matching = false;
                            break;
                        }
                    }
                }

                if(matching){
                    Object[] rawNewRow = new Object[systemResultSchema.size()];
                    /*
                    for(String systemAttribute: systemResultSchema) {
                        String[] split = systemAttribute.split("\\.");
                        String source = split[0];
                        StringBuilder stringBuilder = new StringBuilder();
                        for (int j = 1; j < split.length - 1; j++) {
                            stringBuilder.append(split[j]);
                            stringBuilder.append(".");
                        }
                        stringBuilder.append(split[split.length - 1]);
                        String attribute = stringBuilder.toString();
                        if (source.equals(sourceOne)) {
                            rawNewRow[systemResultSchema.indexOf(systemAttribute)] =
                                    rowOne.getField(schemaSourceOne.indexOf(attribute));
                        } else {
                            rawNewRow[systemResultSchema.indexOf(systemAttribute)] =
                                    rowTwo.getField(schemaSourceTwo.indexOf(attribute));
                        }
                    }
                     */
                    for(int i = 0; i< systemResultSchema.size(); i++){
                        Integer rawIndex = resultSchemaSystemIndexes[i];
                        if(rawIndex >= 0){
                            rawNewRow[i] = rowOne.getField(rawIndex);
                        }else {
                            if(rawIndex == Integer.MIN_VALUE){
                                rawIndex = 0;
                            }
                            rawIndex = rawIndex*-1;
                            rawNewRow[i] = rowTwo.getField(rawIndex);
                        }
                    }
                    if(operations == null || operations.isEmpty()) {
                        joinedRows.add(new RelRow(rawNewRow));
                    }else{
                        joinedRows.add(applyOperations(new RelRow(rawNewRow), operations));
                    }
                }
            }
        }
        LinkedHashSet<RelRow> res = joinedRows;
        if(distinct) {
            res = removeDuplicates(joinedRows);
        }
        return new ArrayList<>(res);
    }


    private RelRow computePartialResults(List<RelRow> shardData, List<Pair<Integer, Integer>> aggregatesOPsToIndexes){
        List<Object> partialResults = new ArrayList<>();
        for(Pair<Integer, Integer> aggregate: aggregatesOPsToIndexes){
            Integer aggregateCode = aggregate.getValue0();
            Integer index = aggregate.getValue1();
            if(aggregateCode.equals(RelReadQueryBuilder.AVG)){
                Double[] countSum = new Double[2];
                Arrays.fill(countSum, 0D);
                for(RelRow row: shardData){
                    Object val = row.getField(index);
                    if(val != null){
                        countSum[1] += ((Number) val).doubleValue();
                    }
                    countSum[0]++;
                }
                partialResults.add(countSum);
            } else if (aggregateCode.equals(RelReadQueryBuilder.MIN)) {
                Double min = Double.MAX_VALUE;
                for (RelRow row: shardData){
                    Object val = row.getField(index);
                    if(val != null){
                        min = Double.min(min, ((Number) val).doubleValue());
                    }
                }
                partialResults.add(min);
            } else if (aggregateCode.equals(RelReadQueryBuilder.MAX)) {
                Double max = Double.MIN_VALUE;
                for (RelRow row: shardData){
                    Object val = row.getField(index);
                    if(val != null){
                        max = Double.max(max, ((Number) val).doubleValue());
                    }
                }
                partialResults.add(max);
            } else if (aggregateCode.equals(RelReadQueryBuilder.COUNT)) {
                Double cnt = 0D;
                cnt += ((Number) shardData.size()).doubleValue();
                partialResults.add(cnt);
            } else if (aggregateCode.equals(RelReadQueryBuilder.SUM)) {
                Double sum = 0D;
                for (RelRow row: shardData){
                    Object val = row.getField(index);
                    if(val != null) {
                        sum += ((Number) val).doubleValue();
                    }
                }
                partialResults.add(sum);
            }
        }
        return new RelRow(partialResults.toArray());
    }
    private LinkedHashSet<RelRow> removeDuplicates(LinkedHashSet<RelRow> data){
        return new LinkedHashSet<>(data);
    }
    private LinkedHashSet<RelRow> filter(LinkedHashSet<RelRow> source, Serializable filterPredicate, Map<String, ReadQueryResults> subqRes, List<Pair<String, Integer>> predVarToIndexes){
        if(source == null){
            source = new LinkedHashSet<>(data);
        }
        Map<String, RelReadQueryResults> sRes = new HashMap<>();
        int index = -1;
        for(Map.Entry<String, ReadQueryResults>entry:subqRes.entrySet()){
            sRes.put(entry.getKey(), (RelReadQueryResults) entry.getValue());
            predVarToIndexes.add(new Pair<>(entry.getKey(), index));
            index--;
        }
        LinkedHashSet<RelRow> filteredData = new LinkedHashSet<>();
        for(RelRow row: source){
            if(checkFilterPredicate(row, predVarToIndexes, sRes, filterPredicate)){
                filteredData.add(row);
            }
        }
        return filteredData;
    }
    private boolean checkFilterPredicate(RelRow row, List<Pair<String, Integer>> predicateVarToIndexes, Map<String, RelReadQueryResults> subqRes, Serializable filterPredicate){
        Map<String, Object> values = new HashMap<>();
        for(Pair<String, Integer> nameToVar: predicateVarToIndexes){
            int index = nameToVar.getValue1();
            Object val = null;
            if(index >= 0) {
                val = row.getField(nameToVar.getValue1());
            }else {
                val = subqRes.get(nameToVar.getValue0()).getData().get(0).getField(0);
            }
            if(val == null){
                return false;
            }else{
                values.put(nameToVar.getValue0(), val);
            }
        }
        if(values.containsValue(null)){
            return false;
        }
        try{
            Object result = MVEL.executeExpression(filterPredicate, values);
            if(!(result instanceof Boolean))
                return false;
            else
                return (Boolean) result;

        }catch (Exception e ){
            System.out.println(e.getMessage());
            return false;
        }
    }
    private LinkedHashSet<RelRow> project(LinkedHashSet<RelRow> data, List<Integer> resultSourceIndexes, List<Serializable> operations){
        LinkedHashSet<RelRow> projectionResults = new LinkedHashSet<>();
        for(RelRow rawRow: data){
            Object[] rawNewRow = new Object[resultSourceIndexes.size()];
            int rawIndex = 0;
            for(Integer index: resultSourceIndexes){
                rawNewRow[rawIndex] = rawRow.getField(index);
                rawIndex++;
            }
            RelRow newRow = new RelRow(rawNewRow);
            if(operations == null || operations.isEmpty()) {
                projectionResults.add(newRow);
            }else{
                projectionResults.add(applyOperations(newRow, operations));
            }
        }
        return projectionResults;
    }
    private RelRow applyOperations(RelRow inputRow, List<Serializable>operations){
        List<Object> newRow = new ArrayList<>();
        for(int i = 0; i<inputRow.getSize(); i++){
            newRow.add(applyOperation(inputRow.getField(i), operations.get(i)));
        }
        return new RelRow(newRow.toArray());
    }
    private Object applyOperation(Object o, Serializable pred){
        if(pred == null){
            return o;
        }
        SerializablePredicate predicate = (SerializablePredicate) pred;
        return predicate.run(o);
    }
}
