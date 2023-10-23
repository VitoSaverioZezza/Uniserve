package edu.stanford.futuredata.uniserve.relational;

import com.google.protobuf.ByteString;
import edu.stanford.futuredata.uniserve.interfaces.ReadQueryResults;
import edu.stanford.futuredata.uniserve.interfaces.Shard;
import edu.stanford.futuredata.uniserve.relationalapi.SerializablePredicate;
import edu.stanford.futuredata.uniserve.utilities.Utilities;
import org.javatuples.Pair;
import org.mvel2.MVEL;

import java.io.*;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

public class RelShard implements Shard {
    private final List<RelRow> data;
    private final String shardPath;

    private final List<RelRow> uncommittedRows = new ArrayList<>();
    private List<RelRow> rowsToRemove = new ArrayList<>();



    public RelShard(Path shardPath, boolean shardExists) throws IOException, ClassNotFoundException {
        if (shardExists) {
            Path mapFile = Path.of(shardPath.toString(), "map.obj");
            FileInputStream f = new FileInputStream(mapFile.toFile());
            ObjectInputStream o = new ObjectInputStream(f);
            this.data = (List<RelRow>) o.readObject();
            o.close();
            f.close();
        } else {
            this.data = new ArrayList<>();
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
        return data;
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
        this.rowsToRemove = data;
    }
    public void removeRows(List<RelRow> rowsToRemove){
        this.rowsToRemove = rowsToRemove;
    }


    public List<RelRow> getData(boolean distinct,
                                boolean proj,
                                List<Integer> projIndex,
                                Serializable compFilterP,
                                Map<String, ReadQueryResults> subqRes,
                                List<Pair<String, Integer>> redVarToIndexes,
                                List<Serializable> operations){
        List<RelRow> output = new ArrayList<>(data);
        if(compFilterP != null && !compFilterP.equals("")) {
            output = filter(compFilterP, subqRes, redVarToIndexes);
        }
        if(proj && projIndex != null && !projIndex.isEmpty()){
            output = project(output, projIndex, operations);
        }
        if(distinct){
            output = removeDuplicates(new ArrayList<>(output));
        }
        return output;
    }


    private ArrayList<RelRow> removeDuplicates(ArrayList<RelRow> data){
        ArrayList<RelRow> nonDuplicateRows = new ArrayList<>();
        for(int i = 0; i < data.size(); i++){
            List<RelRow> sublist = data.subList(i+1, data.size());
            if(!sublist.contains(data.get(i))){
                nonDuplicateRows.add(data.get(i));
            }
        }
        return nonDuplicateRows;
    }
    private List<RelRow> filter(Serializable filterPredicate, Map<String, ReadQueryResults> subqRes, List<Pair<String, Integer>> predVarToIndexes){
        Map<String, RelReadQueryResults> sRes = new HashMap<>();
        int index = -1;
        for(Map.Entry<String, ReadQueryResults>entry:subqRes.entrySet()){
            sRes.put(entry.getKey(), (RelReadQueryResults) entry.getValue());
            predVarToIndexes.add(new Pair<>(entry.getKey(), index));
            index--;
        }
        List<RelRow> filteredData = new ArrayList<>();
        for(RelRow row: data){
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
    private ArrayList<RelRow> project(List<RelRow> data, List<Integer> resultSourceIndexes, List<Serializable> operations){
        ArrayList<RelRow> projectionResults = new ArrayList<>();
        for(RelRow rawRow: data){
            List<Object> rawNewRow = new ArrayList<>(resultSourceIndexes.size());
            for(Integer index: resultSourceIndexes){
                rawNewRow.add(rawRow.getField(index));
            }
            RelRow newRow = new RelRow(rawNewRow.toArray());
            if(operations.isEmpty()) {
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
        SerializablePredicate predicate = (SerializablePredicate) pred;
        return predicate.run(o);
    }

    @Override
    public boolean writeIntermediateShard(ByteString gatherResults){
        List<RelRow> rows = (List<RelRow>) Utilities.byteStringToObject(gatherResults);
        return this.insertRows(rows) && this.committRows();
    }
    @Override
    public boolean writeEphemeralShard(List<ByteString> scatterResults){
        List rows = (List) scatterResults.stream().map(v->(RelRow)Utilities.byteStringToObject(v)).collect(Collectors.toList());
        return this.insertRows(rows) && this.committRows();
    }

}
