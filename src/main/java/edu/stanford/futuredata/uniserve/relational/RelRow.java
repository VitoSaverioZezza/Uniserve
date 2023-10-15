package edu.stanford.futuredata.uniserve.relational;

import edu.stanford.futuredata.uniserve.interfaces.Row;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class RelRow implements Row {
    private final List<Object> data = new ArrayList<>();

    public RelRow(Object... fields){
        data.addAll(Arrays.asList(fields));
    }


    @Override
    public int getPartitionKey(Boolean[] keyStructure) {
        if(keyStructure == null){
            keyStructure = new Boolean[data.size()];
            Arrays.fill(keyStructure, true);
        }
        int hashCodeKey = 0;
        for(int i = 0; i<data.size() && i<keyStructure.length; i++){
            if(keyStructure[i] != null && keyStructure[i]){
                Object val = data.get(i);
                if(val == null){
                    hashCodeKey += 0;
                }else {
                    hashCodeKey += data.get(i).hashCode();
                }
            }
        }
        if(hashCodeKey<0){
            hashCodeKey *=-1;
        }
        return hashCodeKey;
    }
    public Integer getSize(){ return data.size();}
    public Object getField(int attributeIndex){
        return data.get(attributeIndex);
    }

    @Override
    public boolean equals(Object input){
        if(!(input instanceof RelRow)){
            return false;
        }
        RelRow inputRow = (RelRow) input;
        if(inputRow.getSize() != this.getSize()){
            return false;
        }
        for(int i = 0; i<this.getSize(); i++){
            Object thisField = this.getField(i);
            Object inputField = inputRow.getField(i);
            if(thisField == null && inputField == null){
                return true;
            }
            if(thisField == null || inputField == null){
                return false;
            }
            if(!this.getField(i).equals(inputRow.getField(i))){
                return false;
            }
        }
        return true;
    }
    public void print(){
        for(Object field: data){
            System.out.print(field + " | ");
        }
        System.out.println();
    }
}
