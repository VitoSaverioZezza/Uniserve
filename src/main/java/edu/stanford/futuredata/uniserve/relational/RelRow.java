package edu.stanford.futuredata.uniserve.relational;

import edu.stanford.futuredata.uniserve.interfaces.Row;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class RelRow implements Row {
    private Object[] data = null;

    //List<Object> data = new ArrayList<>();

    public RelRow(Object... fields){
        data = fields;
        //data.addAll(Arrays.asList(fields));
    }


    @Override
    public int getPartitionKey(Boolean[] keyStructure) {
        if(keyStructure == null){
            //keyStructure = new Boolean[data.size()];
            keyStructure = new Boolean[data.length];
            Arrays.fill(keyStructure, true);
        }
        int hashCodeKey = 0;
        for(int i = 0; i<data.length && i<keyStructure.length; i++){
            if(keyStructure[i] != null && keyStructure[i]){
                Object val = data[i];
                if(val == null){
                    hashCodeKey += 0;
                }else {
                    hashCodeKey += data[i].hashCode();
                }
            }
        }
        if(hashCodeKey<0){
            hashCodeKey *=-1;
        }
        return hashCodeKey;
    }
    public Integer getSize(){ return data.length;}
    public Object getField(int attributeIndex){
        return data[attributeIndex];
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
            if(!thisField.equals(inputField)){
                return (thisField instanceof Number) && (inputField instanceof Number) && ((Number) thisField).doubleValue() == ((Number) inputField).doubleValue();
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
