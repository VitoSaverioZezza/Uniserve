package edu.stanford.futuredata.uniserve.relational;

import edu.stanford.futuredata.uniserve.interfaces.Row;

import java.util.ArrayList;
import java.util.List;

public class RelRow implements Row {
    List<Object> data = new ArrayList<>();
    @Override
    public int getPartitionKey(Boolean[] keyStructure) {
        int hashCodeKey = 0;
        for(int i = 0; i<keyStructure.length; i++){
            if(keyStructure[i]){
                hashCodeKey += data.get(i).hashCode();
            }
        }
        return hashCodeKey;
    }
    public Integer getSize(){ return data.size();}
    public Object getField(int attributeIndex){
        return data.get(attributeIndex);
    }
}
