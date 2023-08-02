package edu.stanford.futuredata.uniserve.relational;

import java.util.ArrayList;
import java.util.List;

public class ReadQueryResults {
    List<RelRow> data = new ArrayList<>();
    List<String> fieldNames = new ArrayList<>();

    public List<String> getFieldNames(){return this.fieldNames;}
    public void addData(List<RelRow> data ){this.data.addAll(data);}
    public void setFieldNames(List<String> fieldNames){this.fieldNames=fieldNames;}
    public int getIndex(String fieldName){
        int index = this.fieldNames.indexOf(fieldName);
        if(index == -1)
            throw new RuntimeException("Index not specified for attribute " + fieldName);
        return index;
    }
    public boolean isAttributeDefined(String attributeName){
        return this.fieldNames.contains(attributeName);
    }
}
