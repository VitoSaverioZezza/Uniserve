package edu.stanford.futuredata.uniserve.operators;

import edu.stanford.futuredata.uniserve.interfaces.MapQueryPlan;
import edu.stanford.futuredata.uniserve.interfaces.Row;
import edu.stanford.futuredata.uniserve.interfaces.SerializablePredicate;
import edu.stanford.futuredata.uniserve.interfaces.Shard;

import java.util.ArrayList;
import java.util.List;

public class FilterOnWriteOperator<R extends Row> implements MapQueryPlan<R> {
    String queriedTable;
    SerializablePredicate<R> filterPredicate;
    public FilterOnWriteOperator(String queriedTable, SerializablePredicate<R> filterPredicate){
        this.queriedTable = queriedTable;
        this.filterPredicate = filterPredicate;
    }

    @Override
    public String getQueriedTable() {
        return queriedTable;
    }

    @Override
    public boolean map(List<R> rows) {
        List<R> rawData = new ArrayList<>(rows);
        try{
            rawData.removeIf(row -> !filterPredicate.test(row));
        }catch (Exception e){
            return false;
        }
        rows = rawData;
        return true;
    }
}
