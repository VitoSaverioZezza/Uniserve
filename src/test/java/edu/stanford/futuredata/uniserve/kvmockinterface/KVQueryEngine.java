package edu.stanford.futuredata.uniserve.kvmockinterface;

import edu.stanford.futuredata.uniserve.broker.Broker;
import edu.stanford.futuredata.uniserve.interfaces.*;
import edu.stanford.futuredata.uniserve.kvmockinterface.queryplans.*;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class KVQueryEngine implements QueryEngine {
    private Broker broker;
    private List<KVRow> data;


    public void setBroker(Broker broker) {
        this.broker = broker;
    }

    public void setData(List<KVRow> data){
        this.data = data;
    }

    public int filterAndAverage(boolean filterOnWrite, boolean averageOnWrite){
        //at the end intermediateFilter table contains intermediate results of the filter query
        List<KVRow> filteredData;
        if(!filterOnWrite){
            WriteQueryPlan<KVRow, KVShard> insertRawDataPlan = new KVWriteQueryPlanInsert("filterAndAverageRaw");
            broker.writeQuery(insertRawDataPlan, data);
            AnchoredReadQueryPlan<KVShard, List<KVRow>> filterStoredDataPlan = new KVFilterOnRead();
            filteredData = broker.anchoredReadQuery(filterStoredDataPlan);
        }else{
            VolatileShuffleQueryPlan<KVRow, List<KVRow>> filterVolatileDataPlan = new KVFilterOnWrite();
            filteredData = broker.volatileShuffleQuery(filterVolatileDataPlan, data);
        }

        if(!averageOnWrite){
            WriteQueryPlan<KVRow, KVShard> insertFilteredDataPlan = new KVWriteQueryPlanInsert("intermediateFilter");
            broker.writeQuery(insertFilteredDataPlan, filteredData);
            AnchoredReadQueryPlan<KVShard, Integer> averageOnReadPlan = new KVAverageRead();
            return broker.anchoredReadQuery(averageOnReadPlan);
        }else{
            VolatileShuffleQueryPlan<KVRow, Integer> averageOnWritePlan = new KVVolatileAverage();
            return broker.volatileShuffleQuery(averageOnWritePlan, filteredData);
        }
    }
}
