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


}
