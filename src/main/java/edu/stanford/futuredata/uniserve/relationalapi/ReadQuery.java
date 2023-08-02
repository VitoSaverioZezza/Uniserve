package edu.stanford.futuredata.uniserve.relationalapi;

import edu.stanford.futuredata.uniserve.broker.Broker;
import edu.stanford.futuredata.uniserve.interfaces.RetrieveAndCombineQueryPlan;
import edu.stanford.futuredata.uniserve.interfaces.ShuffleOnReadQueryPlan;
import edu.stanford.futuredata.uniserve.relational.ReadQueryResults;
import edu.stanford.futuredata.uniserve.relational.RelShard;

public class ReadQuery {
    Broker broker;



    public ReadQueryResults run(){
        return new ReadQueryResults();
    }
}
