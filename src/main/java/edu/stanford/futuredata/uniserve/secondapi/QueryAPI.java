package edu.stanford.futuredata.uniserve.secondapi;

import edu.stanford.futuredata.uniserve.broker.Broker;
import edu.stanford.futuredata.uniserve.interfaces.Shard;
import edu.stanford.futuredata.uniserve.secondapi.lambdamethods.ExtractFromShardKey;
import edu.stanford.futuredata.uniserve.secondapi.lambdamethods.ExtractFromShardLambda;

import java.util.Collection;
import java.util.List;

public class QueryAPI<S extends Shard> {
    private final Broker broker;

    public QueryAPI(String zkHost, Integer zkPort){
        this.broker = new Broker(zkHost, zkPort, null);
    }

    public List<Object> joinOnRead(String tableOne, String tableTwo,
                                   ExtractFromShardLambda<S, Collection> extractKeysFromTableOne,
                                   ExtractFromShardLambda<S, Collection> extractKeysFromTableTwo,
                                   ExtractFromShardKey<S, Object> selectTableOneLogic,
                                   ExtractFromShardKey<S, Object> selectTableTwoLogic){

        JoinOnReadSecondIteration<S> queryPlan = new JoinOnReadSecondIteration<>(tableOne, tableTwo);
        queryPlan.setExtractKeysOne(extractKeysFromTableOne);
        queryPlan.setExtractKeysTwo(extractKeysFromTableTwo);
        queryPlan.setExtractDataOne(selectTableOneLogic);
        queryPlan.setExtractDataTwo(selectTableTwoLogic);
        return (List<Object>) broker.shuffleReadQuery(queryPlan);
    }


}
