package edu.stanford.futuredata.uniserve.secondapi.lambdamethods;

import edu.stanford.futuredata.uniserve.interfaces.Shard;

import java.io.Serializable;

/**Defines how data should be retrieved from an actor*/
@FunctionalInterface
public interface ExtractFromShardLambda<S extends Shard, O extends Object> extends Serializable {
    /**Defines how data should be retrieved from an actor
     * @param shard the actor from which the data is to be extracted
     * @return the extracted data*/
    O extract(S shard);
}
