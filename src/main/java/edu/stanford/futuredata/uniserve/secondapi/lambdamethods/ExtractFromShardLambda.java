package edu.stanford.futuredata.uniserve.secondapi.lambdamethods;

import edu.stanford.futuredata.uniserve.interfaces.Shard;

import java.io.Serializable;

@FunctionalInterface
public interface ExtractFromShardLambda<S extends Shard, O extends Object> extends Serializable {
    O extract(S shard);
}
