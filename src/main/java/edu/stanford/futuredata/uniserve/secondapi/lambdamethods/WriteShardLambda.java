package edu.stanford.futuredata.uniserve.secondapi.lambdamethods;

import edu.stanford.futuredata.uniserve.interfaces.Row;
import edu.stanford.futuredata.uniserve.interfaces.Shard;

import java.io.Serializable;
import java.util.List;

@FunctionalInterface
public interface WriteShardLambda<S extends Shard> extends Serializable {
    boolean write(S shard, List<Row> rows);
}
