package edu.stanford.futuredata.uniserve.queryapi.predicates;

import edu.stanford.futuredata.uniserve.interfaces.Shard;
import org.javatuples.Pair;

import java.io.Serializable;
import java.util.List;

@FunctionalInterface
public interface ExtractLambda<S extends Shard, A extends List<Pair<Object, Object>>> extends Serializable{
    A extract(S shard);
}