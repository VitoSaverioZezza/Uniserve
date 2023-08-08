package edu.stanford.futuredata.uniserve.interfaces;

import java.util.List;

public interface ReadQueryResults<S extends Shard, T> {
    List<T> getData();
    boolean writeShard(S shard);
    String getAlias();
}
