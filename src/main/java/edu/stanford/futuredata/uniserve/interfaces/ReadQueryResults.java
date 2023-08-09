package edu.stanford.futuredata.uniserve.interfaces;

import java.io.Serializable;
import java.util.List;

public interface ReadQueryResults<S extends Shard, T> extends Serializable {
    List<T> getData();
    boolean writeShard(S shard);
    String getAlias();
}
