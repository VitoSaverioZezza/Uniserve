package edu.stanford.futuredata.uniserve.relationalapi;

import java.io.Serializable;

@FunctionalInterface
public interface SerializablePredicate extends Serializable {
    Object run(Object o);
}
