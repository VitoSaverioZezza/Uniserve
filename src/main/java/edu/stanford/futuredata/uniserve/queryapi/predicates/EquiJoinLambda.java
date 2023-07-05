package edu.stanford.futuredata.uniserve.queryapi.predicates;

import java.io.Serializable;

@FunctionalInterface
public interface EquiJoinLambda<A, B> extends Serializable {
    boolean test(A a, B b);
}
