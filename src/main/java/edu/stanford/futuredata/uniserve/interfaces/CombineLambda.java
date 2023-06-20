package edu.stanford.futuredata.uniserve.interfaces;

import java.io.Serializable;

@FunctionalInterface
public interface CombineLambda<I, O> extends Serializable {
    O combine(I retrieveResults);
}
