package edu.stanford.futuredata.uniserve.interfaces;

import java.io.Serializable;

@FunctionalInterface
public interface RetrieveLambda<I, O> extends Serializable {
    O retrieve(I data);
}
