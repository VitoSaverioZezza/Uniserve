package edu.stanford.futuredata.uniserve.secondapi.lambdamethods;

import java.io.Serializable;
import java.util.List;

/**
 * Takes as parameter a list of those objects that are a result of all combine operations executed by all servers and
 * return the result of the desired query.
 * */
@FunctionalInterface
public interface CombineLambdaShuffle extends Serializable {
    Object combine(List<Object> data);
}
