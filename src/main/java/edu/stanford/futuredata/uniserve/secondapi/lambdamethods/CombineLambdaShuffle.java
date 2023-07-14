package edu.stanford.futuredata.uniserve.secondapi.lambdamethods;

import java.io.Serializable;
import java.util.List;

/**Takes as parameter a list of those objects that are a result of all combine operations executed by all servers and
 * return the result of the desired query.
 * */
@FunctionalInterface
public interface CombineLambdaShuffle extends Serializable {
    /**Takes as parameter a list of those objects that are a result of all combine operations executed by all servers and
     * return the result of the desired query.
     * @param data a list containing the results of all gather operations as they have been returned by the user defined
     *             lambda function
     * @return the query results
     * */
    Object combine(List<Object> data);
}
