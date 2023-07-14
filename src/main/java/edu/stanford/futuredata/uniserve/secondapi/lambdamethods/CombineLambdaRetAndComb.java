package edu.stanford.futuredata.uniserve.secondapi.lambdamethods;

import java.io.Serializable;
import java.util.List;
import java.util.Map;


/**Takes as parameter a mapping between the table names of the tables involved in the query and a list containing all
 * objects retrieved by the retrieve lambda on all actors on that table. The function returns the result of the read query
 **/
@FunctionalInterface
public interface CombineLambdaRetAndComb extends Serializable {
    Object combine(Map<String, List<Object>> retrievedData);
}
