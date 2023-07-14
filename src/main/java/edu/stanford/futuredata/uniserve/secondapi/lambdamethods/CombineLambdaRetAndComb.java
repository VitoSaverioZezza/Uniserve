package edu.stanford.futuredata.uniserve.secondapi.lambdamethods;

import java.io.Serializable;
import java.util.List;
import java.util.Map;


/**Takes as parameter a mapping between the table names of the tables involved in the query and a list containing all
 * objects retrieved by the retrieve lambda on all actors on that table. The function returns the result of the read query
 **/
@FunctionalInterface
public interface CombineLambdaRetAndComb extends Serializable {
    /**Takes as parameter a mapping between the table names of the tables involved in the query and a list containing all
     * objects retrieved by the retrieve lambda on all actors on that table. The function returns the result of the read query
     * @param retrievedData mapping between the table name and the data extracted from the actors of the table by the
     *                      user defined retrieve operation lambda. The system guarantees that the retrieve has been
     *                      executed on all actors of all tables and that all data retrieved from each table is associated
     *                      with the correct entry of the given map
     * @return the query results
     * */
    Object combine(Map<String, List<Object>> retrievedData);
}

