package edu.stanford.futuredata.uniserve.api.lambdamethods;

import edu.stanford.futuredata.uniserve.interfaces.Shard;

import java.io.Serializable;
import java.util.Collection;


/**Executes once for each actor involved in the read query. It takes as parameter the actor and the table the actor
 * belongs to and returns the partial results of the query executed on that portion of the data represented as a
 * collection. All the partial results from all actors of all tables will be given to the defined combine lambda as
 * a parameter.*/
@FunctionalInterface
public interface RetrieveLambda extends Serializable {
    /**Executes once for each actor involved in the read query. It takes as parameter the actor and the table the actor
     * belongs to and returns the partial results of the query executed on that portion of the data represented as a
     * collection. All the partial results from all actors of all tables will be given to the defined combine lambda as
     * a parameter.
     * @param s the actor from which the data is to be extracted
     * @param tableName the name of the table the actor belongs to
     * @return a collection containing all atomic data items extracted from the query on the single actor. The objects
     * in this collection will be passed as they are as parameters to the combine operator
     * */
    Collection retrieve(Shard s, String tableName);
}
