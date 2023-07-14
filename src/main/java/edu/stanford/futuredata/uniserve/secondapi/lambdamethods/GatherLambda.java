package edu.stanford.futuredata.uniserve.secondapi.lambdamethods;

import java.io.Serializable;
import java.util.List;
import java.util.Map;


/**
 * Function that executes once on each server as part of the shuffle query. It takes as parameter a Map between table
 * names and a list of those objects that are assigned to the executing server by the scatter lambda function.
 * The gather lambda function has to return an object as a result, the returned objects from all gather executions
 * on all servers will be combined using the combine function
 * */
@FunctionalInterface
public interface GatherLambda extends Serializable {
    /**
    * Function that executes once on each server as part of the shuffle query. It takes as parameter a Map between table
    * names and a list of those objects that are assigned to the executing server by the scatter lambda function.
    * The gather lambda function has to return an object as a result, the returned objects from all gather executions
     * on all servers will be combined using the combine function.
     * @param scatteredData map between table names of the tables involved in the shuffle query and the list of
     *                      objects extracted from all actors of the table by the previously executed scatter operation and
     *                      assigned to the server running the operation. The system ensures that all data associated with the
     *                      same server ID is available when the gather is executed
     * @return the partial results of the query on the data associated to the server running the gather operation. These
     * results will be forwarded to the combine operator as they are.
     * */
    Object gather(Map<String, List<Object>> scatteredData);
}
