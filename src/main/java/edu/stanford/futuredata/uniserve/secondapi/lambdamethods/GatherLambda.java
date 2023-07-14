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
    Object gather(Map<String, List<Object>> scatteredData);
}
