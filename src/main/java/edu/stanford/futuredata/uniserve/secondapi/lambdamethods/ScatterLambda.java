package edu.stanford.futuredata.uniserve.secondapi.lambdamethods;

import edu.stanford.futuredata.uniserve.interfaces.Shard;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * Define a function that gets executed for every actor involved in the read query. The function takes as
 * parameter the shard containing the data and the number of servers in the cluster. The function needs to extract
 * data items and assign them to servers, returning the assigned items in a list associated with a key that represents
 * the server's identifier (from 0 to numServers -1).
 * The returned objects will be given as input to the provided gather lambda, guaranteeing that all the objects assigned
 * to the same server will be passed as parameters to the same gather call.
 * */
@FunctionalInterface
public interface ScatterLambda extends Serializable {
    Map<Integer, List<Object>> scatter(Shard shard, int numServers);
}
