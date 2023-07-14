package edu.stanford.futuredata.uniserve.secondapi.lambdamethods;

import edu.stanford.futuredata.uniserve.interfaces.Row;
import edu.stanford.futuredata.uniserve.interfaces.Shard;

import java.io.Serializable;
import java.util.List;

/**Lambda function specifying ow to perform a write operation (eventually consistent) on a shard*/
@FunctionalInterface
public interface WriteShardLambda<S extends Shard> extends Serializable {
    /**Lambda function defining how the given list of Rows is to be written on the given Shard.
     * The system ensures that the rows in the list have to be written in the given actor and that the
     * function will be executed on all needed shards for all given data
     * @param shard the actor on which the data needs to be written
     * @param rows the raw data that has to be stored in the given actor
     * @return true if and only if the write operation has been successfully carried out
     * */
    boolean write(S shard, List<Row> rows);
}
