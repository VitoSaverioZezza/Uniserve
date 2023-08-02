package edu.stanford.futuredata.uniserve.interfaces;

import java.io.Serializable;

/**Atomic unit of information that exposes a (non-negative) partition key.
 * The system ensures that rows with the same key are stored in the same shard
 * */
public interface Row extends Serializable {
    /**@return the partition key of the row*/
    int getPartitionKey(Boolean[] keyStructure);
}
