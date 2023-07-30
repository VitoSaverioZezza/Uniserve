package edu.stanford.futuredata.uniserve.api.lambdamethods;

import edu.stanford.futuredata.uniserve.interfaces.Shard;

import java.io.Serializable;

/**Given a Shard and an object representing a Join Key, define how the matching object(s) should be extracted from the
 * shard.
 */
@FunctionalInterface
public interface ExtractFromShardKey<S extends Shard, O extends Object> extends Serializable {
    /**Given a Shard and an object representing a Join Key, define how the matching object(s) should be extracted from the
     * shard.
     * @param shard the shard from which the matching data should be extracted
     * @param key the join key that defines which data item in the shard matches
     * @return the extracted data item
     */
    O extract(S shard, Object key);
}
