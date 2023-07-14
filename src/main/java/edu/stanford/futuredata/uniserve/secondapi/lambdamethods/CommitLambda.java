package edu.stanford.futuredata.uniserve.secondapi.lambdamethods;

import edu.stanford.futuredata.uniserve.interfaces.Shard;

import java.io.Serializable;

/**Takes as parameter the actor on which the precommit operation has been executed and finalizes the write operation*/
@FunctionalInterface
public interface CommitLambda extends Serializable {
    /**Takes as parameter the actor on which the precommit operation has been executed and finalizes the write operation*/
    void run(Shard shard);
}
