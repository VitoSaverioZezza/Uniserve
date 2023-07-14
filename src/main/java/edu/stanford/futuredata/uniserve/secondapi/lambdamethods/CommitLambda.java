package edu.stanford.futuredata.uniserve.secondapi.lambdamethods;

import edu.stanford.futuredata.uniserve.interfaces.Shard;

import java.io.Serializable;

/**Takes as parameter the actor on which the precommit operation has been executed and finalizes the write*/
@FunctionalInterface
public interface CommitLambda extends Serializable {
    void run(Shard shard);
}
