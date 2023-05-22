package edu.stanford.futuredata.uniserve.interfaces;

import java.io.Serializable;
import java.util.List;

public interface MapQueryPlan<R extends Row> extends Serializable {

    String getQueriedTable();
    boolean map(List<R> rows);
}
