package edu.stanford.futuredata.uniserve.relationalmock;

import edu.stanford.futuredata.uniserve.interfaces.Row;
import edu.stanford.futuredata.uniserve.relationalmock.rowbuilders.RMRowMovieBuilder;

import java.util.List;

public class RMRowMovie implements Row {
    private final int partitionKey;
    private final List<String> actorList;
    private final String director;
    private final int revenue;

    public RMRowMovie(RMRowMovieBuilder builder){
        this.partitionKey = builder.getPartitionKey();
        this.actorList = builder.getActorList();
        this.director = builder.getDirector();
        this.revenue = builder.getRevenue();
    }

    @Override
    public int getPartitionKey() {
        return partitionKey;
    }

    public String getDirector() {
        return director;
    }

    public List<String> getActorList() {
        return actorList;
    }

    public int getRevenue() {
        return revenue;
    }
}
