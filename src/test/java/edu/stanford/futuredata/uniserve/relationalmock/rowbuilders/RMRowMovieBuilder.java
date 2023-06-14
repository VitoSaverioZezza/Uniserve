package edu.stanford.futuredata.uniserve.relationalmock.rowbuilders;

import edu.stanford.futuredata.uniserve.relationalmock.RMRowMovie;

import java.util.ArrayList;
import java.util.List;

public class RMRowMovieBuilder {
    private int partitionKey;
    private List<String> actorList = new ArrayList<>();
    private String director;
    private int revenue;

    private RMRowMovieBuilder addActor(String actor){
        actorList.add(actor);
        return this;
    }
    private RMRowMovieBuilder setDirector(String director){
        this.director = director;
        return this;
    }
    private RMRowMovieBuilder setRevenue(int revenue){
        this.revenue = revenue;
        return this;
    }
    private RMRowMovieBuilder setPartitionKey(int partitionKey){
        this.partitionKey = partitionKey;
        return this;
    }

    public List<String> getActorList() {
        return actorList;
    }

    public int getRevenue() {
        return revenue;
    }

    public int getPartitionKey() {
        return partitionKey;
    }

    public String getDirector() {
        return director;
    }

    public RMRowMovie build(){
        return new RMRowMovie(this);
    }
}

