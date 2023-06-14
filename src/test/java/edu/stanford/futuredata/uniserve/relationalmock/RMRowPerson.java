package edu.stanford.futuredata.uniserve.relationalmock;

import edu.stanford.futuredata.uniserve.interfaces.Row;
import edu.stanford.futuredata.uniserve.relationalmock.rowbuilders.RMRowPersonBuilder;

public class RMRowPerson implements Row {
    private final int partitionKey;
    private int age;
    private String name;

    public RMRowPerson(RMRowPersonBuilder builder){
        this.partitionKey = builder.getPartitionKey();
        this.age = builder.getAge();
        this.name = builder.getName();
    }

    @Override
    public int getPartitionKey() {
        return partitionKey;
    }

    public int getAge() {
        return age;
    }

    public String getName() {
        return name;
    }
}
