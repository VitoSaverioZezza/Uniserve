package edu.stanford.futuredata.uniserve.relationalmock.rowbuilders;

import edu.stanford.futuredata.uniserve.relationalmock.RMRow;

public class RMRowBuilder {
    private int partitionKey;
    private int age;
    private String name;

    public RMRowBuilder setAge(int age){
        this.age=age;
        return this;
    }
    public RMRowBuilder setName(String name){
        this.name = name;
        return this;
    }
    public RMRowBuilder setPartitionKey(int partitionKey){
        this.partitionKey = partitionKey;
        return this;
    }

    public int getAge() {
        return age;
    }

    public int getPartitionKey() {
        return partitionKey;
    }

    public String getName() {
        return name;
    }

    public RMRow build(){
        return new RMRow(this);
    }
}
