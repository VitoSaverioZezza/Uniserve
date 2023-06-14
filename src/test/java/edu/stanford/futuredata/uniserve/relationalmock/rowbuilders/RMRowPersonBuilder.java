package edu.stanford.futuredata.uniserve.relationalmock.rowbuilders;

import edu.stanford.futuredata.uniserve.relationalmock.RMRowPerson;

public class RMRowPersonBuilder {
    private int partitionKey;
    private int age;
    private String name;

    public RMRowPersonBuilder setAge(int age){
        this.age=age;
        return this;
    }
    public RMRowPersonBuilder setName(String name){
        this.name = name;
        return this;
    }
    public RMRowPersonBuilder setPartitionKey(int partitionKey){
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

    public RMRowPerson build(){
        return new RMRowPerson(this);
    }
}
