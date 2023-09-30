package edu.stanford.futuredata.uniserve.relationalapi;

import edu.stanford.futuredata.uniserve.broker.Broker;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CreateTableQuery {
    private final Broker broker;
    private final List<String> attributeNames = new ArrayList<>();
    private final String tableName;
    private Boolean[] keyStructure;
    private int shardNumber = Broker.SHARDS_PER_TABLE;

    public CreateTableQuery(String tableName, Broker broker){
        if(Character.isDigit(tableName.charAt(0))){
            throw new RuntimeException("Invalid table name: table name starts with a number");
        }
        this.tableName = tableName;
        this.broker = broker;
    }

    public CreateTableQuery attributes(String... attributeNames){
        Pattern aggregatePattern = Pattern.compile("^(min|max|avg|count)\\(\\s.*\\s\\)$");
        if(!this.attributeNames.isEmpty())
            throw new RuntimeException("Attribute names already defined for table " + tableName);
        for(String attr :attributeNames){
            Matcher matcherAggregate = aggregatePattern.matcher(attr);
            boolean match = matcherAggregate.matches();
            if(match){
                throw new RuntimeException("Invalid attribute name " + attr + " attributes cannot have the same name as the syntax for aggregate requests");
            }
            if(this.attributeNames.contains(attr))
                throw new RuntimeException("Attribute " + attr + " is declared multiple times for the same table");
            this.attributeNames.add(attr);
        }
        return this;
    }
    public CreateTableQuery keys(String... keyAttributes){
        if(this.attributeNames.isEmpty()){
            throw new RuntimeException("Attributes are not defined");
        }
        keyStructure = new Boolean[attributeNames.size()];
        for(String keyAttribute: keyAttributes){
            int index = attributeNames.indexOf(keyAttribute);
            if(index == -1){
                StringBuilder attrNamesBuilder = new StringBuilder();
                for(String a: attributeNames){
                    attrNamesBuilder.append(a);
                }
                throw new RuntimeException("Attribute " + keyAttribute + " is not defined for table " + tableName + "\nAttributes names are: " + attrNamesBuilder.toString());
            }else{
                keyStructure[index] = true;
            }
        }
        for(int i = 0; i< keyStructure.length; i++){
            if(keyStructure[i] == null){
                keyStructure[i] = false;
            }
        }
        return this;
    }
    public CreateTableQuery build(){
        return this;
    }
    public CreateTableQuery shardNumber(int shardNumber){this.shardNumber=shardNumber; return this;}

    public boolean run(){
        return broker.createTable(tableName, shardNumber, attributeNames, keyStructure);}
}
