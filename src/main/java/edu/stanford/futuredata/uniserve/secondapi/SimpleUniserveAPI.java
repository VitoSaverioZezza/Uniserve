package edu.stanford.futuredata.uniserve.secondapi;

import edu.stanford.futuredata.uniserve.secondapi.querybuilders.PersistentReadQueryBuilder;
import edu.stanford.futuredata.uniserve.secondapi.querybuilders.ReadQueryBuilder;
import edu.stanford.futuredata.uniserve.secondapi.querybuilders.WriteQueryBuilder;

public class SimpleUniserveAPI {

    public ReadQueryBuilder readQuery(){
        return new ReadQueryBuilder();
    }

    public WriteQueryBuilder writeQuery(){
        return new WriteQueryBuilder();
    }

    public PersistentReadQueryBuilder persistentQuery(){ return new PersistentReadQueryBuilder();}
}
