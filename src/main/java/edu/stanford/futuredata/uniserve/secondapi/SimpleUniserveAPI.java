package edu.stanford.futuredata.uniserve.secondapi;

import edu.stanford.futuredata.uniserve.secondapi.querybuilders.PersistentReadQueryBuilder;
import edu.stanford.futuredata.uniserve.secondapi.querybuilders.ReadQueryBuilder;
import edu.stanford.futuredata.uniserve.secondapi.querybuilders.WriteQueryBuilder;

public class SimpleUniserveAPI {

    /**Returns a query generic read query builder, which switches to a specific query builder once the correct
     * parameters have been given
     **/
    public ReadQueryBuilder readQuery(){
        return new ReadQueryBuilder();
    }

    /**Returns a query generic write query builder, which switches to a specific query builder once the correct
     * parameters have been given
     **/
    public WriteQueryBuilder writeQuery(){
        return new WriteQueryBuilder();
    }

    /**Returns a persisten query builder, which switches to a specific query builder once the correct
     * parameters have been given. The built query is NOT registered
     */
    public PersistentReadQueryBuilder persistentQuery(){ return new PersistentReadQueryBuilder();}
}
