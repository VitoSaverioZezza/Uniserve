package edu.stanford.futuredata.uniserve.tablemockinterface.queryplans;

import com.google.protobuf.ByteString;
import edu.stanford.futuredata.uniserve.interfaces.ReadQueryResults;
import edu.stanford.futuredata.uniserve.interfaces.ShuffleOnReadQueryPlan;
import edu.stanford.futuredata.uniserve.tablemockinterface.TableShard;
import edu.stanford.futuredata.uniserve.utilities.ConsistentHash;
import edu.stanford.futuredata.uniserve.utilities.Utilities;
import org.javatuples.Pair;

import java.util.*;

/**Returns the most frequent value in column with identifier "v".
 *
 * The query executes on all entries of the table whose identifier is given at construction time.*/
public class TableReadMostFrequent implements ShuffleOnReadQueryPlan<TableShard, Integer> {

    private final List<String> tables;

    /**@param table the table on which the query is being executed*/
    public TableReadMostFrequent(String table) {
        this.tables = List.of(table);
    }

    /**@return the list of tableIdentifiers of the tables involved in the query*/
    @Override
    public List<String> getQueriedTables() {
        return tables;
    }

    /** @return a map containing a single element:
     * < the identifier of the table the query is being executed on (provided to the query constructor), -1 >
     *     where -1 signals the query to be executed on all table's entries
     * */
    @Override
    public Map<String, List<Integer>> keysForQuery() {
        return Map.of(tables.get(0), Collections.singletonList(-1));
    }

    /**
     * In a shuffle read query, a single scatter operation is executed for each shard involved in the query
     * This operation associates a subset of the rows stored in the given shard to each datastore,
     * ensuring that rows having the same value for attribute "v" are associated to the same datastore.
     * @param shard the shard the scatter is being executed for
     * @param numRepartitions the total number of actors
     * @return a map object having all datastores' ids as keys and, as values, a list ByteString containing
     * the scatter's results associated to the datastore. The results are a serialization of a list containing those
     * rows that have been associated to the ds. A row is associated with a datastore id "dsID" if and only if
     *  hash(row.v) % numDS == dsID
     * */
    @Override
    public Map<Integer, List<ByteString>> scatter(TableShard shard, int numRepartitions, String tableName, Map<String, ReadQueryResults> concreteSubqueriesResults) {
        Map<Integer, ArrayList<Map<String, Integer>>> partitionedTables = new HashMap<>();

        /*
        *
        * Map < Hash(row.v) % numActors,  List < rows having the same key > >
        *
        * */


        /*Iterates all queried shard's rows, retrieving the value in the column "v" and hashes it.
        * The reminder of the division between this value and the number of datastores in the query is
        * stored in the partitionKey variable and used as key for the Map built before. The associated value is
        * the row being currently iterated on
        * */

        for (Map<String, Integer> row: shard.table) {
            int partitionKey = ConsistentHash.hashFunction(row.get("v")) % numRepartitions;
            partitionedTables.computeIfAbsent(partitionKey, k -> new ArrayList<>()).add(row);
        }

        /*
        *
        * Map< Hash(row.v) % numActors, (ByteString) List < rows having the same key > >
        *
        * */

        HashMap<Integer, List<ByteString>> serializedTables = new HashMap<>();
        partitionedTables.forEach((k, v) -> serializedTables.put(k, List.of(Utilities.objectToByteString(v))));

        for (int i = 0; i < numRepartitions; i++) {
            if(!serializedTables.containsKey(i)) {
                serializedTables.put(i, List.of(ByteString.EMPTY));
            }
        }
        return serializedTables;
    }


    /**The gather method is executed once by each and every datastore performing the shuffle read query
     * We are assuming that the query executes on a single table, therefore the mappings contain a single entry.
     * This method executes on the scatter's results, in particular, all rows having a certain value for attribute "v"
     * are "forwarded" to the same datastore, enabling the actor to compute the absolute count for that value.
     *
     * @param ephemeralShards Map< tableName, ephemeral shard associated to the table >
     * @param ephemeralData Map< tableName, List< serialization of rows stored by shards being queried
     *                      that have been associated to the datastore performing the gather by the scatter
     *                      operation >>.
     * @returns The most frequent value of v among those assigned to the current datastore
     *     */
    @Override
    public ByteString gather(Map<String, List<ByteString>> ephemeralData, Map<String, TableShard> ephemeralShards) {
    //public List<ByteString> gather(Map<String, List<ByteString>> ephemeralData, Map<String, TableShard> ephemeralShards) {

        /*Under the assumption that the query executes on a single table, the gather deserializes the scatter
        * reuslts, therefore the "table" map is the subset of rows associated to the datastore executing the query
        *
        * The number of occurrencies of each value of the attribute "v" is counted */

        Map<Integer, Integer> frequencies = new HashMap<>();
        for (ByteString b: ephemeralData.get(tables.get(0))) {
            if (!b.isEmpty()) {
                List<Map<String, Integer>> table = (List<Map<String, Integer>>) Utilities.byteStringToObject(b);
                for (Map<String, Integer> row : table) {
                    Integer val = row.get("v");
                    frequencies.merge(val, 1, Integer::sum);
                }
            }
        }

        /*The most frequent value of v among those */

        Optional<Map.Entry<Integer, Integer>> maxEntry =
                frequencies.entrySet().stream().max((entry1, entry2) -> entry1.getValue() > entry2.getValue() ? 1 : -1);
        if (maxEntry.isPresent()) {
            Pair<Integer, Integer> kf = new Pair<>(maxEntry.get().getKey(), maxEntry.get().getValue());
            //return List.of(Utilities.objectToByteString(kf));
            return Utilities.objectToByteString(kf);
        } else {
            //return List.of(ByteString.EMPTY);
            return ByteString.EMPTY;
        }
    }

    /**Takes as parameter all datastores' gather results as ByteStrings, deserializes them, obtaining each datastore
     * most frequent value of attribute v among those that have been associated to it. Then retrieves and returns
     * the most frequent of those values
     *
     * @param shardQueryResults serialized results of all datastore's gather operations. In this case, the most frequent
     *                          value of v among those associated to the given ds and the number of occurencies
     * @return the most frequent value of v among those passed as parameters
     **/
    @Override
    public Integer combine(List<ByteString> shardQueryResults) {
        Integer maxKey = null;
        int maxFrequency = Integer.MIN_VALUE;
        for (ByteString b: shardQueryResults) {
            if (!b.isEmpty()) {
                Pair<Integer, Integer> kf = (Pair<Integer, Integer>) Utilities.byteStringToObject(b);
                int key = kf.getValue0();
                int frequency = kf.getValue1();
                if (frequency > maxFrequency) {
                    maxFrequency = frequency;
                    maxKey = key;
                }
            }
        }
        return maxKey;
    }
}
