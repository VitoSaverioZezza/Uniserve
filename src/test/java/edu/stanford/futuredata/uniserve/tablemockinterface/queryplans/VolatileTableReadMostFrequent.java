package edu.stanford.futuredata.uniserve.tablemockinterface.queryplans;

import com.google.protobuf.ByteString;
import edu.stanford.futuredata.uniserve.interfaces.ShuffleReadQueryPlan;
import edu.stanford.futuredata.uniserve.interfaces.VolatileShuffleQueryPlan;
import edu.stanford.futuredata.uniserve.tablemockinterface.TableRow;
import edu.stanford.futuredata.uniserve.tablemockinterface.TableShard;
import edu.stanford.futuredata.uniserve.utilities.ConsistentHash;
import edu.stanford.futuredata.uniserve.utilities.Utilities;
import org.javatuples.Pair;

import java.util.*;

public class VolatileTableReadMostFrequent implements VolatileShuffleQueryPlan<TableRow, TableShard, Integer> {

    private final String table;

    /**@param table the table on which the query is being executed*/
    public VolatileTableReadMostFrequent(String table) {
        this.table = table;
    }

    /**@return the list of tableIdentifiers of the tables involved in the query*/
    @Override
    public String getQueriedTables() {
        return table;
    }

    @Override
    public Map<Integer, List<ByteString>> scatter(List<TableRow> data, int actorCount) {
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

        for (TableRow row: data) {
            int partitionKey = ConsistentHash.hashFunction(row.getRow().get("v")) % actorCount;
            partitionedTables.computeIfAbsent(partitionKey, k -> new ArrayList<>()).add(row.getRow());
        }

        /*
         *
         * Map< Hash(row.v) % numActors, (ByteString) List < rows having the same key > >
         *
         * */

        HashMap<Integer, List<ByteString>> serializedTables = new HashMap<>();
        partitionedTables.forEach((k, v) -> serializedTables.put(k, List.of(Utilities.objectToByteString(v))));

        for (int i = 0; i < actorCount; i++) {
            if(!serializedTables.containsKey(i)) {
                serializedTables.put(i, List.of(ByteString.EMPTY));
            }
        }
        return serializedTables;
    }

    @Override
    public ByteString gather(List<ByteString> scatteredData) {

        /*Under the assumption that the query executes on a single table, the gather deserializes the scatter
         * reuslts, therefore the "table" map is the subset of rows associated to the datastore executing the query
         *
         * The number of occurrencies of each value of the attribute "v" is counted */

        Map<Integer, Integer> frequencies = new HashMap<>();
        for (ByteString b: scatteredData) {
            List<Map<String, Integer>> table = (List<Map<String, Integer>>) Utilities.byteStringToObject(b);
            for (Map<String, Integer> row : table) {
                Integer val = row.get("v");
                frequencies.merge(val, 1, Integer::sum);
            }
        }

        /*The most frequent value of v among those */

        Optional<Map.Entry<Integer, Integer>> maxEntry =
                frequencies.entrySet().stream().max((entry1, entry2) -> entry1.getValue() > entry2.getValue() ? 1 : -1);
        if (maxEntry.isPresent()) {
            Pair<Integer, Integer> kf = new Pair<>(maxEntry.get().getKey(), maxEntry.get().getValue());
            return Utilities.objectToByteString(kf);
        } else {
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
