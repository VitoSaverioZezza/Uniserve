package edu.stanford.futuredata.uniserve.tablemockinterface.queryplans;

import com.google.protobuf.ByteString;
import edu.stanford.futuredata.uniserve.interfaces.Row;
import edu.stanford.futuredata.uniserve.interfaces.Shard;
import edu.stanford.futuredata.uniserve.interfaces.VolatileShuffleQueryPlan;
import edu.stanford.futuredata.uniserve.tablemockinterface.TableRow;
import edu.stanford.futuredata.uniserve.tablemockinterface.TableShard;
import edu.stanford.futuredata.uniserve.utilities.ConsistentHash;
import edu.stanford.futuredata.uniserve.utilities.Utilities;
import org.javatuples.Pair;
import org.w3c.dom.UserDataHandler;

import java.util.*;

public class VolatileTableReadMostFrequent implements VolatileShuffleQueryPlan<Integer> {

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
    public Map<Integer, List<ByteString>> scatter(Shard d, int actorCount) {
        Map<Integer, ArrayList<Map<String, Integer>>> partitionedTables = new HashMap<>();
        /*
         * Map < Hash(row.v) % numActors,  List < rows having the same key > >
         * */
        List data = ((TableShard) d).getData();
        for (Object r : data) {
            TableRow row = (TableRow) r;
            int partitionKey = ConsistentHash.hashFunction(row.getRow().get("v")) % actorCount;
            partitionedTables.computeIfAbsent(partitionKey, k -> new ArrayList<>()).add(row.getRow());
        }
        /* Map< Hash(row.v) % numActors, (ByteString) List < rows having the same key > >*/
        HashMap<Integer, List<ByteString>> serializedTables = new HashMap<>();
        partitionedTables.forEach((k, v) -> serializedTables.put(k, List.of(Utilities.objectToByteString(v))));
        /*These ByteStrings, once deserialized, are Map<String, Integer>*/

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
            List<Map<String, Integer>> table = (ArrayList<Map<String, Integer>>) Utilities.byteStringToObject(b);
            if(!table.isEmpty()) {
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

    @Override
    public void setTableName(String tableName) {
        ;
    }

    @Override
    public boolean write(Shard s, List<Row> data) {
        TableShard shard = (TableShard) s;
        shard.setRows((List) data);
        shard.insertRows();
        return true;
    }
}
