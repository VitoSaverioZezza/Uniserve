package edu.stanford.futuredata.uniserve.coordinator;

import com.google.protobuf.ByteString;
import edu.stanford.futuredata.uniserve.*;
import edu.stanford.futuredata.uniserve.broker.Broker;
import edu.stanford.futuredata.uniserve.api.PersistentReadQuery;
import edu.stanford.futuredata.uniserve.relationalapi.ReadQuery;
import edu.stanford.futuredata.uniserve.utilities.TableInfo;
import edu.stanford.futuredata.uniserve.utilities.Utilities;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

class ServiceBrokerCoordinator extends BrokerCoordinatorGrpc.BrokerCoordinatorImplBase {
    private static final Logger logger = LoggerFactory.getLogger(ServiceBrokerCoordinator.class);
    private final Coordinator coordinator;

    ServiceBrokerCoordinator(Coordinator coordinator) {
        this.coordinator = coordinator;
    }

    @Override
    public void queryStatistics(QueryStatisticsMessage request, StreamObserver<QueryStatisticsResponse> responseObserver) {
        responseObserver.onNext(queryStatisticsHandler(request));
        responseObserver.onCompleted();
    }
    private QueryStatisticsResponse queryStatisticsHandler(QueryStatisticsMessage m) {
        ConcurrentHashMap<Set<Integer>, Integer> queryStatistics = (ConcurrentHashMap<Set<Integer>, Integer>) Utilities.byteStringToObject(m.getQueryStatistics());
        coordinator.statisticsLock.lock();
        queryStatistics.forEach((s, v) -> coordinator.queryStatistics.merge(s, v, Integer::sum));
        coordinator.statisticsLock.unlock();
        return QueryStatisticsResponse.newBuilder().build();
    }

    @Override
    public void tableInfo(TableInfoMessage request, StreamObserver<TableInfoResponse> responseObserver) {
        responseObserver.onNext(tableIDHandler(request));
        responseObserver.onCompleted();
    }
    private TableInfoResponse tableIDHandler(TableInfoMessage m) {
        String tableName = m.getTableName();
        if (coordinator.tableInfoMap.containsKey(tableName)) {
            TableInfo t = coordinator.tableInfoMap.get(tableName);
            ByteString serAttrNamesArray = Utilities.objectToByteString(t.getAttributeNames().toArray());
            ByteString serKeyStructure = Utilities.objectToByteString(t.getKeyStructure());
            ByteString serShardIDs = Utilities.objectToByteString((ArrayList<Integer>) coordinator.getShardIDsForTable(tableName));
            ByteString storedQueries = Utilities.objectToByteString(t.getRegisteredQueries());
            return TableInfoResponse.newBuilder().setReturnCode(Broker.QUERY_SUCCESS)
                    .setId(t.id)
                    .setNumShards(t.numShards)
                    .setAttributeNames(serAttrNamesArray)
                    .setKeyStructure(serKeyStructure)
                    .setShardIDs(serShardIDs)
                    .setTriggeredQueries(storedQueries)
                    .build();
        } else {
            return TableInfoResponse.newBuilder().setReturnCode(Broker.QUERY_FAILURE).build();
        }
    }

    @Override
    public void createTable(CreateTableMessage request, StreamObserver<CreateTableResponse> responseObserver) {
        responseObserver.onNext(createTableHandler(request));
        responseObserver.onCompleted();
    }
    private CreateTableResponse createTableHandler(CreateTableMessage m) {
        String[] attrNamesArray = (String[]) Utilities.byteStringToObject(m.getAttributeNames());
        List<String> attributeNames = new ArrayList<>();
        attributeNames.addAll(Arrays.asList(attrNamesArray));
        Boolean[] keyStructure = (Boolean[]) Utilities.byteStringToObject(m.getKeyStructure());
        String tableName = m.getTableName();
        int numShards = m.getNumShards();
        int tableID = coordinator.tableNumber.getAndIncrement();
        TableInfo t = new TableInfo(tableName, tableID, numShards);
        t.setAttributeNames(attributeNames);
        t.setKeyStructure(keyStructure);
        t.setTableShardsIDs(new ArrayList<>());
        if (coordinator.tableInfoMap.putIfAbsent(tableName, t) != null) {
            coordinator.assignShards();
            return CreateTableResponse.newBuilder().setReturnCode(Broker.QUERY_FAILURE).build();
        } else {
            logger.info("Creating Table. Name: {} ID: {} NumShards {}. Shard IDs from {} to {}", tableName, tableID, numShards, tableID*Broker.SHARDS_PER_TABLE, (tableID+1)*Broker.SHARDS_PER_TABLE-1);
            return CreateTableResponse.newBuilder().setReturnCode(Broker.QUERY_SUCCESS).build();
        }
    }

    @Override
    public void storeQuery(StoreQueryMessage request, StreamObserver<StoreQueryResponse> responseObserver){
        responseObserver.onNext(storeQueryHandler(request));
        responseObserver.onCompleted();
    }
    private StoreQueryResponse storeQueryHandler(StoreQueryMessage request){
        ReadQuery readQuery = (ReadQuery) Utilities.byteStringToObject(request.getQuery());
        List<String> sourceTables = new ArrayList<>(readQuery.getSourceTables());
        for(String source: sourceTables){
            coordinator.registerQuery(readQuery, source);
        }
        return StoreQueryResponse.newBuilder().setStatus(Broker.QUERY_SUCCESS).build();
    }

}
