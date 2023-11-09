package edu.stanford.futuredata.uniserve.standaloneClient;

import edu.stanford.futuredata.uniserve.broker.Broker;
import edu.stanford.futuredata.uniserve.relational.RelRow;
import edu.stanford.futuredata.uniserve.relationalapi.API;
import edu.stanford.futuredata.uniserve.relationalapi.querybuilders.WriteQueryBuilder;
import org.javatuples.Pair;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

public class TestMethods {
    private String rootPath = ""; //clean!

    public TestMethods(String rootPath){
        this.rootPath = rootPath;
    }

    public List<Pair<String,Long>> loadDataInMem(Broker broker, List<String> tablesToLoad) {
        API api = new API(broker);
        boolean res = true;
        List<Pair<String, Long>> times = new ArrayList<>();

        List<WriteQueryBuilder> batch = new ArrayList<>();

        for (int i = 0; i < TPC_DS_Inv.numberOfTables; i++) {
            if(!tablesToLoad.contains(TPC_DS_Inv.names.get(i))){
                continue;
            }else{
                System.out.println("Loading data for table " + TPC_DS_Inv.names.get(i));
            }
            List<RelRow> memBuffer = new ArrayList<>();
            MemoryLoader memoryLoader = new MemoryLoader(i, memBuffer);
            if(!memoryLoader.run()){
                return null;
            }
            //int shardNum = Math.min(Math.max(memBuffer.size()/1000, 1), Broker.SHARDS_PER_TABLE);

            int shardNum = Math.min(Math.max(TPC_DS_Inv.sizes.get(i)/1000, 1), Broker.SHARDS_PER_TABLE);
            res = api.createTable(TPC_DS_Inv.names.get(i))
                    .attributes(TPC_DS_Inv.schemas.get(i).toArray(new String[0]))
                    .keys(TPC_DS_Inv.schemas.get(i).toArray(new String[0]))
                    .shardNumber(shardNum)
                    .build().run();

            //long startTime = System.currentTimeMillis();
            //res = api.write().table(TPC_DS_Inv.names.get(i)).data(memBuffer.toArray(new RelRow[0])).build().run();
            //long elapsedTime = System.currentTimeMillis() - startTime;
            //times.add(new Pair<>(TPC_DS_Inv.names.get(i), elapsedTime));
            //memBuffer.clear();

            batch.add(api.write().table(TPC_DS_Inv.names.get(i)).data(memBuffer.toArray(new RelRow[0])).build());

            //if(!res){
            //    broker.shutdown();
            //    throw new RuntimeException("Write error for table "+TPC_DS_Inv.names.get(i));
            //}
        }
        Pair<Boolean, List<Pair<String,Long>>> results = api.writeBatch().setPlans(batch.toArray(new WriteQueryBuilder[0])).run();
        return results.getValue1();
    }

    private class MemoryLoader{
        private final int index;
        private final List<RelRow> sink;
        public MemoryLoader(int index, List<RelRow> sink){
            this.sink = sink;
            this.index = index;
        }
        public boolean run(){
            String path = rootPath + TPC_DS_Inv.paths.get(index);
            List<Integer> types = TPC_DS_Inv.types.get(index);

            try (BufferedReader br = new BufferedReader(new FileReader(path))) {
                String line;
                while ((line = br.readLine()) != null) {
                    String[] parts = line.split("\\|");
                    List<Object> parsedRow = new ArrayList<>(types.size());
                    for(int i = 0; i<types.size(); i++){
                        fieldIndex = i;
                        if(i < parts.length){
                            parsedRow.add(parseField(parts[i], types.get(i)));
                        }else{
                            parsedRow.add(parseField("", types.get(i)));
                        }
                    }
                    if(parsedRow.size() == TPC_DS_Inv.schemas.get(index).size()){
                        sink.add(new RelRow(parsedRow.toArray()));
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
                return false;
            }
            return true;
        }


        int fieldIndex =0;

        private Object parseField(String rawField, Integer type) throws IOException {
            if(rawField.isEmpty()){
                if(type.equals(TPC_DS_Inv.id__t)){
                    return -1;
                } else if (type.equals(TPC_DS_Inv.string__t)) {
                    return "";
                } else if (type.equals(TPC_DS_Inv.int__t)) {
                    return 0;
                } else if (type.equals(TPC_DS_Inv.dec__t)) {
                    return 0D;
                } else if (type.equals(TPC_DS_Inv.date__t)) {
                    try{
                        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
                        return simpleDateFormat.parse("0000-00-00");
                    }catch (ParseException e){
                        throw new IOException("null date is not a valid date so it seems");
                    }
                }else {
                    throw new IOException("Unsupported type definition");
                }
            }
            if(type.equals(TPC_DS_Inv.id__t)){
                return Integer.valueOf(rawField);
            } else if (type.equals(TPC_DS_Inv.string__t)) {
                return rawField;
            } else if (type.equals(TPC_DS_Inv.int__t)) {
                try {
                    return Integer.valueOf(rawField);
                }catch (NumberFormatException e){
                    throw new IOException("Raw field cannot be parsed as an Integer.\n" + e.getMessage());
                }
            } else if (type.equals(TPC_DS_Inv.dec__t)) {
                try {
                    return Double.valueOf(rawField);
                }catch (NumberFormatException e){
                    throw new IOException("Raw field cannot be parsed as a Double.\n" + e.getMessage());
                }
            } else if (type.equals(TPC_DS_Inv.date__t)) {
                try{
                    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
                    return simpleDateFormat.parse(rawField);
                }catch (ParseException e){
                    throw new IOException("Raw field cannot be parsed as Date.\n" + e.getMessage());
                }
            }else {
                throw new IOException("Unsupported type definition");
            }
        }
    }

}
