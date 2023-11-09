package edu.stanford.futuredata.uniserve.standaloneServer;

import edu.stanford.futuredata.uniserve.datastore.DataStore;
import edu.stanford.futuredata.uniserve.localcloud.LocalDataStoreCloud;
import edu.stanford.futuredata.uniserve.relational.RelRow;
import edu.stanford.futuredata.uniserve.relational.RelShard;
import edu.stanford.futuredata.uniserve.relational.RelShardFactory;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Scanner;

public class StandaloneServer {
    static String baseDirectory;

    public static void main(String args[]) throws InterruptedException {
        String zkHost = args[0];
        Integer zkPort = Integer.valueOf(args[1]);
        String dsHost = args[2];
        Integer dsPort = Integer.valueOf(args[3]);
        baseDirectory = args[4];
        String localCloudPath = args[5];

        System.out.println("StartingDS\n\tzkHost: "+zkHost+"\n\tzkPort: "+zkPort+"\n\tthis Host: "+dsHost+
                "\n\tthis port: "+dsPort+"\n\tbaseDirectory: "+baseDirectory+"\n\tlocalCloudPath: " + localCloudPath);

        LocalDataStoreCloud localDataStoreCloud = new LocalDataStoreCloud(localCloudPath);
        DataStore<RelRow, RelShard> dataStore = new DataStore<>(
                localDataStoreCloud,
                new RelShardFactory(),
                Path.of(baseDirectory),
                zkHost, zkPort, dsHost, dsPort, -1, false
        );

        try {
            dataStore.startServing();
        }catch (NullPointerException e){
            e.printStackTrace();
            cleanUp();
            try {
                localDataStoreCloud.clear();
            }catch (Exception e1){
                e1.printStackTrace();
                return;
            }
            return;
        }


        Runtime.getRuntime().addShutdownHook(new Thread(dataStore::shutDown));
        Thread.sleep(Long.MAX_VALUE);
    }

    public static void cleanUp() {
        System.out.println("Cleaning up...");
        try {
            FileUtils.deleteDirectory(new File(baseDirectory));
        } catch (IOException e) {
            ;
        }
    }
}
