package edu.stanford.futuredata.uniserve.localcloud;

import edu.stanford.futuredata.uniserve.datastore.DataStoreCloud;
import edu.stanford.futuredata.uniserve.utilities.Utilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.nio.file.*;
import java.util.Optional;
import static java.nio.file.Files.walk;

/*
* The Datastore cloud is implemented as a local directory in the directory DatastoresFiles
* Each Datastore has assigned a directory in it.
*
* The shard serialization is given by the user-defined shardToData method of the Shard interface
* This method returns a path to a directory containing the shard
*
* The dataStore.uploadShardToCloud method takes this directory's path and passes it to this method's interface
*
* The uploadShardToCloud method takes this directory, the shard's name and version, builds an identifier "shardCloudName" for
* the shard and copies the content of the directory in this new location.
*
* The downloadShardFromCloud takes a "shardCloudName" and a given destination and copies the content of shardCloudName into
* this given directory.
* */
public class LocalDataStoreCloud implements DataStoreCloud {
    private static final Logger logger = LoggerFactory.getLogger(LocalDataStoreCloud.class);
    private final String root = "src/main/LocalCloud/";
    /*Cartella destinazione rooted nella cartella LocalCloud* "src/main/LocalCloud" */

    public LocalDataStoreCloud(){
        //this.root = String.format("src/main/LocalCloud/%d/", dsid);
        try{
            if(!Files.exists(Path.of(root))) {
                Files.createDirectory(Path.of(root));
            }
        }catch(IOException e){
            if(Utilities.logger_flag)
                logger.error(e.getMessage());
        }
    }

    @Override
    public Optional<String> uploadShardToCloud(Path localSourceDirectory, String shardName, int versionNumber) {
        String shardCloudName = String.format("%s_%d", shardName, versionNumber);
        String destinationDirectoryString = root + shardCloudName;
        try{
            Path path = Path.of(destinationDirectoryString);
            if(Files.exists(path)) {
                deleteDirectoryRecursion(path);
            }
            Files.createDirectory(path);
            copyDirectory(localSourceDirectory.toString(), destinationDirectoryString);
        }catch (IOException e){
            logger.warn("LocalCloud upload failed for shardDirectory {}, shardName {} and version number {}", localSourceDirectory, shardName, versionNumber);
            logger.info(e.getMessage());
            return Optional.empty();
        }
        if(Utilities.logger_flag)
            logger.info("Successful upload to cloud directory {} from local source directory {}", destinationDirectoryString, localSourceDirectory);
        return Optional.of(shardCloudName);
    }

    @Override
    public int downloadShardFromCloud(Path localDestinationDirectory, String shardCloudName) {
        String src = root + shardCloudName;
        Path destinationDirectory = Path.of(localDestinationDirectory.toString(), shardCloudName);
        try {
            if(!Files.exists(destinationDirectory)){
                Files.createDirectory(destinationDirectory);
            }
            copyDirectory(src, destinationDirectory.toString());
        }catch (IOException e){
            logger.warn("LocalCloud download failed for shardDirectory {}, shardCloudName {}", destinationDirectory, shardCloudName);
            return 1;
        }
        return 0;
    }

    private static void copyDirectory(String sourceDirectoryLocation, String destinationDirectoryLocation) throws IOException {
        walk(Paths.get(sourceDirectoryLocation)).
                forEach(source -> {
                    Path destination = Paths.get(destinationDirectoryLocation, source.toString().substring(sourceDirectoryLocation.length()));
                    try{
                        Files.copy(source, destination, StandardCopyOption.REPLACE_EXISTING);
                    } catch (DirectoryNotEmptyException e){
                        try {
                            deleteDirectoryRecursion(destination);
                            Files.copy(source, destination, StandardCopyOption.REPLACE_EXISTING);
                        } catch (IOException ex) {
                            throw new RuntimeException(ex);
                        }
                    }catch (IOException e){
                        e.printStackTrace();
                    }
                });
    }

    public static void deleteDirectoryRecursion(Path path) throws IOException {
        if (Files.isDirectory(path, LinkOption.NOFOLLOW_LINKS)) {
            try (DirectoryStream<Path> entries = Files.newDirectoryStream(path)) {
                for (Path entry : entries) {
                    deleteDirectoryRecursion(entry);
                }
            }
        }
        Files.delete(path);
    }

    public void clear() throws IOException{
        Path path = Path.of(this.root);
        if (Files.isDirectory(path, LinkOption.NOFOLLOW_LINKS)) {
            try (DirectoryStream<Path> entries = Files.newDirectoryStream(path)) {
                for (Path entry : entries) {
                    deleteDirectoryRecursion(entry);
                }
            }
        }
    }
}

