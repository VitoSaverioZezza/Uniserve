package edu.stanford.futuredata.uniserve.awscloud;

import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.*;
import com.amazonaws.util.Base64;
import edu.stanford.futuredata.uniserve.coordinator.CoordinatorCloud;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class AWSCoordinatorCloud implements CoordinatorCloud {

    private final String ami;
    private String launchDataStoreScript;
    private final InstanceType instanceType;

    private AtomicInteger cloudID = new AtomicInteger(0);
    private final Map<Integer, String> cloudIDToInstanceID = new ConcurrentHashMap<>();

    public AWSCoordinatorCloud(String ami, String launchDataStoreScript, InstanceType instanceType) {
        this.ami = ami;
        this.launchDataStoreScript = launchDataStoreScript;
        this.instanceType = instanceType;
    }

    @Override
    public boolean addDataStore() {
        AmazonEC2 ec2 = AmazonEC2ClientBuilder.defaultClient();
        int cloudID = this.cloudID.getAndIncrement();
        assert(launchDataStoreScript.contains("CLOUDID"));
        launchDataStoreScript = launchDataStoreScript.replace("CLOUDID", Integer.toString(cloudID));
        String encodedScript = Base64.encodeAsString(launchDataStoreScript.getBytes());
        RunInstancesRequest runInstancesRequest =
                new RunInstancesRequest().withImageId(ami) // Uniserve datastore image
                        .withInstanceType(instanceType)
                        .withMinCount(1)
                        .withMaxCount(1)
                        .withKeyName("kraftp")
                        .withSecurityGroups("kraftp-uniserve")
                        .withUserData(encodedScript);
        RunInstancesResult result = ec2.runInstances(
                runInstancesRequest);
        if (Objects.isNull(result) || result.getReservation().getInstances().size() != 1) {
            return false;
        }
        String instanceID = result.getReservation().getInstances().get(0).getInstanceId();
        cloudIDToInstanceID.put(cloudID, instanceID);
        return true;
    }

    @Override
    public void removeDataStore(int cloudID) {
        AmazonEC2 ec2 = AmazonEC2ClientBuilder.defaultClient();
        String instanceId = cloudIDToInstanceID.get(cloudID);
        TerminateInstancesRequest terminateInstancesRequest = new TerminateInstancesRequest().withInstanceIds(instanceId);
        TerminateInstancesResult terminateInstancesResult = ec2.terminateInstances(terminateInstancesRequest);
        assert(!Objects.isNull(terminateInstancesResult));
        assert(terminateInstancesResult.getTerminatingInstances().size() == 1);
    }
}