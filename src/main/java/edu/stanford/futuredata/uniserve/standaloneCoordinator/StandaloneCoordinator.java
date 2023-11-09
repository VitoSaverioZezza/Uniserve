package edu.stanford.futuredata.uniserve.standaloneCoordinator;

import edu.stanford.futuredata.uniserve.coordinator.Coordinator;
import edu.stanford.futuredata.uniserve.coordinator.DefaultLoadBalancer;
import edu.stanford.futuredata.uniserve.localcloud.LocalCoordinatorCloud;
import edu.stanford.futuredata.uniserve.relational.RelShardFactory;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.util.Scanner;

public class StandaloneCoordinator {
    public static void main(String[] args) throws InterruptedException {
        String zkHost = args[0];
        Integer zkPort = Integer.valueOf(args[1]);
        String coordinatorHost = args[2];
        Integer coordinatorPort = Integer.valueOf(args[3]);
        Coordinator coordinator = new Coordinator(
                new LocalCoordinatorCloud(new RelShardFactory()),
                new DefaultLoadBalancer(),
                null,
                zkHost,
                zkPort,
                coordinatorHost,
                coordinatorPort
        );
        coordinator.runLoadBalancerDaemon = false;
        coordinator.startServing();
        Runtime.getRuntime().addShutdownHook(new Thread(coordinator::stopServing));
        Thread.sleep(Long.MAX_VALUE);
    }

    public static void cleanUp(String zkHost, int zkPort) {
        System.out.println("Clean up ZK...");
        // Clean up ZooKeeper
        String connectString = String.format("%s:%d", zkHost, zkPort);
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        CuratorFramework cf = CuratorFrameworkFactory.newClient(connectString, retryPolicy);
        cf.start();
        try {
            for (String child : cf.getChildren().forPath("/")) {
                if (!child.equals("zookeeper")) {
                    cf.delete().deletingChildrenIfNeeded().forPath("/" + child);
                }
            }
        } catch (Exception e) {
            ;
        }
        System.out.println("ZK cleanup ok\n");
    }
}
