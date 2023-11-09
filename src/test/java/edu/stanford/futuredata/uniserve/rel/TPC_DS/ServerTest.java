package edu.stanford.futuredata.uniserve.rel.TPC_DS;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Scanner;

public class ServerTest {

    @Test
    public void serverTest(){
        TestMethods tm = new TestMethods();
        tm.startServers();
        Scanner scanner = new Scanner(System.in);
        String input = scanner.nextLine();
        while (!input.equals("stop")){
            input = scanner.nextLine();
        }
        scanner.close();
        tm.stopServers();
        TestMethods.cleanUp("127.0.0.1", 2181);
    }
}
