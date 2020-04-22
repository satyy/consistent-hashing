package com.satyy.consistenthashing;

import com.satyy.consistenthashing.datastructure.consistenthash.ConsistentHashRing;
import com.satyy.consistenthashing.datastructure.consistenthash.RingNode;

import java.util.Arrays;
import java.util.List;

public class Main {

    public static void main(String[] args) {
        ConsistentHashRing<RingNode> hashRing = new ConsistentHashRing<>();
        RingNode node1 = new RingNode("redisserver1");
        RingNode node2 = new RingNode("server");
        RingNode node3 = new RingNode("oracleserver");

        List<RingNode> ringNodes = Arrays.asList(node1, node2, node3);
        hashRing.addNodes(ringNodes);

        System.out.println("Ring Size: " + hashRing.size());
        hashRing.nodeIndex();

        System.out.println("\n----------Request Service--------\n");
        System.out.println(hashRing.requestServiceNode("127.0.0.1").getKey());
        System.out.println(hashRing.requestServiceNode("192.0.0.1").getKey());

        System.out.println("Hello World!");
    }
}
