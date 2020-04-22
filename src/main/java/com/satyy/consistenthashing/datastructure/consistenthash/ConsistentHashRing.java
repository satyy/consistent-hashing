package com.satyy.consistenthashing.datastructure.consistenthash;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;

public class ConsistentHashRing<T extends Node> implements HashRing<T>, Serializable {

    private static final long serialVersionUID = 10L;

    private static final int DEFAULT_VIRTUAL_NODES = 3;

    private static final int DEFAULT_RING_SIZE = 1000;

    private static final String NODE_KEY_PREFIX = "node";

    private static final String NODE_KEY_SEPARATOR = "_";

    private final transient SortedMap<Integer, ConsistentHashNode<T>> hashRingNodes = new TreeMap<>();

    private final transient Map<Integer, List<Integer>> virtualNodeMap = new HashMap<>();

    private int virtualNodeCount;

    private int ringSize;

    public ConsistentHashRing() {
        this.virtualNodeCount = DEFAULT_VIRTUAL_NODES;
        this.ringSize = DEFAULT_RING_SIZE;
    }

    public ConsistentHashRing(Collection<T> ringNodes) {
        this(ringNodes, DEFAULT_RING_SIZE, DEFAULT_VIRTUAL_NODES);
    }


    public ConsistentHashRing(Collection<T> nodes, int ringSize, int nodeReplica) {
        if (nodeReplica > 0) {
            this.virtualNodeCount = nodeReplica;
        } else {
            this.virtualNodeCount = DEFAULT_VIRTUAL_NODES;
        }
        if (ringSize > 3 * (nodes.size() * (nodeReplica + 1))) {
            this.ringSize = ringSize;
        }

        addNodes(nodes);
    }

    @Override
    public void addNodes(Collection<T> nodes) {
        nodes.forEach(this::addNode);
    }

    @Override
    public void addNode(T node) {
        String nodeKey = getNodeKey(0, node.getKey());
        int nodeIndex = getRingIndex(nodeKey);
        ConsistentHashNode<T> ringNode = new ConsistentHashNode<>(nodeKey, node, false);
        hashRingNodes.put(nodeIndex, ringNode);
        List<Integer> virtualNodesIndexes = new ArrayList<>(virtualNodeCount);

        for (int i = 1; i <= virtualNodeCount; i++) {
            String virtualNodeKey = getNodeKey(i, node.getKey());
            int virtualNodeIndex = getRingIndex(virtualNodeKey);
            ConsistentHashNode<T> virtualNode = new ConsistentHashNode<>(virtualNodeKey, node, true);
            hashRingNodes.put(virtualNodeIndex, virtualNode);
            virtualNodesIndexes.add(virtualNodeIndex);
        }
        virtualNodeMap.put(nodeIndex, virtualNodesIndexes);
    }

    @Override
    public boolean removeNode(String nodeKey) {
        int nodeIndex = getRingIndex(getNodeKey(0, nodeKey));
        if (!hashRingNodes.containsKey(nodeIndex))
            return false;

        virtualNodeMap.get(nodeIndex).forEach(hashRingNodes::remove);
        virtualNodeMap.remove(nodeIndex);
        hashRingNodes.remove(nodeIndex);

        return true;
    }

    @Override
    public Optional<T> getNode(String nodeKey) {
        int nodeIndex = getRingIndex(getNodeKey(0, nodeKey));
        if (!hashRingNodes.containsKey(nodeIndex))
            return Optional.empty();
        return Optional.of(hashRingNodes.get(nodeIndex).getNode());
    }

    @Override
    public T requestServiceNode(String key) {
        if (hashRingNodes.isEmpty())
            throw new IllegalStateException("Hash Ring Empty!!! No nodes added");

        int index = getRingIndex(key);
        System.out.println("Key : " + index);
        SortedMap<Integer, ConsistentHashNode<T>> tailMap = hashRingNodes.tailMap(index);
        int serviceNodeIndex = tailMap.isEmpty() ? hashRingNodes.firstKey() : tailMap.firstKey();
        return hashRingNodes.get(serviceNodeIndex).getNode();
    }

    @Override
    public int size() {
        return hashRingNodes.size() / (virtualNodeCount + 1);
    }

    public void nodeIndex() {
        hashRingNodes.forEach((key, value) -> {
            System.out.println("****Node**** ");
            System.out.println("Index: " + key);
            System.out.println("Virtual: " + value.isVirtual());
            System.out.println("Key: " + value.getNode().getKey());
            System.out.println("Gen Key: " + value.getRingNodeKey());
            System.out.println("-----------------------\n");
        });
    }

    public int replicaCount() {
        return virtualNodeCount;
    }

    private int getRingIndex(String key) {
        return getHashCode(key) % ringSize;
    }

    private int getHashCode (String key) {
        return Math.abs(key.hashCode());
    }

    private String getNodeKey(int virtualNodeNumber, String key) {
        return NODE_KEY_PREFIX.concat(NODE_KEY_SEPARATOR)
                .concat(String.valueOf(virtualNodeNumber)).concat(NODE_KEY_SEPARATOR)
                .concat(key);
    }
}
