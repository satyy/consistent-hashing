package com.satyy.consistenthashing.datastructure.consistenthash;

class ConsistentHashNode<T extends Node> {
    private String ringNodeKey;
    private boolean virtual;
    private T node;

    public ConsistentHashNode(String ringNodeKey, T node, boolean virtual) {
        this.virtual = virtual;
        this.node = node;
        this.ringNodeKey = ringNodeKey;
    }

    public String getRingNodeKey() {
        return ringNodeKey;
    }

    public boolean isVirtual() {
        return virtual;
    }

    public T getNode() {
        return node;
    }
}
