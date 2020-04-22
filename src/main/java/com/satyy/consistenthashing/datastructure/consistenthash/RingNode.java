package com.satyy.consistenthashing.datastructure.consistenthash;

public class RingNode implements Node {

    private String key;

    public RingNode(String key) {
        this.key = key;
    }

    @Override
    public String getKey() {
        return key;
    }

}
