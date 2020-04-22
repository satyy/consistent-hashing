package com.satyy.consistenthashing.datastructure.consistenthash;

import java.util.Collection;
import java.util.Optional;

public interface HashRing<T extends Node> {
    void addNode(T node);

    void addNodes(Collection<T> node);

    boolean removeNode(String nodeKey);

    Optional<T> getNode(String nodeKey);

    T requestServiceNode(String key);

    int size();

}
