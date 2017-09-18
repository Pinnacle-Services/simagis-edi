package com.simagis.r.jdbc;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class RJavaMap {
    private final Map<String, List<String>> map;
    private final String delimiter = "\t";

    RJavaMap(Map<String, List<String>> map) {
        this.map = map;
    }

    public String names() {
        return map
                .keySet()
                .stream()
                .collect(Collectors.joining(delimiter));
    }

    public String get(String name) {
        return map
                .getOrDefault(name, Collections.emptyList())
                .stream()
                .collect(Collectors.joining(delimiter));
    }

    public int size() {
        return map
                .values()
                .stream()
                .findFirst()
                .map(List::size)
                .orElse(0);
    }

    public Map<String, List<String>> map() {
        return map;
    }
}
