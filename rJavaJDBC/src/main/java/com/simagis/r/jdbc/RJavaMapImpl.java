package com.simagis.r.jdbc;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

class RJavaMapImpl implements RJavaMap {
    private final Map<String, List<String>> map;
    private final String delimiter = "\t";

    RJavaMapImpl() {
        map = new LinkedHashMap<>();
    }

    RJavaMapImpl(Map<String, List<String>> map) {
        this.map = map;
    }

    @Override
    public String names() {
        return map()
                .keySet()
                .stream()
                .collect(Collectors.joining(delimiter));
    }

    @Override
    public String get(String name) {
        return map()
                .getOrDefault(name, Collections.emptyList())
                .stream()
                .collect(Collectors.joining(delimiter));
    }

    @Override
    public int size() {
        return map()
                .values()
                .stream()
                .findFirst()
                .map(List::size)
                .orElse(0);
    }

    @Override
    public Map<String, List<String>> map() {
        return map;
    }
}
