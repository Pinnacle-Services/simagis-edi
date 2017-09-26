package com.simagis.r.jdbc;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

public class RJavaTest {

    public static RJavaMap query(String date) {
        final LinkedHashMap<String, List<String>> result = new LinkedHashMap<>();
        for (int i = 0; i < 1000000; i++) {
            result.computeIfAbsent("A", s -> new ArrayList<>()).add("A=" + i);
            result.computeIfAbsent("B", s -> new ArrayList<>()).add("B=" + (i * 2));
            result.computeIfAbsent("C", s -> new ArrayList<>()).add("C=" + (i * 3));
        }
        return new RJavaMapImpl(result);
    }

    public static void main(String[] args) {
        RJavaMap res = query("");
        System.out.println(res.names());
    }
}
