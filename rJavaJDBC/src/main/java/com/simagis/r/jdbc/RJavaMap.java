package com.simagis.r.jdbc;

import java.util.List;
import java.util.Map;

/**
 * <p>
 * Created by alexei.vylegzhanin@gmail.com on 9/26/2017.
 */
public interface RJavaMap {
    String names();

    String get(String name);

    int size();

    Map<String, List<String>> map();
}
