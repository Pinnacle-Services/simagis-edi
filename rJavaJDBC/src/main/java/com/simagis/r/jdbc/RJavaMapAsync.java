package com.simagis.r.jdbc;

/**
 * <p>
 * Created by alexei.vylegzhanin@gmail.com on 9/26/2017.
 */
public interface RJavaMapAsync extends RJavaMap {

    boolean isReady();

    boolean isError();

    void cancel();

    void join();

}
