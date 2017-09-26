package com.simagis.r.jdbc;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * <p>
 * Created by alexei.vylegzhanin@gmail.com on 9/26/2017.
 */
class RJavaMapAsyncImpl extends RJavaMapImpl implements RJavaMapAsync {

    private final Thread thread;
    private volatile Map<String, List<String>> map = Collections.emptyMap();
    private volatile boolean error;
    private volatile boolean ready;

    RJavaMapAsyncImpl(RDriver rDriver, String sql) {
        thread = new Thread(() -> {
            try (RConnection connection = rDriver.open()) {
                map = connection.call(sql).map();
                ready = true;
            } catch (Throwable e) {
                error = true;
                e.printStackTrace();
            }
        }, "RJavaMapAsyncImpl");
        thread.start();
    }

    @Override
    public Map<String, List<String>> map() {
        return map;
    }

    @Override
    public boolean isReady() {
        return ready;
    }

    @Override
    public boolean isError() {
        return error;
    }

    @Override
    public void cancel() {
        if (!isReady()) {
            thread.interrupt();
        }
    }

    @Override
    public void join() {
        try {
            thread.join();
        } catch (InterruptedException ignored) {
        }
    }

}
