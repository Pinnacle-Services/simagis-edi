package com.simagis.r.jdbc;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * <p>
 * Created by alexei.vylegzhanin@gmail.com on 9/26/2017.
 */
class RJavaMapAsyncImpl extends RJavaMapImpl implements RJavaMapAsync {
    private static final String RUNNING = "RUNNING";
    private static final String DONE = "DONE";
    private static final String ERROR = "ERROR";

    private final Thread thread;
    private volatile Map<String, List<String>> map = Collections.emptyMap();
    private volatile String status = RUNNING;
    private volatile String errorMessage = "";

    RJavaMapAsyncImpl(RDriver rDriver, String sql) {
        thread = new Thread(() -> {
            try (RConnection connection = rDriver.open()) {
                map = connection.call(sql).map();
                status = DONE;
            } catch (Throwable e) {
                errorMessage = e.getMessage();
                if (errorMessage == null || errorMessage.isEmpty()) errorMessage = e.getClass().getName();
                status = ERROR;
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
    public String status() {
        return status;
    }

    @Override
    public String errorMessage() {
        return errorMessage;
    }

    @Override
    public void cancel() {
        if (thread.isAlive()) {
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
