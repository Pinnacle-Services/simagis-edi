package com.simagis.r.jdbc;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

public class RJavaMapAsync2Test {
    public static void main(String[] args) throws SQLException, ClassNotFoundException, InterruptedException {
        RDriver rDriver = new RDriver();
        rDriver.setDriverClass("com.intersys.jdbc.CacheDriver");
        rDriver.setUrl("jdbc:Cache://172.17.246.136:56774/DEEPSEE");
        rDriver.setUser("ODBC16");
        rDriver.setPassword(args[0]);

        final List<RJavaMapAsync> asyncs = Arrays.asList(
                rDriver.asyncCall("EXECUTE sp.dsResult_ResultDate('2017-01-01')"),
                rDriver.asyncCall("EXECUTE sp.dsResult_ResultDate('2017-01-02')"));

        System.out.println("Waiting...");
        for (; asyncs.stream().anyMatch(async -> async.status().equals("RUNNING")); ) {
            System.out.print(".");
            Thread.sleep(1000);
        }

        for (RJavaMapAsync async : asyncs) {
            String status = async.status();
            switch (status) {
                case "DONE": {
                    System.out.println(async.names());
                    System.out.println(async.size());
                }
                case "ERROR": {
                    System.out.println(async.errorMessage());
                }
            }
        }

    }
}