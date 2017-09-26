package com.simagis.r.jdbc;

import java.sql.SQLException;

public class RJavaMapAsyncTest {
    public static void main(String[] args) throws SQLException, ClassNotFoundException, InterruptedException {
        RDriver rDriver = new RDriver();
        rDriver.setDriverClass("com.intersys.jdbc.CacheDriver");
        rDriver.setUrl("jdbc:Cache://172.17.246.136:56774/DEEPSEE");
        rDriver.setUser("ODBC16");
        rDriver.setPassword(args[0]);

        RJavaMapAsync rJavaMap = rDriver.asyncCall("EXECUTE sp.dsResult_ResultDate('2017-01-01')");

        String status = "RUNNING";
        for (; status.equals("RUNNING"); status = rJavaMap.status()) {
            System.out.print(".");
            Thread.sleep(1000);
        }
        System.out.println("");
        System.out.println(status);

        switch (status) {
            case "DONE": {
                System.out.println(rJavaMap.names());
                System.out.println(rJavaMap.size());
            }
            case "ERROR": {
                System.out.println(rJavaMap.errorMessage());
            }
        }
    }
}