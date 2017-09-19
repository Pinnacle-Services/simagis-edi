package com.simagis.r.jdbc;

import java.sql.SQLException;

import static org.junit.Assert.*;

public class RConnectionTest {
    public static void main(String[] args) throws SQLException, ClassNotFoundException {
        RDriver rDriver = new RDriver();
        rDriver.setDriverClass("com.intersys.jdbc.CacheDriver");
        rDriver.setUrl("jdbc:Cache://172.17.246.136:56774/DEEPSEE");
        rDriver.setUser("ODBC16");
        rDriver.setPassword(args[0]);

        RJavaMap rJavaMap = rDriver.call("EXECUTE sp.dsResult_ResultDate('2017-01-01')");

        System.out.println(rJavaMap.names());
        System.out.println(rJavaMap.size());
    }
}