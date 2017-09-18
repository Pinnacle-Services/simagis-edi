package com.simagis.r.jdbc;

import java.sql.DriverManager;
import java.sql.SQLException;

public class RDriver {
    private String driverClass;
    private String url;
    private String user;
    private String password;

    public String getDriverClass() {
        return driverClass;
    }

    public void setDriverClass(String driverClass) {
        this.driverClass = driverClass;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public RConnection open() throws ClassNotFoundException, SQLException {
        Class.forName(driverClass);
        return new RConnection(DriverManager.getConnection(url, user,  password));
    }

    public RJavaMap call(String sql) throws SQLException, ClassNotFoundException {
        try (RConnection connection = open()) {
            return connection.call(sql);
        }
    }
}
