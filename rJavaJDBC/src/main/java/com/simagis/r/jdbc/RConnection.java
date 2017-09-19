package com.simagis.r.jdbc;

import java.sql.*;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

public class RConnection implements AutoCloseable {
    private final Connection connection;
    private String nullString = "";

    RConnection(Connection connection) {
        this.connection = connection;
    }

    public String getNullString() {
        return nullString;
    }

    public void setNullString(String nullString) {
        this.nullString = nullString;
    }

    public RJavaMap call(String sql) throws SQLException {
        final LinkedHashMap<String, List<String>> result = new LinkedHashMap<>();
        try (CallableStatement statement = connection.prepareCall(sql)) {
            boolean hadResults = statement.execute();
            if (hadResults) {
                try (ResultSet resultSet = statement.getResultSet()) {
                    final ResultSetMetaData metaData = resultSet.getMetaData();
                    final int columnCount = metaData.getColumnCount();

                    while (resultSet.next()) {
                        for (int i = 1; i <= columnCount; i++) {
                            final String columnName = metaData.getColumnLabel(i);
                            final Object object = resultSet.getObject(i);
                            final String value = object != null ? object.toString() : nullString;
                            result.computeIfAbsent(columnName, s -> new ArrayList<>())
                                    .add(value);
                        }

                    }
                }
            }
        }
        return new RJavaMap(result);
    }

    @Override
    public void close() throws SQLException {
        connection.close();
    }
}
