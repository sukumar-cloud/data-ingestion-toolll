package com.cascade.assignment.service;

import org.springframework.stereotype.Service;
import java.sql.*;

@Service
public class ClickHouseService {
    public Connection connectToClickHouse(String host, int port, String database, String user, String jwtToken) throws SQLException {
        String url = String.format("jdbc:clickhouse://%s:%d/%s?custom_http_headers=Authorization=Bearer %s", host, port, database, jwtToken);
        return DriverManager.getConnection(url, user, "");
    }

    public java.util.List<String> listTables(Connection conn) throws SQLException {
        java.util.List<String> tables = new java.util.ArrayList<>();
        String sql = "SHOW TABLES";
        try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                tables.add(rs.getString(1));
            }
        }
        return tables;
    }

    public java.util.List<String> listColumns(Connection conn, String tableName) throws SQLException {
        java.util.List<String> columns = new java.util.ArrayList<>();
        String sql = "DESCRIBE TABLE " + tableName;
        try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                columns.add(rs.getString(1)); // first column is column name
            }
        }
        return columns;
    }

    public java.util.List<java.util.Map<String, Object>> previewTable(Connection conn, String tableName, int limit, int offset) throws SQLException {
        java.util.List<java.util.Map<String, Object>> rows = new java.util.ArrayList<>();
        String sql = String.format("SELECT * FROM %s LIMIT %d OFFSET %d", tableName, limit, offset);
        try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(sql)) {
            java.sql.ResultSetMetaData meta = rs.getMetaData();
            int colCount = meta.getColumnCount();
            while (rs.next()) {
                java.util.Map<String, Object> row = new java.util.HashMap<>();
                for (int i = 1; i <= colCount; i++) {
                    row.put(meta.getColumnName(i), rs.getObject(i));
                }
                rows.add(row);
            }
        }
        return rows;
    }

    public int insertData(Connection conn, String tableName, java.util.List<String> columns, java.util.List<java.util.Map<String, Object>> data) throws SQLException {
        if (columns == null || columns.isEmpty() || data == null || data.isEmpty()) return 0;
        String colStr = String.join(",", columns);
        String placeholders = String.join(",", java.util.Collections.nCopies(columns.size(), "?"));
        String sql = String.format("INSERT INTO %s (%s) VALUES (%s)", tableName, colStr, placeholders);
        int count = 0;
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            for (java.util.Map<String, Object> row : data) {
                for (int i = 0; i < columns.size(); i++) {
                    ps.setObject(i + 1, row.get(columns.get(i)));
                }
                ps.addBatch();
            }
            int[] results = ps.executeBatch();
            for (int r : results) if (r >= 0) count++;
        }
        return count;
    }
}
