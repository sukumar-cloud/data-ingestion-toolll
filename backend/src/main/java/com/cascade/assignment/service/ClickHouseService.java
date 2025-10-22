package com.cascade.assignment.service;

import org.springframework.stereotype.Service;
import java.sql.*;
import java.io.*;
import java.util.*;

@Service
public class ClickHouseService {
    public Connection connectToClickHouse(String host, int port, String database, String user, String password, String jwtToken, boolean useHttps) throws SQLException {
        String scheme = useHttps ? "jdbc:clickhouse:https://" : "jdbc:clickhouse://";
        StringBuilder url = new StringBuilder(String.format("%s%s:%d/%s", scheme, host, port, database));
        boolean hasJwt = jwtToken != null && !jwtToken.isBlank();
        String sep = "?";
        url.append(sep).append("compress=0");
        sep = "&";
        if (hasJwt) {
            url.append(sep).append("custom_http_headers=")
               .append("Authorization=Bearer ")
               .append(jwtToken);
        }
        String pwd = password != null ? password : "";
        return DriverManager.getConnection(url.toString(), user, pwd);
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
            conn.setAutoCommit(false);
            for (java.util.Map<String, Object> row : data) {
                for (int i = 0; i < columns.size(); i++) {
                    ps.setObject(i + 1, row.get(columns.get(i)));
                }
                ps.addBatch();
            }
            int[] results = ps.executeBatch();
            conn.commit();
            for (int r : results) if (r >= 0) count++;
        }
        return count;
    }

    public int insertDataInChunks(Connection conn, String tableName, java.util.List<String> columns, java.util.List<java.util.Map<String, Object>> data, int chunkSize) throws SQLException {
        if (columns == null || columns.isEmpty() || data == null || data.isEmpty()) return 0;
        String colStr = String.join(",", columns);
        String placeholders = String.join(",", java.util.Collections.nCopies(columns.size(), "?"));
        String sql = String.format("INSERT INTO %s (%s) VALUES (%s)", tableName, colStr, placeholders);
        int totalInserted = 0;
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            conn.setAutoCommit(false);
            for (int i = 0; i < data.size(); i += chunkSize) {
                int end = Math.min(i + chunkSize, data.size());
                java.util.List<java.util.Map<String, Object>> chunk = data.subList(i, end);
                for (java.util.Map<String, Object> row : chunk) {
                    for (int j = 0; j < columns.size(); j++) {
                        ps.setObject(j + 1, row.get(columns.get(j)));
                    }
                    ps.addBatch();
                }
                int[] results = ps.executeBatch();
                totalInserted += results.length;
                conn.commit();
                ps.clearBatch();
            }
        }
        return totalInserted;
    }

    public int insertDataStreaming(Connection conn, String tableName, java.util.List<String> columns, java.io.InputStream csvStream, char delimiter) throws SQLException, java.io.IOException {
        String colStr = String.join(",", columns);
        String placeholders = String.join(",", java.util.Collections.nCopies(columns.size(), "?"));
        String sql = String.format("INSERT INTO %s (%s) VALUES (%s)", tableName, colStr, placeholders);
        int totalInserted = 0;
        int batchSize = 1000;
        try (PreparedStatement ps = conn.prepareStatement(sql);
             java.io.Reader reader = new java.io.InputStreamReader(csvStream);
             org.apache.commons.csv.CSVParser parser = new org.apache.commons.csv.CSVParser(reader,
                 org.apache.commons.csv.CSVFormat.DEFAULT.withDelimiter(delimiter).withFirstRecordAsHeader())) {
            conn.setAutoCommit(false);
            int count = 0;
            for (org.apache.commons.csv.CSVRecord record : parser) {
                for (int i = 0; i < columns.size(); i++) {
                    ps.setObject(i + 1, record.get(columns.get(i)));
                }
                ps.addBatch();
                count++;
                if (count % batchSize == 0) {
                    int[] results = ps.executeBatch();
                    totalInserted += results.length;
                    conn.commit();
                    ps.clearBatch();
                }
            }
            if (count % batchSize != 0) {
                int[] results = ps.executeBatch();
                totalInserted += results.length;
                conn.commit();
            }
        }
        return totalInserted;
    }

    public void createTableIfNotExists(Connection conn, String tableName, java.util.Map<String, Object> sampleRow) throws SQLException {
        StringBuilder sql = new StringBuilder("CREATE TABLE IF NOT EXISTS ").append(tableName).append(" (");
        for (java.util.Map.Entry<String, Object> entry : sampleRow.entrySet()) {
            sql.append(entry.getKey()).append(" ").append(inferClickHouseType(entry.getValue())).append(", ");
        }
        sql.setLength(sql.length() - 2); // Remove trailing comma
        sql.append(") ENGINE = MergeTree() ORDER BY tuple()");
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(sql.toString());
        }
    }

    private String inferClickHouseType(Object value) {
        if (value == null) return "Nullable(String)";
        if (value instanceof Number) {
            if (value instanceof Integer) return "Int32";
            if (value instanceof Long) return "Int64";
            return "Float64";
        } else if (value instanceof Boolean) {
            return "UInt8";
        } else if (value instanceof java.util.Date) {
            return "DateTime";
        }
        return "String";
    }
}
