package com.cascade.assignment.service;

import org.springframework.stereotype.Service;
import org.springframework.beans.factory.annotation.Value;

import java.sql.*;
import java.io.*;
import java.util.*;

@Service
public class DataTransferService {
    private final ClickHouseService clickHouseService;
    private final FileService fileService;

    @Value("${app.batch.size:1000}")
    private int batchSize;

    @Value("${app.streaming.threshold:10000}")
    private int streamingThreshold;

    public DataTransferService(ClickHouseService clickHouseService, FileService fileService) {
        this.clickHouseService = clickHouseService;
        this.fileService = fileService;
    }

    // ClickHouse -> Flat File (CSV export)
    public int transferClickHouseToCsv(String host, int port, String database, String user, String password, String jwtToken, boolean useHttps,
                                       String tableName, java.io.File targetFile, char delimiter, java.util.List<String> columns) throws Exception {
        try (java.sql.Connection conn = clickHouseService.connectToClickHouse(host, port, database, user, password, jwtToken, useHttps)) {
            java.util.List<java.util.Map<String, Object>> data = clickHouseService.previewTable(conn, tableName, Integer.MAX_VALUE, 0);
            // If columns are specified, filter data
            if (columns != null && !columns.isEmpty()) {
                data = filterColumns(data, columns);
            }
            fileService.writeCsv(targetFile, data, delimiter);
            return data.size();
        }
    }

    // Optimized Flat File -> ClickHouse (CSV import) with streaming for large files
    public int transferCsvToClickHouse(java.io.File csvFile, char delimiter, String host, int port, String database, String user, String password, String jwtToken, boolean useHttps,
                                       String tableName, java.util.List<String> columns) throws Exception {
        // Get sample data to infer columns and check size
        java.util.List<java.util.Map<String, Object>> sampleData = fileService.readCsv(csvFile, delimiter);
        if (sampleData.isEmpty()) return 0;

        // Determine columns
        java.util.List<String> finalColumns = columns != null && !columns.isEmpty() ? columns : new java.util.ArrayList<>(sampleData.get(0).keySet());

        try (java.sql.Connection conn = clickHouseService.connectToClickHouse(host, port, database, user, password, jwtToken, useHttps)) {
            // Create table if not exists
            clickHouseService.createTableIfNotExists(conn, tableName, sampleData.get(0));

            // Use streaming for large files (10k+ rows) to minimize memory usage
            if (sampleData.size() > streamingThreshold) {
                try (java.io.InputStream stream = new java.io.FileInputStream(csvFile)) {
                    return clickHouseService.insertDataStreaming(conn, tableName, finalColumns, stream, delimiter);
                }
            } else {
                // Use chunked batch for smaller files
                return clickHouseService.insertDataInChunks(conn, tableName, finalColumns, sampleData, batchSize);
            }
        }
    }

    // Helper to filter columns
    private java.util.List<java.util.Map<String, Object>> filterColumns(java.util.List<java.util.Map<String, Object>> data, java.util.List<String> columns) {
        java.util.List<java.util.Map<String, Object>> filtered = new java.util.ArrayList<>();
        for (java.util.Map<String, Object> row : data) {
            java.util.Map<String, Object> filteredRow = new java.util.HashMap<>();
            for (String col : columns) {
                filteredRow.put(col, row.get(col));
            }
            filtered.add(filteredRow);
        }
        return filtered;
    }
}
