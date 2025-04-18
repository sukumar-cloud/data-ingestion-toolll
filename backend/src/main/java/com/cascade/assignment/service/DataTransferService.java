package com.cascade.assignment.service;

import org.springframework.stereotype.Service;

@Service
public class DataTransferService {
    private final ClickHouseService clickHouseService;
    private final FileService fileService;

    public DataTransferService(ClickHouseService clickHouseService, FileService fileService) {
        this.clickHouseService = clickHouseService;
        this.fileService = fileService;
    }

    // ClickHouse -> Flat File (CSV export)
    public int transferClickHouseToCsv(String host, int port, String database, String user, String jwtToken,
                                       String tableName, java.io.File targetFile, char delimiter, java.util.List<String> columns) throws Exception {
        try (java.sql.Connection conn = clickHouseService.connectToClickHouse(host, port, database, user, jwtToken)) {
            java.util.List<java.util.Map<String, Object>> data = clickHouseService.previewTable(conn, tableName, Integer.MAX_VALUE, 0);
            // If columns are specified, filter data
            if (columns != null && !columns.isEmpty()) {
                data = filterColumns(data, columns);
            }
            fileService.writeCsv(targetFile, data, delimiter);
            return data.size();
        }
    }

    // Flat File -> ClickHouse (CSV import)
    public int transferCsvToClickHouse(java.io.File csvFile, char delimiter, String host, int port, String database, String user, String jwtToken,
                                       String tableName, java.util.List<String> columns) throws Exception {
        java.util.List<java.util.Map<String, Object>> data = fileService.readCsv(csvFile, delimiter);
        // If columns are specified, filter data
        if (columns != null && !columns.isEmpty()) {
            data = filterColumns(data, columns);
        }
        try (java.sql.Connection conn = clickHouseService.connectToClickHouse(host, port, database, user, jwtToken)) {
            return clickHouseService.insertData(conn, tableName, columns != null && !columns.isEmpty() ? columns : new java.util.ArrayList<>(data.get(0).keySet()), data);
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
