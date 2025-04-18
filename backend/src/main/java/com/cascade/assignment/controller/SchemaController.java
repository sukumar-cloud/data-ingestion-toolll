package com.cascade.assignment.controller;

import com.cascade.assignment.dto.ConnectionRequest;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/source")
public class SchemaController {

    // TODO: Replace with real ClickHouse/CSV logic
    private final com.cascade.assignment.service.ClickHouseService clickHouseService;
    private final com.cascade.assignment.service.FileService fileService;

    public SchemaController(com.cascade.assignment.service.ClickHouseService clickHouseService, com.cascade.assignment.service.FileService fileService) {
        this.clickHouseService = clickHouseService;
        this.fileService = fileService;
    }

    @PostMapping("/tables")
    public ResponseEntity<Map<String, Object>> listTables(@RequestBody ConnectionRequest request) {
        try {
            if ("clickhouse".equalsIgnoreCase(request.getType())) {
                try (java.sql.Connection conn = clickHouseService.connectToClickHouse(request.getHost(), request.getPort(), request.getDatabase(), request.getUser(), request.getJwtToken())) {
                    java.util.List<String> tables = clickHouseService.listTables(conn);
                    return ResponseEntity.ok(Map.of("tables", tables));
                }
            } else if ("file".equalsIgnoreCase(request.getType())) {
                // Flat file: just return 'CSV File'
                return ResponseEntity.ok(Map.of("tables", java.util.List.of("CSV File")));
            }
            return ResponseEntity.badRequest().body(Map.of("error", "Unknown source type"));
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
        }
    }

    @PostMapping("/columns")
    public ResponseEntity<Map<String, Object>> listColumns(@RequestBody ConnectionRequest request) {
        try {
            if ("clickhouse".equalsIgnoreCase(request.getType())) {
                try (java.sql.Connection conn = clickHouseService.connectToClickHouse(request.getHost(), request.getPort(), request.getDatabase(), request.getUser(), request.getJwtToken())) {
                    java.util.List<String> columns = clickHouseService.listColumns(conn, request.getTableName());
                    return ResponseEntity.ok(Map.of("columns", columns));
                }
            } else if ("file".equalsIgnoreCase(request.getType())) {
                // For file, try to read header from CSV file path
                java.io.File file = new java.io.File(request.getFilePath());
                java.util.List<java.util.Map<String, Object>> data = fileService.readCsv(file, request.getDelimiter() != null && !request.getDelimiter().isEmpty() ? request.getDelimiter().charAt(0) : ',');
                if (!data.isEmpty()) {
                    java.util.List<String> columns = new java.util.ArrayList<>(data.get(0).keySet());
                    return ResponseEntity.ok(Map.of("columns", columns));
                } else {
                    return ResponseEntity.ok(Map.of("columns", java.util.List.of()));
                }
            }
            return ResponseEntity.badRequest().body(Map.of("error", "Unknown source type"));
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
        }
    }
}
