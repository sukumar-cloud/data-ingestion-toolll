package com.cascade.assignment.controller;

import com.cascade.assignment.dto.IngestionRequest;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/api")
public class IngestionController {

    private final com.cascade.assignment.service.DataTransferService dataTransferService;

    public IngestionController(com.cascade.assignment.service.DataTransferService dataTransferService) {
        this.dataTransferService = dataTransferService;
    }

    // General ingestion (ClickHouse <-> Flat File)
    @PostMapping("/ingest")
    public ResponseEntity<Map<String, Object>> startIngestion(@RequestBody IngestionRequest request) {
        Map<String, Object> resp = new HashMap<>();
        try {
            int recordCount = 0;
            // ClickHouse -> Flat File (CSV export)
            if ("clickhouse".equalsIgnoreCase(request.getSource().getType()) && "file".equalsIgnoreCase(request.getTarget().getType())) {
                String host = request.getSource().getHost();
                int port = request.getSource().getPort();
                String database = request.getSource().getDatabase();
                String user = request.getSource().getUser();
                String jwtToken = request.getSource().getJwtToken();
                String tableName = request.getSource().getTableName();
                String filePath = request.getTarget().getFilePath();
                char delimiter = request.getTarget().getDelimiter() != null && !request.getTarget().getDelimiter().isEmpty() ? request.getTarget().getDelimiter().charAt(0) : ',';
                java.io.File file = new java.io.File(filePath);
                recordCount = dataTransferService.transferClickHouseToCsv(host, port, database, user, jwtToken, tableName, file, delimiter, request.getColumns());
            }
            // ClickHouse -> ClickHouse (optional, same logic, just for demo)
            // else if ("clickhouse".equalsIgnoreCase(request.getSource().getType()) && "clickhouse".equalsIgnoreCase(request.getTarget().getType())) { ... }
            resp.put("status", "success");
            resp.put("recordsTransferred", recordCount);
            return ResponseEntity.ok(resp);
        } catch (Exception e) {
            resp.put("status", "error");
            resp.put("message", e.getMessage());
            return ResponseEntity.badRequest().body(resp);
        }
    }

    // Flat File -> ClickHouse (with file upload)
    @PostMapping("/transfer-csv-to-clickhouse")
    public ResponseEntity<Map<String, Object>> transferCsvToClickHouse(
            @RequestPart("file") MultipartFile file,
            @RequestPart("ingestionRequest") IngestionRequest ingestionRequest
    ) {
        Map<String, Object> resp = new HashMap<>();
        try {
            String fileName = file.getOriginalFilename();
            java.io.File tempFile = java.io.File.createTempFile("ingest", fileName != null ? fileName : ".csv");
            file.transferTo(tempFile);
            char delimiter = ingestionRequest.getSource().getDelimiter() != null && !ingestionRequest.getSource().getDelimiter().isEmpty() ? ingestionRequest.getSource().getDelimiter().charAt(0) : ',';
            int recordCount = dataTransferService.transferCsvToClickHouse(
                    tempFile,
                    delimiter,
                    ingestionRequest.getTarget().getHost(),
                    ingestionRequest.getTarget().getPort(),
                    ingestionRequest.getTarget().getDatabase(),
                    ingestionRequest.getTarget().getUser(),
                    ingestionRequest.getTarget().getJwtToken(),
                    ingestionRequest.getTarget().getTableName(),
                    ingestionRequest.getColumns()
            );
            tempFile.delete();
            resp.put("status", "success");
            resp.put("recordsTransferred", recordCount);
            return ResponseEntity.ok(resp);
        } catch (Exception e) {
            resp.put("status", "error");
            resp.put("message", e.getMessage());
            return ResponseEntity.badRequest().body(resp);
        }
    }
}
