package com.cascade.assignment.controller;

import com.cascade.assignment.dto.ConnectionRequest;
import com.cascade.assignment.dto.PreviewResponse;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.*;

@RestController
@RequestMapping("/api")
@CrossOrigin(origins = "http://localhost:8081")
public class PreviewController {

    // TODO: Replace with real ClickHouse/CSV logic
    private final com.cascade.assignment.service.ClickHouseService clickHouseService;
    private final com.cascade.assignment.service.FileService fileService;

    public PreviewController(com.cascade.assignment.service.ClickHouseService clickHouseService, com.cascade.assignment.service.FileService fileService) {
        this.clickHouseService = clickHouseService;
        this.fileService = fileService;
    }

    @PostMapping("/source/preview")
    public ResponseEntity<PreviewResponse> previewSource(@RequestBody ConnectionRequest request) {
        PreviewResponse response = new PreviewResponse();
        try {
            if ("clickhouse".equalsIgnoreCase(request.getType())) {
                try (java.sql.Connection conn = clickHouseService.connectToClickHouse(request.getHost(), request.getPort(), request.getDatabase(), request.getUser(), request.getJwtToken())) {
                    java.util.List<java.util.Map<String, Object>> data = clickHouseService.previewTable(conn, request.getTableName(), 100, 0);
                    response.setData(data);
                    if (!data.isEmpty()) {
                        response.setColumns(new java.util.ArrayList<>(data.get(0).keySet()));
                    } else {
                        response.setColumns(clickHouseService.listColumns(conn, request.getTableName()));
                    }
                }
            } else if ("file".equalsIgnoreCase(request.getType())) {
                java.io.File file = new java.io.File(request.getFilePath());
                java.util.List<java.util.Map<String, Object>> data = fileService.readCsv(file, request.getDelimiter() != null && !request.getDelimiter().isEmpty() ? request.getDelimiter().charAt(0) : ',');
                response.setData(data.size() > 100 ? data.subList(0, 100) : data);
                if (!data.isEmpty()) {
                    response.setColumns(new java.util.ArrayList<>(data.get(0).keySet()));
                }
            }
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            response.setColumns(java.util.List.of());
            response.setData(java.util.List.of());
            return ResponseEntity.ok(response);
        }
    }

    @PostMapping(value = "/preview-csv", consumes = "multipart/form-data", produces = "application/json")
    public ResponseEntity<PreviewResponse> previewCsv(@RequestParam("file") org.springframework.web.multipart.MultipartFile file, @RequestParam(value = "delimiter", defaultValue = ",") String delimiter) {
        PreviewResponse response = new PreviewResponse();
        try {
            java.io.File tempFile = java.io.File.createTempFile("preview", ".csv");
            file.transferTo(tempFile);
            java.util.List<java.util.Map<String, Object>> data = fileService.readCsv(tempFile, delimiter.charAt(0));
            response.setData(data.size() > 100 ? data.subList(0, 100) : data);
            if (!data.isEmpty()) {
                response.setColumns(new java.util.ArrayList<>(data.get(0).keySet()));
            }
            tempFile.delete();
        } catch (Exception e) {
            response.setColumns(java.util.List.of());
            response.setData(java.util.List.of());
        }
        return ResponseEntity.ok(response);
    }
}
