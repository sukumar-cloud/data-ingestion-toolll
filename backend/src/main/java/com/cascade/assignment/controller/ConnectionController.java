package com.cascade.assignment.controller;

import com.cascade.assignment.dto.ConnectionRequest;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api")
public class ConnectionController {

    @PostMapping("/source/connect")
    public ResponseEntity<String> connectSource(@RequestBody ConnectionRequest request) {
        // TODO: Implement actual connection logic for ClickHouse or file
        if ("clickhouse".equalsIgnoreCase(request.getType())) {
            // Validate ClickHouse connection (mock for now)
            if (request.getHost() != null && request.getPort() != null && request.getDatabase() != null && request.getUser() != null && request.getJwtToken() != null) {
                return ResponseEntity.ok("ClickHouse source connected successfully");
            } else {
                return ResponseEntity.badRequest().body("Missing ClickHouse connection parameters");
            }
        } else if ("file".equalsIgnoreCase(request.getType())) {
            // Validate file (mock for now)
            if (request.getFilePath() != null) {
                return ResponseEntity.ok("File source ready");
            } else {
                return ResponseEntity.badRequest().body("Missing file path");
            }
        }
        return ResponseEntity.badRequest().body("Unknown source type");
    }

    @PostMapping("/target/connect")
    public ResponseEntity<String> connectTarget(@RequestBody ConnectionRequest request) {
        System.out.println("[DEBUG] Target connect request: " + request);
        if (request.getType() == null || request.getType().isEmpty()) {
            // Try to infer type from fields
            if (request.getHost() != null && request.getPort() != null && request.getDatabase() != null && request.getUser() != null) {
                request.setType("clickhouse");
                System.out.println("[WARN] Target type was null, forced to 'clickhouse'");
            } else if (request.getFilePath() != null) {
                request.setType("file");
                System.out.println("[WARN] Target type was null, forced to 'file'");
            } else {
                System.out.println("[ERROR] Target type is missing and cannot be inferred!");
            }
        }

        // TODO: Implement actual connection logic for ClickHouse or file
        if ("clickhouse".equalsIgnoreCase(request.getType())) {
            if (request.getHost() != null && request.getPort() != null && request.getDatabase() != null && request.getUser() != null && request.getJwtToken() != null) {
                return ResponseEntity.ok("ClickHouse target connected successfully");
            } else {
                return ResponseEntity.badRequest().body("Missing ClickHouse connection parameters");
            }
        } else if ("file".equalsIgnoreCase(request.getType())) {
            if (request.getFilePath() != null) {
                return ResponseEntity.ok("File target ready");
            } else {
                return ResponseEntity.badRequest().body("Missing file path");
            }
        }
        return ResponseEntity.badRequest().body("Unknown target type");
    }
}
