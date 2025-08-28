package com.cascade.assignment.controller;

import com.cascade.assignment.dto.ConnectionRequest;
import com.cascade.assignment.service.ClickHouseService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.sql.Connection;
import java.sql.SQLException;

@RestController
@RequestMapping("/api")
public class ConnectionController {

    private final ClickHouseService clickHouseService;

    public ConnectionController(ClickHouseService clickHouseService) {
        this.clickHouseService = clickHouseService;
    }

    @PostMapping("/source/connect")
    public ResponseEntity<String> connectSource(@RequestBody ConnectionRequest request) {
        if ("clickhouse".equalsIgnoreCase(request.getType())) {
            // Validate ClickHouse connection (mock for now). Accept either JWT or password; port can be null (defaults later)
            boolean hasAuth = (request.getJwtToken() != null && !request.getJwtToken().isBlank()) || (request.getPassword() != null && !request.getPassword().isBlank());
            if (request.getHost() != null && request.getDatabase() != null && request.getUser() != null && hasAuth) {
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

        // Implement actual connection logic for ClickHouse or file
        if ("clickhouse".equalsIgnoreCase(request.getType())) {
            boolean hasAuth = (request.getJwtToken() != null && !request.getJwtToken().isBlank()) || (request.getPassword() != null && !request.getPassword().isBlank());
            if (request.getHost() == null || request.getDatabase() == null || request.getUser() == null || !hasAuth) {
                return ResponseEntity.badRequest().body("Missing ClickHouse connection parameters");
            }

            // Defaults and HTTPS handling
            Integer port = request.getPort();
            boolean useHttps = request.getUseHttps() != null && request.getUseHttps();
            if (port == null) {
                port = 8123;
            }
            if (port == 8443) {
                useHttps = true;
            }

            try (Connection conn = clickHouseService.connectToClickHouse(
                    request.getHost(),
                    port,
                    request.getDatabase(),
                    request.getUser(),
                    request.getPassword(),
                    request.getJwtToken(),
                    useHttps
            )) {
                // Simple ping
                if (conn != null && !conn.isClosed()) {
                    return ResponseEntity.ok("ClickHouse target connected successfully");
                } else {
                    return ResponseEntity.badRequest().body("Unable to open ClickHouse connection");
                }
            } catch (SQLException ex) {
                return ResponseEntity.badRequest().body("ClickHouse connection failed: " + ex.getMessage());
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
