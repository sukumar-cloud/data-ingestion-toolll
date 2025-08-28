package com.cascade.assignment.dto;

import lombok.Data;

@Data
public class ConnectionRequest {
    private String type; 
    private String host;
    private Integer port;
    private String database;
    private String user;
    private String password;
    private String jwtToken;
    private String filePath;
    private String delimiter;
    private String tableName;
    private Boolean useHttps; 
}
