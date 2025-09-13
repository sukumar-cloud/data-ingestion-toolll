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

    public String getType() { return type; }
    public String getHost() { return host; }
    public Integer getPort() { return port; }
    public String getDatabase() { return database; }
    public String getUser() { return user; }
    public String getPassword() { return password; }
    public String getJwtToken() { return jwtToken; }
    public String getFilePath() { return filePath; }
    public String getDelimiter() { return delimiter; }
    public String getTableName() { return tableName; }
    public Boolean getUseHttps() { return useHttps; }
}
