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
    public void setType(String type) { this.type = type; }
    public String getHost() { return host; }
    public void setHost(String host) { this.host = host; }
    public Integer getPort() { return port; }
    public void setPort(Integer port) { this.port = port; }
    public String getDatabase() { return database; }
    public void setDatabase(String database) { this.database = database; }
    public String getUser() { return user; }
    public void setUser(String user) { this.user = user; }
    public String getPassword() { return password; }
    public void setPassword(String password) { this.password = password; }
    public String getJwtToken() { return jwtToken; }
    public void setJwtToken(String jwtToken) { this.jwtToken = jwtToken; }
    public String getFilePath() { return filePath; }
    public void setFilePath(String filePath) { this.filePath = filePath; }
    public String getDelimiter() { return delimiter; }
    public void setDelimiter(String delimiter) { this.delimiter = delimiter; }
    public String getTableName() { return tableName; }
    public void setTableName(String tableName) { this.tableName = tableName; }
    public Boolean getUseHttps() { return useHttps; }
    public void setUseHttps(Boolean useHttps) { this.useHttps = useHttps; }
}
