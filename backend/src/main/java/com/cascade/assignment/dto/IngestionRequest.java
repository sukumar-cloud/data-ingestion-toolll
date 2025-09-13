package com.cascade.assignment.dto;

import lombok.Data;
import java.util.List;

@Data
public class IngestionRequest {
    private ConnectionRequest source;
    private ConnectionRequest target;
    private List<String> columns;

    public ConnectionRequest getSource() { return source; }
    public ConnectionRequest getTarget() { return target; }
    public List<String> getColumns() { return columns; }
}
