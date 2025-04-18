package com.cascade.assignment.dto;

import lombok.Data;
import java.util.List;
import java.util.Map;

@Data
public class PreviewResponse {
    private List<String> columns;
    private List<Map<String, Object>> data;
}
