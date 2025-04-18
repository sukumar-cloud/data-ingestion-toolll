package com.cascade.assignment.service;

import org.springframework.stereotype.Service;
import java.io.*;
import java.util.*;

@Service
public class FileService {
    public List<Map<String, Object>> readCsv(File csvFile, char delimiter) throws IOException {
        List<Map<String, Object>> data = new ArrayList<>();
        try (Reader reader = new FileReader(csvFile)) {
            org.apache.commons.csv.CSVFormat format = org.apache.commons.csv.CSVFormat.DEFAULT.withDelimiter(delimiter).withFirstRecordAsHeader();
            org.apache.commons.csv.CSVParser parser = new org.apache.commons.csv.CSVParser(reader, format);
            List<String> headers = parser.getHeaderNames();
            for (org.apache.commons.csv.CSVRecord record : parser) {
                Map<String, Object> row = new HashMap<>();
                for (String header : headers) {
                    row.put(header, record.get(header));
                }
                data.add(row);
            }
        }
        return data;
    }

    public void writeCsv(File csvFile, List<Map<String, Object>> data, char delimiter) throws IOException {
        if (data == null || data.isEmpty()) return;
        List<String> headers = new ArrayList<>(data.get(0).keySet());
        try (Writer writer = new FileWriter(csvFile)) {
            org.apache.commons.csv.CSVFormat format = org.apache.commons.csv.CSVFormat.DEFAULT.withDelimiter(delimiter).withHeader(headers.toArray(new String[0]));
            try (org.apache.commons.csv.CSVPrinter printer = new org.apache.commons.csv.CSVPrinter(writer, format)) {
                for (Map<String, Object> row : data) {
                    List<String> values = new ArrayList<>();
                    for (String header : headers) {
                        Object val = row.get(header);
                        values.add(val == null ? "" : val.toString());
                    }
                    printer.printRecord(values);
                }
            }
        }
    }
}
