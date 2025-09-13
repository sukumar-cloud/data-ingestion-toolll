package com.cascade.assignment;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Main application class for the ClickHouse Data Ingestion Tool.
 * This application provides a web interface for transferring data between
 * CSV files and ClickHouse databases with support for various authentication methods.
 */
@SpringBootApplication
public class ClickHouseDataIngestionApp {

    public static void main(String[] args) {
        SpringApplication.run(ClickHouseDataIngestionApp.class, args);
    }
}
