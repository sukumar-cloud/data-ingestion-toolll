yes# ClickHouse-FlatFile Ingestion Tool

A web-based tool for bidirectional data ingestion between ClickHouse and Flat File (CSV), featuring JWT authentication, schema discovery, column selection, data preview, and robust error handling.

## Features
- Ingest data: ClickHouse → Flat File and Flat File → ClickHouse
- JWT-based authentication for ClickHouse
- Schema discovery and column selection
- Data preview (first 100 records)
- Status and record count reporting
- Simple UI built with AngularJS (1.x)
- Spring Boot backend (Java)

## Project Structure
- `backend/` - Spring Boot Java REST API
- `frontend/` - AngularJS 1.x app
L
## Setup

### Backend
1. Navigate to `backend/`
2. Build and run with Maven:
   ```sh
   mvn spring-boot:run
   ```
3. Default port: 8080

### Frontend
1. Navigate to `frontend/`
2. Serve with any static server (e.g., http-server, Python SimpleHTTPServer, or open `index.html` directly)

## Usage
- Open the frontend in your browser
- Configure source/target (ClickHouse or Flat File)
- Connect, preview data, select columns, and start ingestion
- View status and record counts

## Requirements
- Java 17+ (for backend)
- Node.js/npm (for frontend development, optional)
- ClickHouse instance (local or remote)

## AI Tool Usage
See `prompts.txt` for all prompts used in AI-assisted development.

