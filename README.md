# ClickHouse Data Ingestion Tool
## Hosted Link : https://data-ingestion-tool.netlify.app/
A powerful web application for seamless data transfer between CSV files and ClickHouse databases. This tool supports both local and cloud-based ClickHouse instances with secure authentication, schema discovery, and data preview capabilities.

## Features
* __CSV → ClickHouse__ (primary flow). Batch insert with prepared statements.
* __ClickHouse → CSV__ scaffolding and preview paths.
* __Auth__: Password and optional JWT bearer token.
* __HTTPS__: Auto-enabled for port 8443. JDBC URL adds `compress=0` to avoid LZ4 client issues over TLS.
* __Schema & Preview__: List tables/columns, preview first 100 rows.
* __UI__: AngularJS 1.x form with status, errors, and record counts.

## Project Structure
* `backend/` – Spring Boot 3.2 (Java 17) REST API
  * Controllers: `ConnectionController`, `IngestionController`, `SchemaController`, `PreviewController`, `HealthController`
  * Services: `ClickHouseService`, `DataTransferService`
  * DTOs: `ConnectionRequest`
* `frontend/` – AngularJS 1.x single page (`index.html`, `app.js`, `style.css`)

## Setup

### Backend
1) Requirements: Java 17+, Maven.
2) From `backend/`:
```sh
mvn -DskipTests spring-boot:run
```
Default URL: `http://localhost:8080` (APIs under `/api`).

### Frontend
1) Open `frontend/index.html` directly or serve the folder on port 8000:
```sh
npx http-server ./frontend -p 8000
```
App URL: `http://localhost:8000` (expects backend on 8080).

## Using with ClickHouse Cloud
1) From the Cloud console, get:
   * Host: e.g., `xxxx.germanywestcentral.azure.clickhouse.cloud`
   * Port: `8443`
   * User: `default`
   * Password: your service password
2) Create target table (example for `test_data.csv`):
```sql
CREATE DATABASE IF NOT EXISTS default;
CREATE TABLE IF NOT EXISTS default.people (
  id UInt32,
  name String,
  age UInt8,
  email String
) ENGINE=MergeTree ORDER BY id;
```
3) In the UI (Target Configuration):
   * Host: cloud host (no https)
   * Port: `8443`
   * Use HTTPS: checked
   * Database: `default`
   * User: `default`
   * Password: your password
   * Target Table: `default.people`

## Typical Flow (CSV → ClickHouse)
1) Choose CSV and delimiter, click Preview to confirm columns.
2) Fill Target (as above), click Connect. You should see "ClickHouse target connected successfully".
3) Click Start Ingestion. On success, status shows records processed.
4) Verify in Cloud console:
```sql
SELECT count(*) FROM default.people;
SELECT * FROM default.people ORDER BY id;
```

## Requirements
* Java 17+
* Maven
* A ClickHouse instance (local or Cloud)

## Deploy to Render

1. **Prerequisites**:
   - A GitHub account
   - A Render account (sign up at [render.com](https://render.com))
   - A ClickHouse Cloud account (or self-hosted ClickHouse)

2. **Deploy to Render**:
   - Fork this repository to your GitHub account
   - Go to [Render Dashboard](https://dashboard.render.com/)
   - Click "New" and select "Web Service"
   - Connect your GitHub account and select the forked repository
   - Configure the service:
     - Name: `clickhouse-ingestion-tool`
     - Region: Choose the one closest to you
     - Branch: `main`
     - Runtime: Docker
   - Click "Create Web Service"
   - Wait for the deployment to complete
   - Note the URL of your deployed application

3. **Configure Environment Variables**:
   - In the Render dashboard, go to your service
   - Click on "Environment" tab
   - Add the following environment variables:
     - `SPRING_PROFILES_ACTIVE=prod`
     - (Add any other environment variables your app needs)
   - Save the changes

4. **Access Your Application**:
   - Once deployed, you can access your application at the URL provided by Render
   - The frontend will be served from the root URL
   - The backend API will be available at `/api`

## Local Development

1. **Prerequisites**:
   - Java 17+
   - Maven
   - Docker and Docker Compose

2. **Run with Docker Compose**:
   ```bash
   docker-compose up --build
   ```
   - Frontend: http://localhost:80
   - Backend: http://localhost:8080/api

3. **Run Locally (Development)**:
   - Backend:
     ```bash
     cd backend
     mvn spring-boot:run
     ```
   - Frontend:
     ```bash
     cd frontend
     npx http-server -p 8000
     ```
   - Access the app at http://localhost:8000

## Performance Optimizations

### Batch Insertions
The application uses chunked batch insertions to optimize performance for large CSV files (10k+ rows). Here's how it works:

1. **Chunked Processing**: Data is processed in configurable chunks (default 1000 rows) to manage memory efficiently.
2. **Streaming**: For files larger than 10,000 rows, the application uses streaming insertion, processing row-by-row without loading the entire file into memory.
3. **Connection Pooling**: Uses HikariCP for efficient database connections.

### Testing Performance
I tested the performance using Postman to measure response times for different file sizes:

- **Small Files (< 1000 rows)**: Used standard batch insertion
- **Medium Files (1000-10000 rows)**: Used chunked batch processing
- **Large Files (> 10000 rows)**: Used streaming insertion

To test:
1. Create a CSV file with sample data
2. Use Postman to upload the file via `/api/transfer-csv-to-clickhouse`
3. Measure the response time using Postman's timing feature
4. Verify the number of records transferred in the response

Example Postman Request:
```
POST http://localhost:8080/api/transfer-csv-to-clickhouse
Content-Type: multipart/form-data

Form Data:
- file: [select your CSV file]
- ingestionRequest: {"source":{"type":"file","delimiter":","},"target":{"type":"clickhouse","host":"localhost","port":8123,"database":"default","tableName":"test_table","user":"default","password":"","useHttps":false},"columns":[]}
```

Performance Results:
- 1k rows: ~2-3 seconds
- 10k rows: ~5-10 seconds (chunked)
- 50k rows: ~15-25 seconds (streaming)

The streaming approach significantly reduces memory usage and improves performance for large files.
