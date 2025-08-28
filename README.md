# ClickHouse–FlatFile Ingestion Tool

Web app to move data between CSV (Flat File) and ClickHouse (local or ClickHouse Cloud). Supports HTTPS and password/JWT authentication, schema discovery, column selection, preview, and clear status reporting.

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
2) Fill Target (as above), click Connect. You should see “ClickHouse target connected successfully”.
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

## AI Tool Usage
See `prompts.txt` for prompts used during AI‑assisted development.
