# ClickHouse Data Ingestion Tool

## 🌐 Hosted Link

[https://data-ingestion-tool.netlify.app/](https://data-ingestion-tool.netlify.app/)

A powerful web application for seamless data transfer between CSV files and ClickHouse databases. This tool supports both local and cloud-based ClickHouse instances with secure authentication, schema discovery, and data preview capabilities.

---

## ✨ Features

* **CSV → ClickHouse** — primary flow using batch inserts with prepared statements.
* **ClickHouse → CSV** — preview and scaffolding for export.
* **Authentication** — password-based and optional JWT bearer token.
* **HTTPS** — auto-enabled for port 8443; JDBC adds `compress=0` to avoid LZ4 client issues over TLS.
* **Schema & Preview** — list tables and columns, preview first 100 rows.
* **Frontend** — AngularJS 1.x form UI with connection status, error display, and record counts.

---

## 🗂️ Project Structure

```
backend/  → Spring Boot 3.2 (Java 17) REST API
│  ├─ Controllers: ConnectionController, IngestionController, SchemaController, PreviewController, HealthController
│  ├─ Services: ClickHouseService, DataTransferService
│  └─ DTOs: ConnectionRequest
frontend/ → AngularJS 1.x SPA (index.html, app.js, style.css)
```

---

## ⚙️ Setup Instructions

### Backend

#### Requirements

* Java 17+
* Maven 3.9+

#### Run

```bash
cd backend
mvn -DskipTests spring-boot:run
```

Backend runs on: **[http://localhost:8080/api](http://localhost:8080/api)**

---

### Frontend

#### Run

```bash
cd frontend
npx http-server ./frontend -p 8000
```

Frontend runs on: **[http://localhost:8000](http://localhost:8000)** (expects backend on port 8080)

---

## ☁️ Using with ClickHouse Cloud

1. In your ClickHouse Cloud console, note:

   * Host: e.g. `xxxx.germanywestcentral.azure.clickhouse.cloud`
   * Port: `8443`
   * User: `default`
   * Password: your service password

2. Create a target table:

```sql
CREATE DATABASE IF NOT EXISTS default;
CREATE TABLE IF NOT EXISTS default.people (
  id UInt32,
  name String,
  age UInt8,
  email String
) ENGINE = MergeTree
ORDER BY id;
```

3. In the UI under **Target Configuration**:

   * Host: your cloud host (no `https://` prefix)
   * Port: `8443`
   * Use HTTPS: ✅ checked
   * Database: `default`
   * User: `default`
   * Password: your password
   * Target Table: `default.people`

---

## 🔄 Typical Flow (CSV → ClickHouse)

1. Upload CSV and choose delimiter.
2. Click **Preview** to check inferred schema.
3. Fill **Target Connection** details.
4. Click **Connect** — you should see “ClickHouse target connected successfully”.
5. Click **Start Ingestion**.
6. Verify data in ClickHouse Cloud:

```sql
SELECT count(*) FROM default.people;
SELECT * FROM default.people ORDER BY id;
```

---

## 💻 Requirements

* Java 17+
* Maven
* ClickHouse instance (local or Cloud)

---

## 🚀 Deploy to Render

### Prerequisites

* GitHub account
* Render account ([render.com](https://render.com))
* ClickHouse Cloud (or self-hosted)

### Steps

1. Fork this repository to your GitHub account.
2. In [Render Dashboard](https://dashboard.render.com/):

   * Click **New → Web Service**.
   * Connect GitHub and select your repo.
   * Runtime: **Docker**.
   * Branch: `main`.
   * Name: `clickhouse-ingestion-tool`.
3. Click **Create Web Service** and wait for deployment.
4. Add environment variables:

   ```
   SPRING_PROFILES_ACTIVE=prod
   ```
5. Access deployed frontend at your Render-provided URL. API is available at `/api`.

---

## 🧩 Local Development

### Prerequisites

* Java 17+
* Maven
* Docker & Docker Compose

### Run with Docker Compose

```bash
docker-compose up --build
```

Access:

* Frontend → [http://localhost](http://localhost:80)
* Backend → [http://localhost:8080/api](http://localhost:8080/api)

### Manual Run

```bash
# Backend
cd backend
mvn spring-boot:run

# Frontend
cd frontend
npx http-server -p 8000
```

App runs at: [http://localhost:8000](http://localhost:8000)

---

## ⚡ Performance Optimizations

### 1️⃣ Batch Insertions

* **Chunked Processing**: Processes rows in configurable batches (default 1,000) for efficient memory use.
* **Streaming Mode**: For large files (>10k rows), uses streaming to avoid full memory load.
* **Connection Pooling**: Uses **HikariCP** for efficient database connectivity.

### 2️⃣ Benchmark Results

| Rows | Mode      | Avg. Duration |
| ---- | --------- | ------------- |
| 1k   | Batch     | ~2–3s         |
| 10k  | Chunked   | ~5–10s        |
| 50k  | Streaming | ~15–25s       |

**Testing via Postman:**

```http
POST http://localhost:8080/api/transfer-csv-to-clickhouse
Content-Type: multipart/form-data

Form Data:
file: <your CSV file>
ingestionRequest: {"source":{"type":"file","delimiter":","},"target":{"type":"clickhouse","host":"localhost","port":8123,"database":"default","tableName":"test_table","user":"default","password":"","useHttps":false},"columns":[]}
```

**Verification:**
Use Postman timing or check inserted row counts in ClickHouse.

---

## 🧠 Performance Summary

| Operation        | Time (approx) | Notes                    |
| ---------------- | ------------- | ------------------------ |
| Upload + Preview | 1–2s          | Parses and infers schema |
| 10k Row Insert   | 5–10s         | Chunked mode             |
| 50k Row Insert   | 15–25s        | Streaming mode           |
| Memory Footprint | Low           | Constant memory usage    |

---

## 🛡️ Security

* Enable HTTPS (port 8443).
* Use JWT for multi-user environments.
* Restrict CORS to known origins.

---

## 🧰 Troubleshooting

| Issue                | Cause                           | Fix                       |
| -------------------- | ------------------------------- | ------------------------- |
| Connection refused   | Wrong port or missing TLS       | Check `useHttps` and host |
| CSV columns mismatch | Incorrect delimiter/header flag | Adjust settings and retry |
| CORS error           | Frontend origin not whitelisted | Update CORS config        |
| Timeout / OOM        | Too large file                  | Enable streaming mode     |

---

## 🧾 License

MIT

---

## 🛠️ Roadmap

* [ ] Upgrade frontend to **Angular 17**
* [ ] Add column mapping UI and dry-run validation
* [ ] JWT-based user authentication
* [ ] Add metrics + Prometheus support
* [ ] S3 ingestion (server-side, no browser upload)
