# 🚀 Real-Time Streaming Pipeline
### Kafka + Spark Structured Streaming + Delta Lake + PostgreSQL + Grafana

[![CI/CD](https://github.com/YOUR_USERNAME/kafka-stream-store/actions/workflows/ci-cd.yml/badge.svg)](https://github.com/YOUR_USERNAME/kafka-stream-store/actions)

A production-grade streaming pipeline that ingests mixed event streams (clickstream, IoT sensors, application logs), processes them through a 3-stage Kafka topology, transforms with Spark Structured Streaming, persists to Delta Lake + PostgreSQL, and visualises in real-time on Grafana.

---

## Architecture

```
┌─────────────┐    events.raw     ┌──────────────────────────────────────┐
│             │ ─────────────────▶│                                      │
│  Producer   │                   │     Spark Structured Streaming       │
│  10 ev/sec  │   events.cleaned  │                                      │
│  - clicks   │ ◀───────────────  │  Stage 1: Parse + validate JSON      │
│  - sensors  │                   │  Stage 2: Clean, dedupe, enrich      │
│  - logs     │   events.curated  │  Stage 3: Window aggregations        │
│             │ ◀───────────────  │                                      │
└─────────────┘                   └──────────┬─────────────┬─────────────┘
                                             │             │
                                     ┌───────▼──────┐ ┌───▼──────────┐
                                     │  Delta Lake  │ │  PostgreSQL  │
                                     │  (Parquet)   │ │  (live data) │
                                     └──────────────┘ └──────┬───────┘
                                                             │
                                                     ┌───────▼───────┐
                                                     │    Grafana    │
                                                     │  Dashboard    │
                                                     └───────────────┘
```

## Kafka Topic Design

| Topic | Purpose | Retention |
|-------|---------|-----------|
| `events.raw` | Raw JSON from producer | 24h |
| `events.cleaned` | Validated, deduplicated | 24h |
| `events.curated` | Aggregated metrics | 48h |
| `events.dlq` | Dead letter queue (parse failures) | 72h |

---

## Quick Start

### Prerequisites
- Docker + Docker Compose
- 8GB RAM minimum
- Ports free: 3000, 8080, 9090, 9092

### 1. Clone and start

```bash
git clone https://github.com/YOUR_USERNAME/kafka-stream-store
cd kafka-stream-store
docker compose up -d
```

### 2. Watch topics come alive

```bash
# Tail raw events
docker compose exec kafka \
  kafka-console-consumer \
    --bootstrap-server localhost:9092 \
    --topic events.raw \
    --from-beginning

# Check consumer lag
docker compose exec kafka \
  kafka-consumer-groups \
    --bootstrap-server localhost:9092 \
    --describe --all-groups
```

### 3. Open Grafana dashboard

Navigate to http://localhost:3000 → login `admin / admin123`
→ Dashboards → Streaming → **Real-Time Streaming Pipeline**

### 4. Explore Kafka UI

http://localhost:8080 – browse topics, consumer groups, message replay

---

## Services

| Service | URL | Credentials |
|---------|-----|-------------|
| Grafana | http://localhost:3000 | admin / admin123 |
| Kafka UI | http://localhost:8080 | — |
| Schema Registry | http://localhost:8081 | — |
| Prometheus | http://localhost:9090 | — |
| PostgreSQL | localhost:5432 | pipeline / pipeline123 |

---

## Project Structure

```
kafka-stream-store/
├── docker-compose.yml          # All services wired together
├── kafka/
│   ├── producer.py             # Event generator (clicks + sensors + logs)
│   └── Dockerfile.producer
├── spark/
│   ├── pipeline.py             # 3-stage Spark Structured Streaming job
│   └── Dockerfile
├── postgres/
│   └── init.sql                # Schema, indexes, Grafana-friendly views
├── grafana/
│   ├── prometheus.yml          # Scrape config
│   ├── provisioning/           # Auto-provision datasources + dashboards
│   └── dashboards/
│       └── pipeline.json       # Pre-built Grafana dashboard
├── tests/
│   └── test_pipeline.py        # Unit + smoke tests
└── .github/
    └── workflows/
        └── ci-cd.yml           # Full CI/CD pipeline
```

---

## 3-Day Build Plan

### Day 1 – Infrastructure & Ingestion
- [ ] Spin up Docker Compose: Zookeeper, Kafka, Schema Registry, Kafka UI
- [ ] Verify topics created: `events.raw`, `events.cleaned`, `events.curated`, `events.dlq`
- [ ] Run producer: `docker compose up producer`
- [ ] Confirm messages visible in Kafka UI at http://localhost:8080
- [ ] Start PostgreSQL, verify `init.sql` created all tables
- [ ] Push code to GitHub, confirm CI lint job passes

### Day 2 – Spark Processing
- [ ] Run Spark streaming job: `docker compose up spark-streaming`
- [ ] Verify events appearing in `events_cleaned` table:
  ```sql
  SELECT event_type, COUNT(*) FROM events_cleaned GROUP BY 1;
  ```
- [ ] Verify aggregations in `clickstream_metrics`, `sensor_metrics`, `log_metrics`
- [ ] Check Delta Lake files created at `/opt/delta-lake/events/`
- [ ] Confirm data is partitioned by `event_type`
- [ ] Run unit tests: `pytest tests/ -v`

### Day 3 – Dashboard & CI/CD
- [ ] Open Grafana → confirm dashboard auto-provisioned
- [ ] All 10 panels showing live data with `refresh=10s`
- [ ] Add GitHub Secrets: `STAGING_HOST`, `STAGING_SSH_KEY`, etc.
- [ ] Merge a PR → watch CI/CD pipeline run: lint → build → integration → deploy
- [ ] Screenshot or record dashboard for portfolio

---

## Key Queries for Understanding the Pipeline

```sql
-- Event rate per minute
SELECT date_trunc('minute', event_ts), event_type, COUNT(*)
FROM events_cleaned
WHERE event_ts > NOW() - INTERVAL '10 minutes'
GROUP BY 1, 2 ORDER BY 1;

-- Revenue by page (last hour)  
SELECT page, ROUND(SUM(revenue)::numeric, 2) AS revenue
FROM events_cleaned
WHERE event_type = 'clickstream' AND event_ts > NOW() - INTERVAL '1 hour'
GROUP BY 1 ORDER BY 2 DESC;

-- Sensor anomalies
SELECT * FROM v_sensor_anomalies LIMIT 20;

-- Consumer lag (run in Kafka container)
-- kafka-consumer-groups --bootstrap-server localhost:9092 --describe --all-groups
```

---

## Scaling Notes

- **More throughput**: Increase `EVENTS_PER_SECOND` env var on the producer
- **More partitions**: Edit `kafka-init` command in docker-compose.yml
- **Spark workers**: Add `spark-worker` services to docker-compose.yml
- **Production Kafka**: Replace with MSK (AWS) or Confluent Cloud

---

## CI/CD Pipeline Stages

```
Push to GitHub
     │
     ├── [test]       flake8 + black + isort + pytest --cov
     │
     ├── [build]      Docker build + push to GHCR (ghcr.io)
     │                (producer image + spark image)
     │
     ├── [integration] docker compose up → pytest tests/integration/
     │
     ├── [deploy-staging]  SSH deploy on push to develop
     │
     └── [deploy-prod]     SSH deploy on push to main (requires approval)
```

---
