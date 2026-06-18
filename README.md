# Real-Time-Click-Stream
Real-time data streaming using Kafka, Clickhouse &amp; Grafana

## Project Overview
A self‑contained demo (Docker‑Compose) that ingests click events from Kafka into ClickHouse, stores them in a MergeTree table, and creates a materialized view that continuously rolls up unique active users per minute. The resulting metric is visualised in a live Grafana dashboard. The stack includes Kafka (or Redpanda) → ClickHouse Kafka engine → MergeTree storage → Materialized view → Grafana.

## Problem To Be Solved
- Websites and mobile apps generate huge volumes of click-stream data every second.
  - Batch analytics often introduces delay, making insights too late for real-time decisions.
  - Product and marketing teams need immediate visibility into:
      - Traffic spikes
    - User engagement
    - Campaign performance
    - Funnel drop-offs
    - UX or checkout issues
  - Traditional systems may struggle to ingest, store, and query high-volume event data at low latency.
  - The challenge is to build a pipeline that can handle high throughput while still supporting fast analytical queries.

## Business Impact 
 - Enables near real-time monitoring of user behavior.
  - Helps teams detect traffic spikes or product issues quickly.
  - Improves marketing campaign analysis by showing engagement as it happens.
  - Supports faster A/B testing decisions.
  - Reduces infrastructure complexity by using ClickHouse for both storage and aggregation.
  - Lowers latency from batch-style minutes or hours to seconds.
  - Gives product, marketing, and operations teams a single source of truth for user activity.

## Business Leverage
- Demonstrates strong data engineering skills across the full pipeline:
      - Streaming ingestion
    - Schema design
    - Data modeling
    - Materialized views
    - Performance tuning
    - Dashboarding
    - Monitoring
  - ClickHouse provides major leverage because it can:
      - Ingest large volumes of data quickly
    - Store events efficiently
    - Run fast analytical queries
    - Maintain aggregated tables automatically
  - The same architecture can be reused for:
      - Ad-tech analytics
    - IoT event monitoring
    - Log analytics
    - Product analytics
    - Fraud detection dashboards
  - The project is highly portfolio-friendly because it shows both:
      - Technical depth
    - Clear business value

## Prerequisite

## Project Flow
