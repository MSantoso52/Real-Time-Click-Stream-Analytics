# Real-Time Click-Stream Analytics
Real-time data streaming using Kafka, Clickhouse &amp; Grafana

## *Project Overview*
A self‑contained demo (Docker‑Compose) that ingests click events from Kafka into ClickHouse, stores them in a MergeTree table, and creates a materialized view that continuously rolls up unique active users per minute. The resulting metric is visualised in a live Grafana dashboard. The stack includes Kafka (or Redpanda) → ClickHouse Kafka engine → MergeTree storage → Materialized view → Grafana.

## *Problem To Be Solved*
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

## *Business Impact* 
  - Enables near real-time monitoring of user behavior.
  - Helps teams detect traffic spikes or product issues quickly.
  - Improves marketing campaign analysis by showing engagement as it happens.
  - Supports faster A/B testing decisions.
  - Reduces infrastructure complexity by using ClickHouse for both storage and aggregation.
  - Lowers latency from batch-style minutes or hours to seconds.
  - Gives product, marketing, and operations teams a single source of truth for user activity.

## *Business Leverage*
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

## Prerequisite
Need to be installed on your system (mine: *cachyos*):
- Docker & docker-compose installed for docker build
  ```bash
  sudo pacman -S docker docker-compose buildx

  docker --version
  Docker version 29.5.2, build 79eb04c7d8

  docker-compose --version
  Docker Compose version 5.1.4
  ```
- Neovim for code editor and enhance with lazyvim
  ```bash
  sudo pacman -S neovim ripgrep fd git curl
  
  git clone https://github.com/LazyVim/starter ~/.config/nvim

  nvim --version
  NVIM v0.12.3
  Build type: RelWithDebInfo
  LuaJIT 2.1.1780076327
  Run "nvim -V1 -v" for more info  
  ```
- git for versioning control
  ```bash
  sudo pacman -S git
  
  git --version
  git version 2.54.0
  ```
- Web browser (Brave)
  ```bash
  yay -S brave-bin

  brave --version
  Brave Browser 149.1.91.172
  ```
- tmux for terminal multiplexer - to make easy workspace because all done terminal
  ```bash
  sudo pacman -S tmux
  ``` 

## *Project Flow*

## *Screenshot*
![Grafana](Real-time streaming.png)
