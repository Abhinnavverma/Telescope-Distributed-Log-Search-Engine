# 🔭 Telescope — Distributed Log Ingestion & Search Engine

[![Go](https://img.shields.io/badge/Go-High_Performance-00ADD8?style=flat&logo=go)](https://golang.org/)
[![Kafka](https://img.shields.io/badge/Kafka-Durable_Ingestion-231F20?style=flat&logo=apachekafka)](https://kafka.apache.org/)
[![gRPC](https://img.shields.io/badge/gRPC-Low_Latency-4285F4?style=flat&logo=grpc)](https://grpc.io/)
[![Distributed Systems](https://img.shields.io/badge/Architecture-Scatter_Gather-black?style=flat)](#)

**Telescope** is a **high-throughput, distributed log ingestion and search engine** built for large-scale microservice ecosystems.

It is designed to ingest **tens of thousands of log events per second**, index them efficiently, and support **low-latency search** across massive datasets using a **shard-based scatter–gather architecture**.

This is not a wrapper around existing observability tools — Telescope is a ground-up implementation focused on **explicit system design, failure handling, and scalability trade-offs**.

---

## ⚡ Core Capabilities

- **High-Frequency Ingestion**  
  Designed to sustain extreme write throughput without blocking producers.

- **Near Real-Time Search**  
  Logs become searchable shortly after ingestion with bounded delay.

- **Horizontally Scalable by Design**  
  Ingestion, storage, and query execution scale independently.

- **Failure-Isolated Architecture**  
  Partial failures degrade functionality gracefully — never catastrophically.

- **Production-Oriented Internals**  
  Explicit buffering, checkpointing, and recovery semantics.

---

## 🏗️ System Architecture

Telescope follows a **decoupled, distributed architecture** separating ingestion, storage, and query execution paths.


### Architectural Principles

- No synchronous writes on hot paths  
- No single point of failure  
- Stateless query orchestration  
- Shard-local ownership of data  
- Explicit backpressure handling  

---

## ✍️ Write Path (Ingestion)

The write path is engineered for **maximum throughput and durability**.

### Ingestion Flow

<img width="1548" height="768" alt="image" src="https://github.com/user-attachments/assets/4cb348f1-18db-4e8f-beec-166b6833633b" />


### Design Goals

- Absorb traffic spikes without collapsing downstream systems
- Decouple producers from storage latency
- Enable safe replay and recovery after crashes
- Maintain ordering guarantees where required

### Guarantees

- **At-least-once delivery**
- **Durable buffering**
- **Non-blocking producer writes**

---

## 🔍 Read Path (Search)

Search queries are executed using a **scatter–gather model**, enabling parallelism across shards.

### Query Flow

<img width="1769" height="711" alt="image" src="https://github.com/user-attachments/assets/c1bb2cc7-4b86-4d27-9e50-a8caeafcea55" />






### Execution Model

1. Query is received by an aggregator
2. Aggregator dispatches requests to relevant shards in parallel
3. Each shard executes the query locally
4. Results are merged, ranked, and returned

This avoids centralized bottlenecks and ensures predictable query latency as data volume grows.

---

## ⚖️ Consistency Model

Telescope is optimized for **observability workloads**, where availability and throughput outweigh strict consistency.

- **Writes:** At-least-once
- **Reads:** Eventually consistent
- **Search Visibility:** Bounded delay after ingestion

This mirrors real-world logging systems used in production environments.

---

## 📈 Scalability Model

Telescope scales **horizontally at every layer**.

- Increase ingestion throughput by adding partitions
- Increase storage & query capacity by adding shards
- Increase query QPS by scaling stateless aggregators

No re-architecture required as scale increases.

---

## 🧯 Fault Tolerance & Recovery

- Durable ingestion prevents data loss on crashes
- Shard failures isolate impact to a subset of data
- Consumers resume safely after restarts
- Query layer remains available during partial failures

Failure is treated as a **first-class design constraint**, not an edge case.

---

## 🛠️ Tech Stack

### Core Systems
- **Language:** Go
- **Ingestion:** Kafka
- **RPC:** gRPC
- **Storage:** Persistent shard-local storage
- **Search:** Custom indexing & query engine

### Infrastructure
- **Containerized:** Docker
- **Deployment-Ready:** Kubernetes-compatible
- **Observability:** Metrics & logging hooks built-in

---

## 📊 Current Status

- [x] Distributed ingestion pipeline implemented
- [ ] Shard-based storage architecture complete
- [ ] Scatter–gather query execution working
- [ ] Index compaction & optimization
- [ ] Query ranking and scoring
- [x] Load testing at scale
- [ ] Production hardening

---

## 🛣️ Roadmap

- [ ] Query caching layer
- [ ] Time-based retention policies
- [ ] Compression for cold data
- [ ] Advanced failure recovery
- [ ] Multi-tenant isolation
- [ ] Authentication & RBAC

---

## 🧠 Engineering Philosophy

Telescope is built with a **systems-engineering mindset**.

The goal is not feature count — it is:
- Understanding distributed system trade-offs
- Designing for failure and scale
- Making architecture decisions explicit
- Building primitives that behave predictably under load

This project reflects how real-world infrastructure is **designed, reasoned about, and evolved**.

---

## 📄 License

MIT
