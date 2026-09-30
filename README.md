# Career Events — Kafka Queue Layer

## Overview

This repository contains the Kafka queue/event-bus layer for the career-site event analytics pipeline.

```text
Event Generator → Kafka → Consumer → Database Service
```

My responsibility is the Kafka-based transport and processing boundary between the event generator and the database team.

```text
 Event Generator (teammate)                 YOUR PART                         DB Team
┌────────────────────┐   ┌──────────────────────────────────────────────┐   ┌──────────────┐
│ builds standard    │──▶│ EventProducer ──▶ career.events.raw (6 part.) │   │              │
│ JSON event         │   │    key = career_site_id:user_id               │   │              │
└────────────────────┘   │                      │                        │   │              │
                         │                      ▼                        │   │              │
                         │ EventConsumer (group career-db-writer)        │   │              │
                         │   validate → dedup → db.write_event(event) ───┼──▶│ Cassandra    │
                         │        ▲ ack ─────────────────────────────────┼───│ writer       │
                         │        └─ commit offset                       │   │              │
                         │   on failure: retry (exp. backoff) ×N         │   │              │
                         │        └─▶ career.events.dlq (+ commit)       │   │              │
                         └──────────────────────────────────────────────┘   └──────────────┘
```

Kafka acts as the durable event buffer between ingestion and database processing.

## What I Implemented

### Kafka Producer

`EventProducer` provides the interface used by the event-generator team. It validates the minimum event contract, creates the Kafka partition key, serializes the event as JSON, publishes to `career.events.raw`, adds `event_id`/`event_type` headers, and uses an idempotent producer with `acks=all`.

The generator can use:

```python
producer = EventProducer(load_config())
producer.publish(event)
producer.flush()
```

### Kafka Topic and Partitioning

Raw topic:

```text
career.events.raw
```

The topic has **6 partitions**. Kafka does not split a JSON event across partitions; each complete event is stored as one Kafka record in one partition.

The key is:

```text
career_site_id:user_id
```

Kafka hashes the key to choose a partition. Therefore events with the same key remain in the same partition and retain their relative order. Anonymous events fall back to `session_id`.

Example:

```text
site1:user1 → Partition 2
site1:user2 → Partition 5
site1:user3 → Partition 2
```

The six partitions also provide the basis for consumer parallelism.

### Consumer

`EventConsumer` uses the consumer group:

```text
career-db-writer
```

Processing flow:

```text
Kafka poll → validate → dedup → db.write_event(event) → DB ACK → commit offset
```

`enable.auto.commit=false` is used so the application commits only after successful processing.

If the DB write fails, the offset is not committed and the event is retried.

## Retries

Transient database failures are retried inline on the same partition rather than through a separate retry topic. This preserves partition ordering but means a stuck event can delay later events on that partition.

Backoff follows approximately:

```text
min(max_backoff, base × 2^n)
```

with jitter. Defaults are:

```text
Maximum retries : 5
Base backoff    : 0.5 seconds
Maximum backoff : 30 seconds
Jitter          : enabled
```

The configuration also checks that the retry/backoff budget does not exceed Kafka's `max.poll.interval.ms`.

## Dead Letter Queue

DLQ topic:

```text
career.events.dlq
```

An event is sent to the DLQ after retries are exhausted, for a permanent DB error, or when it is invalid/non-retryable.

A DLQ record contains:

```json
{
  "original_event": "...",
  "error": {"type": "...", "message": "..."},
  "retry_count": 3,
  "source": {"topic": "career.events.raw", "partition": 2, "offset": 123},
  "consumer_group": "career-db-writer",
  "failed_at": "..."
}
```

The original offset is committed only after the DLQ publish succeeds. If DLQ publishing fails, the offset is not committed, allowing redelivery.

## Idempotency

The design uses **at-least-once processing**. A crash can occur after the DB write succeeds but before the Kafka offset is committed, causing the same event to be delivered again.

Every event therefore needs a unique `event_id`.

The consumer has a bounded in-process cache of recent IDs as a best-effort optimization. Because that cache is lost on restart, the real idempotency guarantee must be implemented by the database writer using `event_id`.

## Database Interface

The Kafka layer is deliberately decoupled from Cassandra implementation details. The consumer calls:

```python
db.write_event(event)
```

Expected contract:

```text
successful write → return normally → commit offset
TransientDBError → retry
PermanentDBError → DLQ
```

The DB team implements the actual Cassandra persistence and durable idempotency.

## Demo and Testing

Run:

```bash
pip install -r requirements.txt
brew services start kafka
python -m pytest -q
python scripts/create_topics.py
python scripts/demo.py
python scripts/monitor.py --watch
```

The unit tests do not require Kafka. The end-to-end demo uses a real local Kafka broker, producer, consumer, topics, and failure/retry/DLQ paths, but uses an **in-memory database stand-in** rather than Cassandra. This validates the queue behavior independently of the final database integration.

The demo covers normal events, transient failures that eventually succeed, and an event that reaches the DLQ.

## Monitoring

`monitor.py` reports consumer lag per partition and DLQ depth.

```text
lag = Kafka end offset - consumer committed offset
```

For example, if events arrive faster than the DB can process them, Kafka absorbs the backlog and consumer lag grows. Scaling the consumer group and/or database capacity can then increase processing throughput.

## Failure Model

### Normal

```text
Producer → Kafka → Consumer → Cassandra ACK → Commit offset
```

### Transient DB failure

```text
Producer → Kafka → Consumer → DB failure → Backoff → Retry → DB ACK → Commit
```

### Permanent/dead event

```text
Producer → Kafka → Consumer → Failure → Retries exhausted → DLQ → DLQ ACK → Commit
```

### DLQ failure

```text
DB failure → Retries exhausted → DLQ publish fails → NO commit → redelivery
```

## Assumptions

1. The event generator produces events following the agreed standard schema.
2. Every event has a unique `event_id` generated by the event generator.
3. `career_site_id:user_id` is the desired ordering key; anonymous events use `session_id`.
4. Six partitions are sufficient for the initial design and determine the initial consumer parallelism.
5. The database writer is idempotent because processing is at-least-once.
6. Cassandra/database errors can be classified as transient or permanent.
7. Retries are inline to preserve partition ordering.
8. Kafka retains records according to topic retention, enabling buffering and replay of retained records.
9. The Kafka layer is responsible for transport, ordering, retries, DLQ handling, offset management, and delivery to the DB service—not business analytics.
10. The current local environment is a development setup; production requires appropriate Kafka replication and availability settings.

## Production Considerations

Before production deployment, address:

- Kafka replication factor and minimum in-sync replicas;
- multiple Kafka brokers and consumer instances;
- consumer-lag and DLQ alerts;
- Cassandra idempotency using `event_id`;
- hot partition/key analysis;
- capacity planning and partition count;
- event schema/version compatibility;
- Kafka authentication/authorization;
- DLQ replay procedures;
- production Cassandra write performance and scaling.

## Responsibility Boundary

### My responsibility

- Kafka producer
- topic configuration
- partitioning strategy
- Kafka consumer
- manual offset management
- retries and exponential backoff
- DLQ handling
- consumer-side duplicate detection
- Kafka monitoring
- queue-layer tests/demo

### Event generator team's responsibility

- generate real career-site events
- follow the agreed event schema
- generate unique `event_id`
- provide correct event metadata and payload

### Database team's responsibility

- Cassandra schema and connection/configuration
- `DatabaseWriter.write_event(event)`
- transient/permanent error classification
- durable idempotency using `event_id`
- Cassandra write performance and scaling

## Summary

> I implemented the Kafka event-queue layer that takes standardized career events from the generator, distributes them across six partitions using a user-based key, processes them through a consumer group, retries transient database failures, sends unrecoverable events to a DLQ, and commits Kafka offsets only after successful database or DLQ acknowledgement.
