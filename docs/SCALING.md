# Scaling the CDC Pipeline - Architecture & Trade-offs

## 📈 Current Architecture (Baseline)

```
┌──────────────────┐
│   PostgreSQL     │ (Source DB with WAL = logical)
│   Source DB      │
└────────┬─────────┘
         │ CDC via Logical Replication
         ▼
┌──────────────────┐
│    Debezium      │ (CDC Connector)
│    Connect       │
└────────┬─────────┘
         │ Change Events (JSON)
         ▼
┌──────────────────┐
│   Apache Kafka   │ (Event Streaming)
│   (3 Topics)     │
└────────┬─────────┘
         │ Streaming Data
         ▼
┌──────────────────┐
│  Apache Spark    │ (Transform & Enrich)
│  Streaming       │
└────────┬─────────┘
         │ Analytics-Ready Data
         ▼
┌──────────────────┐
│  Data Warehouse  │ (BigQuery/Snowflake)
│  (Star Schema)   │
└──────────────────┘
```

## 🚀 Scaling to 10x Volume

### Current Baseline Assumptions:
- **Data Volume**: ~1,000 events/second
- **Latency**: < 5 seconds end-to-end
- **Data Size**: 100GB/day

### 10x Scaled Requirements:
- **Data Volume**: ~10,000 events/second
- **Latency**: < 5 seconds (same SLA)
- **Data Size**: 1TB/day

---

## 🔍 Component-by-Component Scaling Analysis

### 1. PostgreSQL Source (❌ **WILL BREAK FIRST**)

#### What Will Break:
- **WAL Generation Rate**: Logical replication WAL files grow rapidly
  - At 10x volume: ~10-20 GB/hour of WAL
  - Disk I/O becomes bottleneck
  - Replication lag increases
  
- **CPU on Logical Decoding**: Converting WAL to logical events is CPU-intensive
  - Single replication slot = single thread bottleneck
  
- **Connection Limits**: Max connections exhausted by multiple consumers

#### Solutions:
```
Option 1: Read Replicas + Debezium Federation
┌──────────┐     ┌──────────┐     ┌──────────┐
│ Primary  │────▶│ Replica1 │────▶│Debezium1 │
│    DB    │     └──────────┘     └──────────┘
│          │     ┌──────────┐     ┌──────────┐
└──────────┘────▶│ Replica2 │────▶│Debezium2 │
                 └──────────┘     └──────────┘

- Use table-level sharding across replicas
- Each Debezium connector handles subset of tables
- Reduces load per instance
```

```
Option 2: Database Sharding
┌──────────┐     ┌──────────┐
│  Shard 1 │────▶│Debezium1 │
│ (users)  │     └──────────┘
└──────────┘     
┌──────────┐     ┌──────────┐
│  Shard 2 │────▶│Debezium2 │
│ (orders) │     └──────────┘
└──────────┘

- Partition tables by user_id or date range
- Each shard has its own CDC pipeline
- Application-level routing
```

**Trade-offs:**
- ✅ Better scalability
- ❌ Increased operational complexity
- ❌ Cross-shard joins become difficult
- ❌ Need distributed transaction handling

**Cost**: $$$$ (Multiple database instances)

---

### 2. Debezium Connect (⚠️ **WILL BREAK SECOND**)

#### What Will Break:
- **Single Connector Throughput**: Each connector task processes sequentially
- **Memory Pressure**: Event buffers grow with increased throughput
- **Schema Registry Calls**: Schema validation becomes bottleneck

#### Solutions:
```
Debezium Connect Cluster (Distributed Mode):

┌────────────────────────────────────┐
│      Debezium Connect Cluster      │
├────────────┬────────────┬──────────┤
│  Worker 1  │  Worker 2  │Worker 3  │
│  Task 1-3  │  Task 4-6  │Task 7-9  │
└────────────┴────────────┴──────────┘
         │         │          │
         └─────────┴──────────┘
                   │
              ┌────▼────┐
              │  Kafka  │
              └─────────┘
```

**Configuration:**
```properties
# Increase parallelism
tasks.max=12  # Up from 1

# Optimize buffer sizes
max.batch.size=10000
max.queue.size=100000

# Reduce schema lookups
key.converter.schemas.enable=false
value.converter.schemas.enable=false
```

**Trade-offs:**
- ✅ Horizontal scalability
- ✅ Fault tolerance (worker failures)
- ❌ Increased memory requirements
- ❌ More complex deployment

**Cost**: $$$ (3-5 worker nodes)

---

### 3. Apache Kafka (✅ **CAN SCALE WELL**)

#### What Will Break (Eventually):
- **Broker Disk I/O**: Log segments fill up quickly
- **Network Bandwidth**: Inter-broker replication saturates network
- **Consumer Lag**: Consumers can't keep up

#### Solutions:
```
Kafka Cluster Expansion:

Current: 1 broker, 3 partitions/topic
Scaled:  5 brokers, 20 partitions/topic

Topic: cdc.postgres_source.public.users
  Partition 0 ─▶ Broker 1, 2 (replication)
  Partition 1 ─▶ Broker 2, 3
  ...
  Partition 19 ─▶ Broker 5, 1
```

**Configuration:**
```properties
# Increase partitions for parallelism
num.partitions=20  # Up from 3

# Optimize retention
log.retention.hours=24  # Short retention
log.segment.bytes=1073741824  # 1GB segments
log.cleanup.policy=delete

# Compression
compression.type=snappy  # Reduce storage/network

# Replication
replication.factor=3  # High availability
min.insync.replicas=2
```

**Trade-offs:**
- ✅ Excellent horizontal scalability
- ✅ Built-in fault tolerance
- ✅ High throughput (millions of events/sec)
- ❌ Increased operational overhead
- ❌ More expensive storage

**Cost**: $$$ (5-node cluster + SSD storage)

---

### 4. Apache Spark Streaming (⚠️ **MODERATE SCALING**)

#### What Will Break:
- **Executor Memory**: Large state stores (windowed aggregations)
- **Checkpoint Size**: State metadata grows over time
- **Job Scheduling**: Single driver becomes bottleneck

#### Solutions:
```
Spark Cluster (Standalone or Kubernetes):

┌──────────────────────────────────────┐
│         Spark Master (Driver)        │
└──────────────┬───────────────────────┘
               │
      ┌────────┴─────────┬──────────┐
      │                  │          │
┌─────▼─────┐  ┌────────▼──┐  ┌────▼────┐
│ Executor 1│  │Executor 2 │  │Executor3│
│ 8GB RAM   │  │ 8GB RAM   │  │ 8GB RAM │
│ 4 cores   │  │ 4 cores   │  │ 4 cores │
└───────────┘  └───────────┘  └─────────┘
```

**Configuration:**
```python
spark = SparkSession.builder \
    .config("spark.executor.instances", "10")  # Up from 1
    .config("spark.executor.memory", "8g")
    .config("spark.executor.cores", "4")
    .config("spark.default.parallelism", "200")
    .config("spark.sql.shuffle.partitions", "200")
    .config("spark.streaming.kafka.maxRatePerPartition", "10000")
    .config("spark.sql.streaming.stateStore.stateSchemaCheck", "false")
    .getOrCreate()
```

**Optimizations:**
1. **Microbatching**: Process in 5-10 second batches
2. **Watermarking**: Limit state store size for windowed operations
3. **Checkpointing**: Use S3/HDFS for distributed checkpoint storage
4. **Schema Evolution**: Use schema registry for graceful upgrades

**Trade-offs:**
- ✅ Good horizontal scalability
- ✅ Fault-tolerant state management
- ❌ Higher latency (micro-batching vs true streaming)
- ❌ Complex tuning required
- ❌ Driver can be single point of failure

**Cost**: $$$$ (10-node cluster)

---

### 5. Data Warehouse Sink (✅ **SCALES WELL**)

#### BigQuery:
- **Native Scaling**: Auto-scales to petabytes
- **Streaming Inserts**: 100,000 rows/second
- **Cost**: Pay per TB scanned/stored

#### Snowflake:
- **Virtual Warehouses**: Scale up/out independently
- **Auto-suspend**: Save costs during idle periods
- **Cost**: Pay per compute-second + storage

**Loading Strategies:**

```
Option 1: Streaming (Low Latency)
Spark ──streaming──▶ BigQuery Streaming API
Latency: Seconds
Cost: $$$$

Option 2: Micro-Batch (Balanced)
Spark ──5 min batch──▶ Cloud Storage ──▶ BigQuery Load
Latency: 5-10 minutes
Cost: $$$

Option 3: Batch (Cost-Optimized)
Spark ──hourly──▶ Parquet Files ──▶ BigQuery Load
Latency: 1 hour
Cost: $$
```

---

## 💥 What Breaks First: Priority Order

### 1. **PostgreSQL Source** (❌ CRITICAL BOTTLENECK)
**Why**: Single-threaded logical decoding + WAL I/O

**Symptoms**:
- Replication lag > 10 seconds
- High CPU on WAL sender process
- Disk I/O saturation

**Fix**: Implement read replicas + table sharding

---

### 2. **Debezium Single Task** (❌ SECONDARY BOTTLENECK)
**Why**: Sequential event processing

**Symptoms**:
- Connector task backlog growing
- Consumer lag increasing
- OutOfMemoryError

**Fix**: Increase `tasks.max`, deploy distributed mode

---

### 3. **Kafka Broker Disk I/O** (⚠️ MANAGEABLE)
**Why**: High write throughput

**Symptoms**:
- Disk queue depth > 100
- Producer timeouts
- High replication lag between brokers

**Fix**: Add brokers, enable compression, tune retention

---

### 4. **Spark Executor Memory** (⚠️ TUNABLE)
**Why**: Stateful aggregations grow

**Symptoms**:
- OutOfMemoryError
- Task failures
- Long GC pauses

**Fix**: Increase executors, optimize watermarking

---

### 5. **Network Bandwidth** (⚠️ INFRASTRUCTURE)
**Why**: High data volume between services

**Symptoms**:
- Network saturation
- Connection timeouts
- Packet loss

**Fix**: Upgrade network infrastructure, co-locate services

---

## 📊 Scaling Cost Breakdown (10x)

| Component | Current | 10x Scaled | Monthly Cost |
|-----------|---------|------------|--------------|
| PostgreSQL | 1 instance | 3 shards + replicas | $2,000 |
| Debezium | 1 container | 3-5 workers | $500 |
| Kafka | 1 broker | 5-node cluster | $3,000 |
| Spark | Local | 10-node cluster | $5,000 |
| BigQuery | 100GB/day | 1TB/day | $6,000 |
| **Total** | **~$500** | **~$16,500** | **33x cost** |

---

## 🛡️ Handling Edge Cases

### 1. Malformed Input
```python
# In Spark processor
parsed_df = raw_df \
    .withColumn("parsed", from_json(col("value"), schema)) \
    .filter(col("parsed").isNotNull())  # Drop malformed
    
# Log malformed records to dead-letter queue
malformed_df = raw_df \
    .filter(col("parsed").isNull()) \
    .write.format("kafka") \
    .option("topic", "dead-letter-queue") \
    .save()
```

### 2. Schema Evolution
```python
# Use schema registry with backwards compatibility
schema_registry_url = "http://schema-registry:8081"

# Read with schema evolution handling
df = spark.readStream \
    .format("kafka") \
    .option("kafka.schema.registry.url", schema_registry_url) \
    .option("kafka.value.deserializer.schema.version", "latest") \
    .load()

# Handle missing fields gracefully
df = df.withColumn("new_field", 
                   when(col("new_field").isNotNull(), col("new_field"))
                   .otherwise(lit("default_value")))
```

### 3. Duplicate Events
```python
# Debezium provides transaction IDs for idempotency
df = df.dropDuplicates(["event_key", "transaction_id"])
```

### 4. Out-of-Order Events
```python
# Use watermarking in Spark
df.withWatermark("event_timestamp", "10 minutes") \
    .groupBy(window("event_timestamp", "5 minutes")) \
    .agg(...)
```

---

## 🎯 Recommended Scaling Path

### Phase 1: 2-3x Growth
- ✅ Increase Kafka partitions (3 → 10)
- ✅ Scale Spark executors (1 → 3)
- ✅ Add Debezium tasks (1 → 3)
- **Cost**: +100%

### Phase 2: 5-7x Growth
- ✅ Deploy Kafka cluster (3 brokers)
- ✅ Distributed Debezium (3 workers)
- ✅ Spark cluster (5 nodes)
- ✅ Add PostgreSQL read replicas
- **Cost**: +500%

### Phase 3: 10x+ Growth
- ✅ Shard PostgreSQL
- ✅ Kafka cluster (5+ brokers)
- ✅ Spark cluster (10+ nodes)
- ✅ Multi-region deployment
- **Cost**: +3300%

---

## 📝 Key Takeaways

1. **PostgreSQL is the weakest link** - Plan for sharding early
2. **Kafka scales amazingly** - Partitioning is your friend
3. **Debezium needs love** - Distributed mode essential at scale
4. **Spark is tunable** - Requires expertise to optimize
5. **Warehouses scale** - BigQuery/Snowflake handle petabytes
6. **Cost grows non-linearly** - 10x data ≠ 10x cost

**The Bottom Line**: With proper architecture, this pipeline can scale to **100,000+ events/second**, but requires careful planning and significant investment.
