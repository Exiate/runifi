# RuniFi Architecture

## System Overview

```mermaid
graph TB
    subgraph USER["User Layer"]
        CLI["runifi-cli<br/><i>Management CLI</i>"]
        DASH["React Flow Dashboard<br/><i>Embedded SPA</i>"]
        TOML["flow.toml<br/><i>Flow Configuration</i>"]
    end

    subgraph API_LAYER["runifi-api · REST + SSE"]
        direction LR
        AX["Axum Router"]
        RL["Rate Limiter<br/><i>per-IP · governor</i>"]
        CORS["CORS"]
        SSE["SSE /events<br/><i>Real-time updates</i>"]
        REST["REST Endpoints<br/>/api/processors<br/>/api/connections<br/>/api/flow<br/>/api/plugins<br/>/api/bulletins"]
    end

    subgraph SERVER["runifi-server · Binary Entrypoint"]
        BOOT["Bootstrap<br/>1. Init tracing<br/>2. Discover plugins<br/>3. Load config<br/>4. Build engine<br/>5. Start API"]
    end

    subgraph CORE["runifi-core · Engine Runtime"]
        direction TB
        subgraph ENGINE["Engine"]
            FE["FlowEngine<br/><i>DAG Scheduler</i>"]
            EH["EngineHandle<br/><i>Async query interface</i>"]
            PN["ProcessorNode<br/><i>Per-processor task</i>"]
            SV["Supervisor<br/><i>catch_unwind<br/>circuit breaker<br/>exp. backoff</i>"]
            MET["Rolling Metrics<br/><i>5-min window</i>"]
            BB["BulletinBoard<br/><i>Alerts & warnings</i>"]
        end
        subgraph CONN["Connections"]
            FC["FlowConnection<br/><i>crossbeam channel<br/>~100ns latency</i>"]
            BP["BackPressure<br/><i>AtomicUsize count<br/>AtomicU64 bytes</i>"]
            SQ["Shadow Queue<br/><i>VecDeque snapshots<br/>for API inspection</i>"]
        end
        subgraph SESSION["Session"]
            CS["CoreProcessSession<br/><i>Transactional<br/>commit / rollback</i>"]
        end
        subgraph REPO["Repositories"]
            CR["ContentRepository<br/><i>DashMap · ref-counted<br/>zero-copy Bytes</i>"]
            PR["ProvenanceRepository<br/><i>Persistent · indexed search<br/>lineage · replay</i>"]
            FFR["FlowFileRepository<br/><i>WAL-based · encrypted</i>"]
        end
        subgraph REG["Registry"]
            PREG["PluginRegistry<br/><i>inventory crate<br/>compile-time discovery</i>"]
            SREG["ServiceRegistry<br/><i>Controller services</i>"]
        end
        subgraph AUTH["Auth & Security"]
            AUTHN["AuthN Providers<br/><i>OIDC · LDAP · mTLS<br/>API keys · local</i>"]
            RBAC["RBAC<br/><i>Role-based access</i>"]
            AUDIT["Audit Logger<br/><i>Structured events</i>"]
        end
        subgraph CLUSTER["Clustering"]
            COORD["Coordinator<br/><i>Leader election</i>"]
            GOSSIP["Gossip Protocol<br/><i>Node discovery</i>"]
            HB["Heartbeat<br/><i>Health monitoring</i>"]
        end
        subgraph EXTRA["Additional Subsystems"]
            EXPR["Expression Language<br/><i>${...} syntax</i>"]
            PG["Process Groups<br/><i>Nested flow hierarchy</i>"]
            VER["Flow Versioning<br/><i>Git-backed · diff</i>"]
            RT["Reporting Tasks<br/><i>Prometheus · logs</i>"]
        end
    end

    subgraph PROCESSORS["runifi-processors · Built-in Plugins (20+)"]
        direction LR
        subgraph DEBUG["debug"]
            GEN["GenerateFlowFile"]
            LOG["LogAttribute"]
        end
        subgraph FS["filesystem"]
            GF["GetFile"]
            PF["PutFile"]
        end
        subgraph ROUTE["routing"]
            ROA["RouteOnAttribute"]
            UA["UpdateAttribute"]
            FUN["Funnel"]
        end
        subgraph JSON["json"]
            SJ["SplitJSON"]
            EJP["EvaluateJsonPath"]
            VJ["ValidateJSON"]
            FJ["FlattenJSON"]
        end
        subgraph CONTENT["content"]
            SC["SplitContent"]
            ET["ExtractText"]
            PSL["ParseSyslog"]
        end
        subgraph RECORD["record"]
            CVR["ConvertRecord"]
            PTR["PartitionRecord"]
            UPR["UpdateRecord"]
        end
        subgraph S2S["site-to-site"]
            PUSH["PushFlowFile"]
            PULL["PullFlowFile"]
        end
    end

    subgraph PLUGIN_API["runifi-plugin-api · Stable Contract"]
        direction LR
        PT["Processor trait<br/><i>sync on_trigger()</i>"]
        FF["FlowFile<br/><i>u64 id · Vec attrs<br/>ContentClaim</i>"]
        PS["ProcessSession trait<br/><i>get · write · transfer<br/>commit · rollback</i>"]
        PD["ProcessorDescriptor<br/><i>inventory registration</i>"]
        REL["Relationship<br/>PropertyDescriptor"]
    end

    subgraph TRANSPORT["runifi-transport · Transfer Strategy"]
        direction LR
        IS["InlineStream<br/><i>≤ 64 KB · batched</i>"]
        CHS["ChunkedStream<br/><i>64 KB – 100 MB</i>"]
        ZC["ZeroCopy<br/><i>> 100 MB · mmap<br/>sendfile · io_uring</i>"]
    end

    %% Connections
    DASH --> AX
    CLI --> AX
    TOML --> BOOT
    BOOT --> FE
    BOOT --> AX
    AX --> RL --> REST
    AX --> SSE
    AX --> CORS
    REST --> EH
    SSE --> EH
    EH --> FE
    FE --> PN
    PN --> SV
    SV --> CS
    CS --> FC
    FC --> BP
    FC --> SQ
    CS --> CR
    PN --> MET
    SV --> BB
    FE --> PREG
    PREG --> PROCESSORS
    PROCESSORS --> PT
    PROCESSORS --> FF
    CS -.-> PS
    TRANSPORT -->|"inter-node transfers"| FC
    CLUSTER -.-> TRANSPORT

    %% Styling
    classDef api fill:#4a90d9,stroke:#2c5f8a,color:#fff
    classDef core fill:#5b8c5a,stroke:#3d6b3c,color:#fff
    classDef plugin fill:#d4a44c,stroke:#a67c2e,color:#fff
    classDef transport fill:#9b59b6,stroke:#7d3c98,color:#fff
    classDef user fill:#e67e73,stroke:#c0544a,color:#fff
    classDef server fill:#6c7a89,stroke:#4e5a65,color:#fff

    class AX,RL,CORS,SSE,REST api
    class FE,EH,PN,SV,MET,BB,FC,BP,SQ,CS,CR,PR,FFR,PREG,SREG,AUTHN,RBAC,AUDIT,COORD,GOSSIP,HB,EXPR,PG,VER,RT core
    class GEN,LOG,GF,PF,ROA,UA,FUN,SJ,EJP,VJ,FJ,SC,ET,PSL,CVR,PTR,UPR,PUSH,PULL,PT,FF,PS,PD,REL plugin
    class IS,CHS,ZC transport
    class CLI,DASH,TOML user
    class BOOT server
```

## Data Flow Pipeline

```mermaid
sequenceDiagram
    participant Config as flow.toml
    participant Engine as FlowEngine
    participant Node as ProcessorNode
    participant Sup as Supervisor
    participant Proc as Processor
    participant Sess as CoreProcessSession
    participant Conn as FlowConnection
    participant Repo as ContentRepository

    Config->>Engine: Load processors & connections
    Engine->>Engine: Build DAG topology
    Engine->>Node: Spawn tokio task per processor

    loop Processing Loop
        Conn-->>Node: Notify (data available)
        Node->>Sup: Execute on_trigger
        Sup->>Sup: spawn_blocking + catch_unwind
        Sup->>Proc: on_trigger(context, session)

        Proc->>Sess: session.get()
        Sess->>Conn: crossbeam::try_recv()
        Conn-->>Sess: FlowFile
        Sess-->>Proc: FlowFile

        Proc->>Repo: Read/write content
        Repo-->>Proc: Bytes (zero-copy)

        Proc->>Sess: session.transfer(ff, "success")
        Note over Sess: Buffered in pending_transfers

        Proc->>Sess: session.commit()
        Sess->>Conn: conn.try_send(flowfile)
        Note over Conn: Back-pressure check<br/>(atomic count + bytes)
        Conn->>Conn: Update shadow queue

        alt Failure
            Sup->>Sup: Increment failure count
            Sup->>Sup: Exponential backoff (100ms → 30s)
            Note over Sup: 5 failures → circuit breaker OPEN
        end
    end
```

## Crate Dependency Graph

```mermaid
graph BT
    PA["runifi-plugin-api<br/><i>Stable contract<br/>No async deps</i>"]
    CORE["runifi-core<br/><i>Engine runtime<br/>tokio · crossbeam · dashmap</i>"]
    PROC["runifi-processors<br/><i>Built-in plugins<br/>feature-gated</i>"]
    TRANS["runifi-transport<br/><i>QUIC · io_uring<br/>quinn · rustls</i>"]
    API["runifi-api<br/><i>REST + SSE<br/>axum · tower · governor</i>"]
    SRV["runifi-server<br/><i>Binary entrypoint</i>"]
    CLI["runifi-cli<br/><i>Management CLI</i>"]

    CORE --> PA
    PROC --> PA
    API --> CORE
    TRANS --> PA
    SRV --> CORE
    SRV --> PROC
    SRV --> API
    SRV --> TRANS
    CLI --> CORE

    style PA fill:#d4a44c,stroke:#a67c2e,color:#fff
    style CORE fill:#5b8c5a,stroke:#3d6b3c,color:#fff
    style PROC fill:#d4a44c,stroke:#a67c2e,color:#fff
    style TRANS fill:#9b59b6,stroke:#7d3c98,color:#fff
    style API fill:#4a90d9,stroke:#2c5f8a,color:#fff
    style SRV fill:#6c7a89,stroke:#4e5a65,color:#fff
    style CLI fill:#e67e73,stroke:#c0544a,color:#fff
```

## Transfer Strategy Selection

```mermaid
flowchart LR
    FF["FlowFile<br/>arrives"] --> SIZE{File Size?}
    SIZE -->|"≤ 64 KB"| INLINE["InlineStream<br/><i>Batched in QUIC frame<br/>Micro: 1.56M+/sec single-thread</i>"]
    SIZE -->|"64 KB – 100 MB"| CHUNK["ChunkedStream<br/><i>Streaming chunks<br/>Standard: ≥500 MB/s</i>"]
    SIZE -->|"> 100 MB"| ZERO["ZeroCopy<br/><i>mmap + sendfile/io_uring<br/>Bulk: ≥800 MB/s</i>"]

    style INLINE fill:#27ae60,stroke:#1e8449,color:#fff
    style CHUNK fill:#f39c12,stroke:#d68910,color:#fff
    style ZERO fill:#e74c3c,stroke:#c0392b,color:#fff
```

## Fault Tolerance Model

```mermaid
stateDiagram-v2
    [*] --> Running

    Running --> Success: on_trigger OK
    Success --> Running: next poll

    Running --> Failure: on_trigger panics/errors
    Failure --> Backoff: increment failure count

    Backoff --> Running: wait (100ms × 2^n, cap 30s)

    Failure --> CircuitOpen: 5 consecutive failures
    CircuitOpen --> [*]: processor disabled

    note right of Backoff
        Exponential backoff
        Base: 100ms
        Cap: 30s
    end note

    note right of CircuitOpen
        Auto-disabled
        Requires manual reset
    end note
```

## Engine Performance (Single-Thread Benchmarks)

These numbers reflect the overhead of RuniFi's core primitives, measured with `criterion` on a single thread:

| Operation | Time | Throughput |
|---|---|---|
| FlowFile creation (empty) | ~4 ns | 250M/sec |
| FlowFile creation (3 attributes) | ~20 ns | 50M/sec |
| ID generation (u64 atomic) | 1.65 ns | 606M/sec |
| Attribute lookup (16 attrs) | 5.35 ns | 187M/sec |
| FlowFile clone (8 attrs) | 63 ns | 15.8M/sec |
| Connection send+recv cycle | 48 ns | 20.8M/sec |
| Back-pressure check | 0.64 ns | 1.5B/sec |
| Content create 5KB | 232 ns | 4.3M/sec |
| Content read 5KB (zero-copy) | 28.7 ns | 34.8M/sec |
| Zero-copy slice 1KB from 1MB | 29 ns | 34.5M/sec |
| Full pipeline step (5KB create+write+read+transfer+commit) | 643 ns | 1.56M/sec |
| Pipeline with connection (end-to-end 5KB) | 7.49 µs | 133K/sec |
| Batch create 100 FlowFiles | 18.9 µs | 5.28M FF/sec |
| Session rollback with content | 435 ns | 2.3M/sec |

Run benchmarks: `cargo bench --package runifi-core`

## Subsystem Details

### Authentication & Authorization

RuniFi supports multiple authentication providers chained together:
- **OIDC** — OpenID Connect for SSO integration
- **LDAP** — directory-based authentication
- **mTLS** — mutual TLS client certificate authentication
- **API keys** — programmatic access
- **Local** — username/password with bcrypt hashing

Role-based access control (RBAC) governs all API operations.

### Clustering

Multi-node clustering with:
- **Leader election** — Raft-inspired coordinator selection
- **Gossip protocol** — decentralized node discovery and state propagation
- **Heartbeat** — health monitoring with configurable intervals
- **Dynamic membership** — nodes can join and leave without downtime
- **Load-balanced connections** — distribute FlowFiles across cluster nodes

### Data Provenance

Full lineage tracking for every FlowFile:
- **Persistent repository** — append-only file-backed storage
- **Indexed search** — query by FlowFile ID, processor, time range, or event type
- **Lineage graphs** — trace a FlowFile's full history through the flow
- **Replay** — re-queue FlowFiles from any provenance event

### Expression Language

NiFi-compatible `${...}` syntax for dynamic property values:
- Attribute references: `${filename}`, `${mime.type}`
- String functions: `${filename:toLower()}`, `${path:append('/output')}`
- Conditional logic: `${fileSize:gt(1024):ifElse('large','small')}`

### Flow Versioning

Git-backed version control for flow definitions:
- Automatic snapshots on flow changes
- Diff between versions
- Rollback to any previous version

### Process Groups

Hierarchical flow organization:
- Nested processor groups with independent scheduling
- Input/output ports for inter-group communication
- Breadcrumb navigation in the dashboard
