# Pipeline Workflow

```mermaid
flowchart LR
    S0["Re-run at 1, 2, 4, 8, 16 executors"]

    subgraph DIST["Distributed pipeline — Apache Spark"]
        direction LR
        A1[Long documents] --> B1[Ingest as DataFrame]
        B1 -->|flatMap| C1[Chunk to fit context window]
        C1 -->|parallel LLM calls, retry on failure| D1[Executors summarize chunks]
        D1 -->|reduce| E1[Final summary - distributed]
    end

    subgraph BASE["Single-node baseline"]
        direction LR
        A2[Long documents] --> B2[Sequential chunk + summarize]
        B2 -->|merge| C2[Final summary - baseline]
    end

    S0 -.-> D1
    E1 --> EVAL[Evaluate: runtime, throughput, speedup, ROUGE-L]
    C2 --> EVAL
```

## Features

| Node      | Feature                              | Priority |
| --------- | ------------------------------------ | -------- |
| `A1`      | Load long-document dataset           | P0       |
| `B1`      | Store documents as Spark DataFrame   | P0       |
| `C1`      | Chunk documents with Spark `flatMap` | P0       |
| `D1`      | Parallel LLM summarization           | P0       |
| `D1`      | Retry failed LLM calls               | P0       |
| `E1`      | Merge chunk summaries                | P0       |
| `E1`      | Re-summarize oversized merged output | P0       |
| `A2`→`C2` | Single-node sequential baseline      | P0       |
| `S0`      | Test 1/2/4/8/16 executors            | P0       |
| `EVAL`    | Runtime and throughput               | P0       |
| `EVAL`    | Speedup vs. baseline                 | P0       |
| `EVAL`    | ROUGE-L quality                      | P0       |

## Optional Features

| Node   | Feature                     | Priority |
| ------ | --------------------------- | -------- |
| `C1`   | Semantic/paragraph chunking | P1       |
| `D1`   | Compare multiple LLMs       | P1       |
| `D1`   | GPU inference               | P2       |
| `D1`   | Worker fault tolerance      | P2       |
| `EVAL` | Results dashboard           | P2       |
| `A1`   | Streaming input             | P2       |
