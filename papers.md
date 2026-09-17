# Papers and Research Directions for Topic 8

## Scope and selection rule

These 10 papers support a project on **Apache Spark and LLM-based distributed long-document summarization**. Each citation below points to a final peer-reviewed ACM or IEEE publisher record through its DOI. Preprints and archive-only papers are intentionally excluded.

The literature falls into three connected areas:

1. Spark execution and structured data processing.
2. Hierarchical long-document summarization and summary evaluation.
3. Distributed, memory-efficient, and long-context LLM serving.

No single paper covers all three. That is the main opportunity for this project.

## Ten relevant papers

| # | Paper and venue | Relevance to this project | Idea to reuse and gap to investigate |
| --- | --- | --- | --- |
| 1 | Zaharia et al., **“Apache Spark: A Unified Engine for Big Data Processing,”** *Communications of the ACM*, 2016. [ACM DOI](https://doi.org/10.1145/2934664) | Establishes Spark's cluster model, in-memory processing, fault recovery, and unified APIs. | Use Spark for document/chunk data parallelism. Measure where Spark scheduling and serialization overhead outweigh useful LLM work; the paper predates LLM inference workloads. |
| 2 | Armbrust et al., **“Spark SQL: Relational Data Processing in Spark,”** *ACM SIGMOD*, 2015. [ACM DOI](https://doi.org/10.1145/2723372.2742797) | Motivates DataFrames, query optimization, and integration of relational and procedural processing. | Represent document ID, chunk order, source offsets, token counts, retries, and timings as typed columns. Compare a DataFrame/`mapPartitions` implementation with a naive per-row UDF design. |
| 3 | Huang et al., **“An Extraction-Abstraction Hybrid Approach for Long Document Summarization,”** *IEEE BESC*, 2019. [IEEE DOI](https://doi.org/10.1109/BESC48373.2019.8962979) | Directly studies long-document summarization using a hybrid pipeline rather than a single unconstrained pass. | Add a cheap extractive filtering stage before LLM abstraction. Test whether it reduces tokens and runtime without losing section coverage; update the approach with modern instruction-tuned LLMs. |
| 4 | Su, Wu, and Cheng, **“A Two-Stage Transformer-Based Approach for Variable-Length Abstractive Summarization,”** *IEEE/ACM TASLP*, 2020. [IEEE DOI](https://doi.org/10.1109/TASLP.2020.3006731) | Uses segmentation followed by segment- and document-level summarization, closely matching a hierarchical map/reduce design. | Compare fixed-token chunks with section/semantic segments and flat concatenation with recursive reduction. The open gap is how those choices behave under distributed execution, skew, retries, and nondeterministic completion order. |
| 5 | Lee et al., **“Towards Dataset-Scale and Feature-Oriented Evaluation of Text Summarization in Large Language Model Prompts,”** *IEEE TVCG*, 2025. [IEEE DOI](https://doi.org/10.1109/TVCG.2024.3456398) | Shows that prompt evaluation at dataset scale should examine characteristics beyond one aggregate overlap metric. | Evaluate formality, complexity, naturalness, factuality, and coverage in addition to ROUGE. Test whether the fastest distributed configuration changes these features or increases run-to-run variance. |
| 6 | Kwon et al., **“Efficient Memory Management for Large Language Model Serving with PagedAttention,”** *ACM SOSP*, 2023. [ACM DOI](https://doi.org/10.1145/3600006.3613165) | Explains KV-cache memory fragmentation and continuous batching in high-throughput LLM serving. | Use vLLM or a comparable batched server behind Spark and separate orchestration gains from serving-engine gains. Study batch size and chunk-length bucketing instead of attributing all speedup to executor count. |
| 7 | Aminabadi et al., **“DeepSpeed-Inference: Enabling Efficient Inference of Transformer Models at Unprecedented Scale,”** *IEEE SC*, 2022. [IEEE DOI](https://doi.org/10.1109/SC41404.2022.00051) | Covers multi-GPU and heterogeneous CPU/GPU/NVMe inference for large transformer models. | Clearly distinguish Spark's inter-request data parallelism from inference-engine model parallelism. If hardware permits, compare adding Spark executors with adding GPUs inside one model server. |
| 8 | Patel et al., **“Splitwise: Efficient Generative LLM Inference Using Phase Splitting,”** *ACM/IEEE ISCA*, 2024. [IEEE DOI](https://doi.org/10.1109/ISCA59077.2024.00019) | Separates the compute-heavy prefill phase from the memory-bandwidth-heavy decode phase. Long-document summaries have large prefills and relatively short outputs. | Instrument prefill and decode separately. Test length-aware routing or separate worker pools for long input chunks and final reduction requests instead of treating every LLM call as identical. |
| 9 | Gao et al., **“Online Context Caching for Distributed Large Language Models Serving,”** *IEEE INFOCOM*, 2025. [IEEE DOI](https://doi.org/10.1109/INFOCOM55648.2025.11044599) | Jointly studies cache placement and request scheduling in distributed LLM serving. | Measure reuse of the shared system prompt, document boilerplate, or overlapping chunk context. Investigate cache-aware partitioning while guarding against load imbalance and cross-document data leakage. |
| 10 | Li et al., **“Tetris: Efficient Long-Context LLM Serving with Chunkwise Dynamic Sequence Parallelism,”** *ACM/IEEE ISCA*, 2026. [IEEE DOI](https://doi.org/10.1109/ISCA66397.2026.00098) | Directly addresses varying-length long-context requests using chunkwise dynamic sequence parallelism and disaggregated serving. | Use it as the strongest systems comparison: Spark parallelizes independent summarization tasks, while Tetris parallelizes long sequences inside the serving layer. Quantify when each layer is the bottleneck and whether the two approaches complement one another. |

## Research gap

The summarization papers focus on how to segment and combine text, but do not study Spark-scale execution, cluster failures, skewed document lengths, or modern LLM-serving behavior. The serving papers optimize tokens, KV-cache memory, batching, and GPU scheduling, but generally evaluate generic request traces rather than hierarchical long-document summarization. The Spark papers provide general dataflow and fault-tolerance mechanisms, but predate expensive, rate-limited, and sometimes nondeterministic LLM calls.

The project can bridge these areas by answering:

> How should long documents be chunked, scheduled, retried, and hierarchically reduced across Spark executors to maximize throughput without sacrificing coverage or factual consistency?

This is more substantial than “call an LLM from Spark” because it requires a controlled systems evaluation across executor scaling, workload skew, serving behavior, failures, and output quality.

## Recommended project design

### Core experiment

Build one sequential baseline and two distributed variants:

1. **Fixed-token Spark pipeline:** equal-size token chunks, parallel chunk summaries, and deterministic hierarchical reduction.
2. **Structure-aware Spark pipeline:** section/paragraph boundaries, token-length bucketing, batched inference, and the same reduction prompt.

Run all variants on the same long-document sample and model configuration. Scale the distributed variants over 1, 2, 4, 8, and 16 executors. Record stage-level timings rather than only end-to-end runtime.

### Strong extension: length- and straggler-aware scheduling

Document lengths and LLM call durations are highly skewed. Estimate work from input tokens and requested output tokens, then bucket similarly sized chunks within `mapPartitions`. Compare this with Spark's default partitioning. Inject delays and transient failures to evaluate bounded retries, speculative re-execution, duplicate requests, and tail latency.

**Expected contribution:** a scheduling policy or batching rule that improves p95 completion time and executor utilization while keeping outputs identical or statistically equivalent.

### Strong extension: evidence-preserving hierarchical reduction

Require every chunk summary to return source offsets or sentence identifiers with its claims. During reduction, preserve coverage across sections and flag claims whose evidence disappears or conflicts.

**Expected contribution:** a reduce method that limits cross-chunk omissions and unsupported synthesis, evaluated against simple concatenation-and-resummarization.

### Optional extension: cache-aware serving

If the inference backend exposes prefix/KV caching, route chunks with reusable prefixes to cache-compatible workers. Measure hit rate, load balance, time to first token, and privacy isolation.

**Expected contribution:** evidence showing when caching repeated instructions or overlapping context helps a Spark workload and when routing overhead cancels the benefit.

## Evaluation matrix

| Independent variable | Suggested values |
| --- | --- |
| Executors | 1, 2, 4, 8, 16 |
| Chunking | Fixed tokens; paragraph/section-aware |
| Partitioning | Default; token-length balanced |
| Inference | Sequential requests; batched/continuous-batched server |
| Reduction | Single merge; balanced hierarchical tree; evidence-preserving tree |
| Fault condition | None; delayed task; transient request failure; executor loss if feasible |
| Document length | Short, medium, and long quantiles from the chosen corpus |

Report runtime, throughput, speedup, parallel efficiency, median/p95 latency, tokens and requests per document, retry overhead, ROUGE-1/2/L, BERTScore, compression ratio, factual consistency, section coverage, and a small blinded human assessment. A Pareto plot of quality versus runtime or cost will communicate the main trade-off better than a single metric.

## Full references

[1] M. Zaharia et al., “Apache Spark: A Unified Engine for Big Data Processing,” *Communications of the ACM*, vol. 59, no. 11, pp. 56-65, 2016, doi: [10.1145/2934664](https://doi.org/10.1145/2934664).

[2] M. Armbrust et al., “Spark SQL: Relational Data Processing in Spark,” in *Proceedings of the 2015 ACM SIGMOD International Conference on Management of Data*, pp. 1383-1394, 2015, doi: [10.1145/2723372.2742797](https://doi.org/10.1145/2723372.2742797).

[3] S. Huang, R. Wang, Q. Xie, L. Li, and Y. Liu, “An Extraction-Abstraction Hybrid Approach for Long Document Summarization,” in *2019 6th International Conference on Behavioral, Economic and Socio-Cultural Computing (BESC)*, pp. 1-6, 2019, doi: [10.1109/BESC48373.2019.8962979](https://doi.org/10.1109/BESC48373.2019.8962979).

[4] M.-H. Su, C.-H. Wu, and H.-T. Cheng, “A Two-Stage Transformer-Based Approach for Variable-Length Abstractive Summarization,” *IEEE/ACM Transactions on Audio, Speech, and Language Processing*, vol. 28, pp. 2061-2072, 2020, doi: [10.1109/TASLP.2020.3006731](https://doi.org/10.1109/TASLP.2020.3006731).

[5] S. Y.-T. Lee, A. Bahukhandi, D. Liu, and K.-L. Ma, “Towards Dataset-Scale and Feature-Oriented Evaluation of Text Summarization in Large Language Model Prompts,” *IEEE Transactions on Visualization and Computer Graphics*, vol. 31, no. 1, pp. 481-491, 2025, doi: [10.1109/TVCG.2024.3456398](https://doi.org/10.1109/TVCG.2024.3456398).

[6] W. Kwon et al., “Efficient Memory Management for Large Language Model Serving with PagedAttention,” in *Proceedings of the 29th ACM Symposium on Operating Systems Principles*, pp. 611-626, 2023, doi: [10.1145/3600006.3613165](https://doi.org/10.1145/3600006.3613165).

[7] R. Y. Aminabadi et al., “DeepSpeed-Inference: Enabling Efficient Inference of Transformer Models at Unprecedented Scale,” in *SC22: International Conference for High Performance Computing, Networking, Storage and Analysis*, pp. 1-15, 2022, doi: [10.1109/SC41404.2022.00051](https://doi.org/10.1109/SC41404.2022.00051).

[8] P. Patel et al., “Splitwise: Efficient Generative LLM Inference Using Phase Splitting,” in *2024 ACM/IEEE 51st Annual International Symposium on Computer Architecture (ISCA)*, pp. 118-132, 2024, doi: [10.1109/ISCA59077.2024.00019](https://doi.org/10.1109/ISCA59077.2024.00019).

[9] B. Gao, Z. He, Y. Yao, Z. L. Lou, Z. Zhou, and W.-F. Wong, “Online Context Caching for Distributed Large Language Models Serving,” in *IEEE INFOCOM 2025 - IEEE Conference on Computer Communications*, pp. 1-10, 2025, doi: [10.1109/INFOCOM55648.2025.11044599](https://doi.org/10.1109/INFOCOM55648.2025.11044599).

[10] C. Li et al., “Tetris: Efficient Long-Context LLM Serving with Chunkwise Dynamic Sequence Parallelism,” in *2026 ACM/IEEE 53rd Annual International Symposium on Computer Architecture (ISCA)*, pp. 1287-1301, 2026, doi: [10.1109/ISCA66397.2026.00098](https://doi.org/10.1109/ISCA66397.2026.00098).
