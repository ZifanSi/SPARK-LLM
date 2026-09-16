# Document-Length Heterogeneity and Load Balancing in Spark-Based LLM Summarization

**ECE 750: Scalable Computer System Design**  
**Group members:** [Names]

## Motivation and related work

Longer documents generally require more fixed-size chunks and may produce more generated tokens and merge work. Equal document counts can therefore conceal unequal inference workloads, leaving workers idle while a few tasks determine job completion time. Spark partitioning controls how this work is grouped and scheduled. Token balancing may reduce imbalance, but finer partitions or independent chunk scheduling may already achieve much of the benefit, with different coordination costs.

SkewTune demonstrates skew mitigation through repartitioning unfinished MapReduce work [1]. S³ studies output-length prediction for inference batching, while BlendServe exploits resource-demand diversity in offline inference [2,3]. These motivate testing, rather than assuming, whether source-token counts adequately represent work. Liu et al. provide direct Spark-plus-LLM precedent through cache-aware relational queries [4]. LLM×MapReduce examines split-and-merge long-sequence processing, and Parrot considers dependencies and application-level performance across LLM calls [5,6]. Ousterhout et al. motivate measuring actual execution bottlenecks [7]. We adapt these established concerns to an empirical study of offline Spark summarization.

## Objective and research questions

**Objective:** Characterize when document-length heterogeneity harms distributed summarization and when balancing benefits exceed partitioning costs.

**Main question:** How does document-length heterogeneity affect completion time and load balance in Spark-based LLM summarization, and under what conditions does workload-aware partitioning improve scalability?

The core workload-aware policy is input-token balancing. Five subquestions guide the study:

1. At matched document count and source-token volume, how does length variability affect runtime and imbalance?
2. When does input-token balancing outperform equal document-count partitioning, and how does this change with GPU-worker count?
3. Does finer Spark partition granularity already remove most imbalance?
4. Does independently scheduling fixed-size chunks reduce the value of document-level balancing?
5. At what workload sizes does balancing overhead outweigh its benefit?

## Experimental design

### Core study

**Variables and workload.** Cross three document-length variability levels (low, moderate, high), two partitioning policies, and available GPU-worker counts. Use one long-document corpus, matching document count and total source tokens across variability levels within reported tolerances. Report achieved length coefficient of variation (standard deviation / mean), maximum length and workload size. Matching source volume does not guarantee equal inference work; record actual tokens and call counts. Any artificially regrouped workloads will receive an intact-document validation.

**Baselines.** Equal document-count partitioning uses seeded, length-independent assignment with counts differing by at most one. Input-token-balanced partitioning sorts documents by decreasing source-token count and greedily places each into the partition with the lowest token total. This uses an existing assignment heuristic. Compare policies on identical documents with equal partition counts and consistent within-partition ordering rules.

**Execution and controls.** Let p denote concurrent GPU workers and K denote Spark partitions. Core runs keep each document's chunks and merge work assigned together. Use one worker and additional configurations supported by comparable GPUs, for example 1/2/3 or 1/2/4, conditional on access. Fix model, tokenizer, precision, prompts, chunk size, summarization structure, output-token limits, batching policy and cache policy. A pilot will establish a context-safe structure, preferably chunk summaries followed by one merge per document; it will then remain fixed.

**Strong scaling.** Increase p while holding the document set, K and policy-specific assignment fixed; choose K at least as large as the maximum tested p. This isolates worker-count effects from partition-count changes. Use at least three paired repetitions, randomize run order and report run-to-run variation. Pilot timing will determine feasible workload sizes.

### Planned supporting experiments

Run all four experiments on selected cases, avoiding a full cross-product with the core study.

| Experiment | Controlled comparison and purpose |
|---|---|
| Partition granularity | At fixed p, compare K=p with K>p, such as 4p, for both policies. Determine whether ordinary Spark scheduling of smaller partitions is sufficient. |
| Chunk scheduling | At fixed p and chunk-stage K, compare document-assigned with independently scheduled chunks. Preserve chunk boundaries, prompts, merge structure and summary order; include regrouping costs. Test whether scheduling granularity explains the benefit of balancing. |
| Overhead / break-even | At fixed p, K and length distribution, vary job size. Include token counting, sorting, assignment and data movement in runtime; identify where net savings appear, if any. |
| Reduced weak scaling | Increase document count and source-token volume proportionally to p, preserving the length distribution and K/p. Compare both policies on a limited subset. |

### Optional extension

Only if input-token balancing leaves substantial unexplained imbalance, evaluate simple estimated-compute partitioning using input size and predicted generation work. Calibrate on separate pilot data, without using future measured outputs to assign evaluation jobs. This extension is not required for the main conclusions.

## Measurements and analysis

- **Primary outcomes:** End-to-end job runtime through completed summaries; Spark stage makespan; per-stage task median, maximum and max/median duration ratio; worker activity/idle timelines; documents/s and source tokens/s. These measure completion time, straggler severity and useful throughput.
- **Attribution:** Record source tokens separately from actual model-input tokens, generated tokens, chunk counts and merge-call counts. Time partitioning, sorting, scheduling, serialization and shuffle work. Include required preprocessing in job runtime; use consistent startup conditions and report model initialization separately. Sampled GPU utilization and task p95 are secondary diagnostics when sample counts support them. Exclude p99 from core metrics.
- **Scaling:** For each policy, report strong-scaling speedup S(p)=T(1)/T(p) and efficiency S(p)/p. For proportional workloads, report weak-scaling efficiency T(1,D)/T(p,pD), alongside realized token and call counts. Scaling evaluates the same workload-balancing question.

## Expected contribution, risks and work plan

The contribution is a reproducible empirical characterization of when heterogeneity matters, when token balancing pays off, and when finer ordinary scheduling is sufficient. Neutral results are useful. We claim neither a new scheduling algorithm nor a formal research gap.

If chunk scheduling removes most imbalance, report that finding and inspect residual merge-stage tails. If input tokens predict cost poorly or generated-token variability dominates, explain this using realized work counters. Limited comparable GPUs will bound scaling claims. If inference dominates, quantify its share rather than assume Spark overhead is decisive. Validate artificial workloads on intact documents; control experiment growth through fixed secondary settings and selected supporting cases. Check complete document/chunk coverage so faster runs do not reflect omitted work.

Work proceeds through a feasibility pilot, pipeline and instrumentation validation, core measurements, supporting experiments, and joint analysis. Three pairs can lead workload construction and correctness; Spark execution and partitioning; and instrumentation and benchmark analysis. All six members review experimental design and conclusions. Deliverables include reproducible configurations, workload manifests, traces and comparative results.

## References

[1] Y. Kwon et al. [“SkewTune: Mitigating Skew in MapReduce Applications.”](https://doi.org/10.1145/2213836.2213840) SIGMOD, 2012.

[2] Y. Jin et al. [“S³: Increasing GPU Utilization during Generative Inference for Higher Throughput.”](https://proceedings.neurips.cc/paper_files/paper/2023/file/3a13be0c5dae69e0f08065f113fb10b8-Paper-Conference.pdf) NeurIPS, 2023.

[3] Y. Zhao et al. [“BlendServe: Optimizing Offline Inference with Resource-Aware Batching.”](https://doi.org/10.1145/3779212.3790133) ASPLOS, 2026.

[4] S. Liu et al. [“Optimizing LLM Queries in Relational Data Analytics Workloads.”](https://proceedings.mlsys.org/paper_files/paper/2025/file/b5dc49f44db2fadc5c4d717c57f4a424-Paper-Conference.pdf) MLSys, 2025.

[5] Z. Zhou et al. [“LLM×MapReduce: Simplified Long-Sequence Processing using Large Language Models.”](https://aclanthology.org/2025.acl-long.1341/) ACL, 2025.

[6] C. Lin et al. [“Parrot: Efficient Serving of LLM-based Applications with Semantic Variable.”](https://www.usenix.org/conference/osdi24/presentation/lin-chaofan) OSDI, 2024.

[7] K. Ousterhout et al. [“Making Sense of Performance in Data Analytics Frameworks.”](https://www.usenix.org/conference/nsdi15/technical-sessions/presentation/ousterhout) NSDI, 2015.
