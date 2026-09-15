# spark-llm

Course project for **ECE 750 T39 — Scalable Computer System Design** (University of Waterloo, Fall 2026).

Project idea #8 from the course project list: **Apache Spark and LLM-based distributed long document summarization**.

## Problem

Summarizing very large document collections (or very long individual documents) with an LLM does not scale
on a single machine: context-window limits force chunking, and LLM inference per chunk is slow enough that
throughput becomes the bottleneck. This project explores using Apache Spark to distribute the chunking,
LLM-inference, and reduce/merge stages of long-document summarization across a cluster, and evaluates how
the approach scales with document size, cluster size, and chunking strategy.

## Repository layout

```
Requirements/     Course requirements as distributed (evaluation structure, deliverables, ECE server how-to)
docs/
  proposal/       Project proposal (due per LEARN schedule)
  presentation/   Slide deck for the project presentation
  report/         Final project report (IEEE double-column format)
src/
  spark_llm_summarization/   Main Python package: Spark pipeline + LLM summarization code
scripts/          Entry-point scripts for running the pipeline on ecehadoop / locally
data/
  raw/            Input documents/datasets (not committed — see .gitignore)
  processed/      Intermediate/output data (not committed)
notebooks/        Exploratory notebooks
tests/            Unit tests
```

## Course deliverables checklist

- [ ] Project proposal (5%)
- [ ] Paper review 1 (individual)
- [ ] Programming assignment (group, 7%)
- [ ] Test 1
- [ ] Paper review 2 (individual)
- [ ] Test 2
- [ ] Project presentation (20%)
- [ ] Project report (20%, IEEE double-column, >= 4 pages excl. references)
- [ ] Implementation upload (zip/tarball + how-to doc) to LEARN dropbox
- [ ] Backup copy of code on git.uwaterloo.ca (GitLab) — do not use a public repo for course code

See `Requirements/Evaluation_structure_and_important_dates.txt` for exact dates (subject to change on LEARN)
and `Requirements/Project_deliverables.txt` for full deliverable descriptions.

## Running the project

See `docs/report/how-to.md` (to be written) for setup and reproduction instructions, including how to run
on the ECE `ecehadoop` Spark/MPI cluster (see `Requirements/ece_servers_how-to.pdf`).
