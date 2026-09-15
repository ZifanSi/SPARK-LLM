"""Entry point for running the Spark long-document summarization pipeline.

Usage:
    python scripts/run_pipeline.py --input data/raw --output data/processed
"""

import argparse


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, help="Path to input documents (local or HDFS)")
    parser.add_argument("--output", required=True, help="Path to write summarization output")
    args = parser.parse_args()

    raise NotImplementedError("Wire up spark_llm_summarization.pipeline here")


if __name__ == "__main__":
    main()
