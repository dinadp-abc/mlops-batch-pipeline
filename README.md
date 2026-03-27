# MLOps Batch Pipeline

This project implements a minimal MLOps-style batch pipeline for generating trading signals from OHLCV data.

The pipeline is fully reproducible, configurable via YAML, and designed with production practices including structured logging, error handling, and Dockerized execution.

## Features

- Config-driven execution (YAML)
- Deterministic runs using seed control
- Rolling mean-based signal generation
- Structured metrics output (JSON)
- Detailed logging for observability
- Robust input validation and error handling
- Fully Dockerized for one-command execution

## Pipeline Overview

Input CSV → Validation → Rolling Mean → Signal Generation → Metrics → Logs
