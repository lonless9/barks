# barks

[![CI](https://github.com/lonless9/barks/actions/workflows/rust.yml/badge.svg)](https://github.com/lonless9/barks/actions/workflows/rust.yml)
[![codecov](https://codecov.io/gh/lonless9/barks/graph/badge.svg)](https://codecov.io/gh/lonless9/barks)
[![Hits-of-Code](https://hitsofcode.com/github/lonless9/barks?branch=main&label=Hits-of-Code)](https://hitsofcode.com/github/lonless9/barks/view?branch=main&label=Hits-of-Code)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](https://github.com/lonless9/barks/blob/main/LICENSE)

An experimental, Spark-like distributed computing framework built natively in Rust.

## ⚠️ Disclaimer: Highly Experimental

This project is a learning exercise and is **extremely immature**. It is not suitable for any production use. Many fundamental features are either missing or are simple placeholders.

Key limitations include:
*   **Placeholder Modules:** Most high-level modules like `sql`, `streaming`, and `mllib` are just empty skeletons.
*   **Basic Fault Tolerance:** The system has basic fault tolerance, including task retries and executor failure detection, but lacks advanced recovery strategies like stage-level retries or checkpointing.
*   **Basic Scheduler:** The task scheduler is very simple and lacks advanced features like speculation or fairness.
*   **Incomplete API:** The RDD API is minimal and missing many common transformations and actions compared to Spark.
*   **No Web UI:** There is no monitoring interface.

## ✨ Features (What's Implemented)

Despite its limitations, `barks` has a basic foundation for distributed computation:

*   **Generic RDD Abstraction:** The core abstraction is `DistributedRdd<T>`, a generic Resilient Distributed Dataset. It supports any data type `T` that implements the `RddDataType` trait, enabling type-safe distributed collections.
*   **Serializable & Extensible Tasks:** The framework uses a `Task` trait with `typetag` for serialization. This allows custom computations to be packaged, sent over the network, and executed remotely, avoiding the limitations of closures in a distributed environment.
*   **Local Parallelism:** `FlowContext` provides a local execution environment that leverages `rayon` to run computations in parallel on a single machine, mirroring Spark's local mode.
*   **Distributed Execution Model:** A robust Driver/Executor architecture using gRPC for communication.
    *   Resilient executor registration with connection retries.
    *   Executor liveness monitoring via heartbeats and automatic re-scheduling of tasks from failed executors.
*   **Shuffle Mechanism:** A foundational shuffle system for wide-dependency operations.
    *   An HTTP-based shuffle service (using `axum`) for transferring intermediate data.
    *   Specialized `ShuffleMapTask` and `ShuffleReduceTask` for orchestrating shuffles.
    *   Pair RDD operations like `reduceByKey`, `groupByKey`, and `join` are implemented on top of this system.
*   **Optimized Shuffle Writer:** A `HashShuffleWriter` that can spill to disk and sort data to improve shuffle performance.

## 🏗️ Building and Testing

To build the project, you'll need the Protocol Buffers compiler (`protoc`).

### Ubuntu/Debian
```sh
sudo apt-get update && sudo apt-get install -y protobuf-compiler
```

# Build & Test
## Build all modules
```sh
cargo build --workspace
```

## Run all tests
```sh
cargo test --workspace
```

# 🤝 Contributing
This is a personal learning project. Feel free to explore the code, but please be aware of its experimental and incomplete nature.
