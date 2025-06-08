# barks

[![CI](https://github.com/lonless9/barks/actions/workflows/rust.yml/badge.svg)](https://github.com/lonless9/barks/actions/workflows/rust.yml)
[![codecov](https://codecov.io/gh/lonless9/barks/graph/badge.svg)](https://codecov.io/gh/lonless9/barks)

An experimental, Spark-like distributed computing framework built natively in Rust.

## ⚠️ Disclaimer: Highly Experimental

This project is a learning exercise and is **extremely immature**. It is not suitable for any production use. Many fundamental features are either missing or are simple placeholders.

Key limitations include:
*   **Placeholder Modules:** Most high-level modules like `sql`, `streaming`, and `mllib` are just empty skeletons.
*   **Limited Fault Tolerance:** The system does not handle node or task failures gracefully.
*   **Basic Scheduler:** The task scheduler is very simple and lacks advanced features like speculation or fairness.
*   **Incomplete API:** The RDD API is minimal and missing many common transformations and actions.
*   **No Web UI:** There is no monitoring interface.

## ✨ Features (What's Implemented)

Despite its limitations, `barks` has a basic foundation for distributed computation:

*   **RDD Abstractions:** A `SimpleRdd` for local execution and a `DistributedRdd` for cluster execution.
*   **Local Parallelism:** `FlowContext` uses a local thread pool (`rayon`) to run computations in parallel.
*   **Distributed Execution Model:** A basic Driver/Executor architecture using gRPC for communication.
*   **Serializable Tasks:** Operations on `DistributedRdd` are serializable, allowing them to be sent across the network for execution.
*   **Shuffle Mechanism:** A foundational shuffle service for wide-dependency operations like `reduceByKey`.

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
