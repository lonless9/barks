//! Distributed Computing Demo
//!
//! This example demonstrates the distributed Driver-Executor architecture
//! with RPC communication using gRPC and task execution with rayon.

use barks_core::distributed::{
    context::{DistributedConfig, DistributedContext, ExecutionMode, ExecutorConfig},
    driver::Driver,
    executor::Executor,
    types::{ExecutorInfo, ExecutorStatus},
};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{info, warn};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    info!("=== Barks Distributed Computing Demo ===");

    // Parse command line arguments to determine mode
    let args: Vec<String> = std::env::args().collect();
    
    if args.len() < 2 {
        println!("Usage: {} <mode> [options]", args[0]);
        println!("Modes:");
        println!("  driver [port]           - Start driver on specified port (default: 8080)");
        println!("  executor <driver_addr> [executor_port] - Start executor and connect to driver");
        println!("  local                   - Run in local mode");
        return Ok(());
    }

    let mode = &args[1];

    match mode.as_str() {
        "driver" => {
            let port = args.get(2)
                .and_then(|s| s.parse::<u16>().ok())
                .unwrap_or(8080);
            
            run_driver(port).await?;
        }
        "executor" => {
            if args.len() < 3 {
                println!("Error: executor mode requires driver address");
                println!("Usage: {} executor <driver_addr> [executor_port]", args[0]);
                return Ok(());
            }
            
            let driver_addr = &args[2];
            let executor_port = args.get(3)
                .and_then(|s| s.parse::<u16>().ok())
                .unwrap_or(8081);
            
            run_executor(driver_addr, executor_port).await?;
        }
        "local" => {
            run_local_demo().await?;
        }
        _ => {
            println!("Unknown mode: {}", mode);
            println!("Valid modes: driver, executor, local");
        }
    }

    Ok(())
}

/// Run the driver
async fn run_driver(port: u16) -> Result<(), Box<dyn std::error::Error>> {
    info!("Starting Driver on port {}", port);

    let config = DistributedConfig::default();
    let context = DistributedContext::new_driver("barks-distributed-demo".to_string(), config);
    
    let addr: SocketAddr = format!("127.0.0.1:{}", port).parse()?;
    
    // Start the driver service
    tokio::spawn(async move {
        if let Err(e) = context.start(addr).await {
            warn!("Driver service error: {}", e);
        }
    });

    // Simulate some work and monitoring
    let mut interval = tokio::time::interval(Duration::from_secs(5));
    loop {
        interval.tick().await;
        
        if let Some(stats) = context.get_driver_stats().await {
            info!("Driver Stats - Executors: {}, Pending Tasks: {}", 
                  stats.executor_count, stats.pending_task_count);
        }
        
        // For demo purposes, submit some tasks if we have executors
        if let Some(stats) = context.get_driver_stats().await {
            if stats.executor_count > 0 && stats.pending_task_count == 0 {
                info!("Submitting demo tasks...");
                submit_demo_tasks(&context).await;
            }
        }
    }
}

/// Run an executor
async fn run_executor(driver_addr: &str, executor_port: u16) -> Result<(), Box<dyn std::error::Error>> {
    info!("Starting Executor on port {} connecting to driver at {}", executor_port, driver_addr);

    let executor_id = format!("executor-{}", uuid::Uuid::new_v4());
    let host = "127.0.0.1".to_string();
    
    let config = DistributedConfig {
        driver_addr: Some(driver_addr.parse()?),
        executor_config: ExecutorConfig {
            cores: 4,
            memory_mb: 2048,
            max_concurrent_tasks: 4,
            attributes: HashMap::new(),
        },
        ..Default::default()
    };

    let mut context = DistributedContext::new_executor(
        "barks-distributed-demo".to_string(),
        executor_id,
        host,
        executor_port,
        config,
    );

    let addr: SocketAddr = format!("127.0.0.1:{}", executor_port).parse()?;
    
    // Start the executor service
    tokio::spawn(async move {
        if let Err(e) = context.start(addr).await {
            warn!("Executor service error: {}", e);
        }
    });

    // Register with driver
    context.register_with_driver(driver_addr.to_string()).await?;
    
    info!("Executor registered and running. Press Ctrl+C to stop.");
    
    // Keep the executor running
    loop {
        sleep(Duration::from_secs(10)).await;
        info!("Executor heartbeat...");
    }
}

/// Run local demo
async fn run_local_demo() -> Result<(), Box<dyn std::error::Error>> {
    info!("Running local distributed computing demo");

    let context = DistributedContext::new_local("barks-local-demo".to_string());
    
    // Create some test data
    let data = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
    info!("Original data: {:?}", data);

    // Create RDD with multiple partitions
    let rdd = context.parallelize_with_partitions(data, 3);
    info!("Created RDD with {} partitions", rdd.num_partitions());

    // Apply transformations
    let mapped_rdd = rdd.map(|x| x * 2);
    let filtered_rdd = mapped_rdd.filter(|&x| x > 10);

    // Execute and collect results
    let result = context.run(filtered_rdd).await?;
    info!("Final result: {:?}", result);

    Ok(())
}

/// Submit demo tasks to the driver
async fn submit_demo_tasks(context: &DistributedContext) {
    // This is a simplified demo - in a real implementation,
    // tasks would be created from RDD operations
    info!("Demo task submission would happen here");
    
    // For now, just log that we would submit tasks
    info!("Would submit tasks for distributed execution");
}

// Simple UUID implementation for demo
mod uuid {
    use std::fmt;

    pub struct Uuid(u128);

    impl Uuid {
        pub fn new_v4() -> Self {
            use std::time::{SystemTime, UNIX_EPOCH};
            let timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            Self(timestamp)
        }
    }

    impl fmt::Display for Uuid {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "{:032x}", self.0)
        }
    }
}
