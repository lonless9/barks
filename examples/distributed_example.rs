use barks_core::distributed::{Driver, Executor};
use barks_core::distributed::types::ExecutorInfo;
use std::net::SocketAddr;
use tokio::time::{sleep, Duration};
use tracing::{info, error};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::init();

    info!("Starting distributed Barks example");

    // Start driver and executor in parallel
    let driver_handle = tokio::spawn(start_driver());
    let executor_handle = tokio::spawn(start_executor());

    // Wait a bit for services to start
    sleep(Duration::from_secs(2)).await;

    // Submit some example tasks
    let task_handle = tokio::spawn(submit_example_tasks());

    // Wait for all tasks to complete
    let (driver_result, executor_result, task_result) = tokio::join!(
        driver_handle,
        executor_handle,
        task_handle
    );

    if let Err(e) = driver_result? {
        error!("Driver error: {}", e);
    }
    if let Err(e) = executor_result? {
        error!("Executor error: {}", e);
    }
    if let Err(e) = task_result? {
        error!("Task submission error: {}", e);
    }

    info!("Distributed example completed");
    Ok(())
}

async fn start_driver() -> Result<(), Box<dyn std::error::Error>> {
    info!("Starting Driver service");
    
    let driver = Driver::new("driver-1".to_string());
    let addr: SocketAddr = "127.0.0.1:50051".parse()?;
    
    info!("Driver listening on {}", addr);
    driver.start(addr).await?;
    
    Ok(())
}

async fn start_executor() -> Result<(), Box<dyn std::error::Error>> {
    info!("Starting Executor service");
    
    // Wait a bit for driver to start
    sleep(Duration::from_secs(1)).await;
    
    let executor_info = ExecutorInfo::new(
        "executor-1".to_string(),
        "127.0.0.1".to_string(),
        50052,
        4, // 4 cores
        8192, // 8GB memory
    );
    
    let mut executor = Executor::new(executor_info, 10); // max 10 concurrent tasks
    
    // Register with driver
    executor.register_with_driver("http://127.0.0.1:50051".to_string()).await?;
    info!("Executor registered with driver");
    
    // Start heartbeat
    executor.start_heartbeat().await?;
    info!("Executor heartbeat started");
    
    // Start executor service
    let addr: SocketAddr = "127.0.0.1:50052".parse()?;
    info!("Executor listening on {}", addr);
    executor.start(addr).await?;
    
    Ok(())
}

async fn submit_example_tasks() -> Result<(), Box<dyn std::error::Error>> {
    info!("Submitting example tasks");
    
    // Wait for services to be ready
    sleep(Duration::from_secs(3)).await;
    
    // Create a simple driver client for task submission
    // In a real implementation, this would be done through the SparkContext
    info!("Example tasks would be submitted here");
    info!("In a real implementation, tasks would be submitted through SparkContext");
    
    // Simulate some work
    for i in 0..5 {
        info!("Simulating task {}", i);
        sleep(Duration::from_secs(1)).await;
    }
    
    Ok(())
}
