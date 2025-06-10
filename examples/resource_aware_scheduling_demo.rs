/// Resource-Aware Scheduling Demo
///
/// This example demonstrates how the resource-aware scheduling works in practice.
/// It shows how the TaskScheduler considers executor resource utilization when
/// making scheduling decisions.
use barks_core::distributed::task::{PendingTask, TaskScheduler};
use barks_core::distributed::types::{ExecutorInfo, ExecutorMetrics};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Resource-Aware Scheduling Demo ===\n");

    // Create a new task scheduler
    let scheduler = TaskScheduler::new();

    // Register three executors with different resource profiles
    println!("1. Registering executors with different resource profiles:");

    let executor1 = ExecutorInfo::new(
        "executor-1".to_string(),
        "host-1".to_string(),
        8080,
        8081,
        4,    // 4 cores
        4096, // 4GB memory
    );

    let executor2 = ExecutorInfo::new(
        "executor-2".to_string(),
        "host-2".to_string(),
        8080,
        8081,
        8,    // 8 cores
        8192, // 8GB memory
    );

    let executor3 = ExecutorInfo::new(
        "executor-3".to_string(),
        "host-3".to_string(),
        8080,
        8081,
        2,    // 2 cores
        2048, // 2GB memory
    );

    scheduler.register_executor(executor1).await;
    scheduler.register_executor(executor2).await;
    scheduler.register_executor(executor3).await;

    println!("   ✓ executor-1: 4 cores, 4GB memory");
    println!("   ✓ executor-2: 8 cores, 8GB memory");
    println!("   ✓ executor-3: 2 cores, 2GB memory\n");

    // Set up different resource utilization scenarios
    println!("2. Setting up resource utilization scenarios:");

    // Executor 1: Moderate load
    let moderate_load = ExecutorMetrics {
        active_tasks: 2,
        cpu_load_percentage: 50.0,
        memory_utilization_percentage: 60.0,
        available_memory_bytes: 1600 * 1024 * 1024, // 1.6GB available
        cpu_cores_available: 2,
        ..Default::default()
    };

    // Executor 2: Light load (best resources)
    let light_load = ExecutorMetrics {
        active_tasks: 1,
        cpu_load_percentage: 20.0,
        memory_utilization_percentage: 30.0,
        available_memory_bytes: 5600 * 1024 * 1024, // 5.6GB available
        cpu_cores_available: 7,
        ..Default::default()
    };

    // Executor 3: Heavy load (overloaded)
    let heavy_load = ExecutorMetrics {
        active_tasks: 2,
        cpu_load_percentage: 95.0,                 // Overloaded!
        memory_utilization_percentage: 90.0,       // Overloaded!
        available_memory_bytes: 200 * 1024 * 1024, // Only 200MB available
        cpu_cores_available: 0,
        ..Default::default()
    };

    scheduler
        .update_executor_metrics(&"executor-1".to_string(), moderate_load.clone())
        .await;
    scheduler
        .update_executor_metrics(&"executor-2".to_string(), light_load.clone())
        .await;
    scheduler
        .update_executor_metrics(&"executor-3".to_string(), heavy_load.clone())
        .await;

    println!("   ✓ executor-1: Moderate load (CPU: 50%, Memory: 60%)");
    println!("   ✓ executor-2: Light load (CPU: 20%, Memory: 30%)");
    println!("   ✓ executor-3: Heavy load (CPU: 95%, Memory: 90%) - OVERLOADED!\n");

    // Calculate and display resource scores
    println!("3. Resource scores (lower is better):");
    println!("   • executor-1: {:.3}", moderate_load.resource_score());
    println!("   • executor-2: {:.3}", light_load.resource_score());
    println!(
        "   • executor-3: {:.3} (overloaded: {})",
        heavy_load.resource_score(),
        heavy_load.is_overloaded()
    );
    println!();

    // Create tasks to demonstrate scheduling behavior
    println!("4. Demonstrating task scheduling:");

    // Task 1: No locality preference
    let task1 = PendingTask {
        task_id: "task-1".to_string(),
        stage_id: "stage-1".to_string(),
        partition_index: 0,
        serialized_task: vec![],
        preferred_locations: vec![], // No preference
        retries: 0,
        attempt: 0,
    };

    // Task 2: Prefers executor-3 (but it's overloaded)
    let task2 = PendingTask {
        task_id: "task-2".to_string(),
        stage_id: "stage-1".to_string(),
        partition_index: 1,
        serialized_task: vec![],
        preferred_locations: vec!["executor-3".to_string()], // Prefers overloaded executor
        retries: 0,
        attempt: 0,
    };

    scheduler.submit_pending_task(task1).await;
    scheduler.submit_pending_task(task2).await;

    // Try to schedule tasks on each executor
    println!("   Attempting to schedule tasks:");

    // executor-2 should get task-1 (best resources, no overload)
    if let Some(task) = scheduler
        .get_next_task_for_executor(&"executor-2".to_string())
        .await
    {
        println!("   ✓ executor-2 (light load) got: {}", task.task_id);
    }

    // executor-1 should get task-2 (moderate load, not overloaded)
    if let Some(task) = scheduler
        .get_next_task_for_executor(&"executor-1".to_string())
        .await
    {
        println!("   ✓ executor-1 (moderate load) got: {}", task.task_id);
    }

    // executor-3 should get nothing (overloaded)
    if let Some(task) = scheduler
        .get_next_task_for_executor(&"executor-3".to_string())
        .await
    {
        println!(
            "   ✗ executor-3 (overloaded) got: {} - This shouldn't happen!",
            task.task_id
        );
    } else {
        println!("   ✓ executor-3 (overloaded) got: nothing - correctly avoided!");
    }

    println!();

    // Demonstrate best executor selection
    println!("5. Best executor selection for a new task:");

    let new_task = PendingTask {
        task_id: "task-3".to_string(),
        stage_id: "stage-1".to_string(),
        partition_index: 2,
        serialized_task: vec![],
        preferred_locations: vec![], // No preference
        retries: 0,
        attempt: 0,
    };

    let best_executors = scheduler.get_best_executors_for_task(&new_task).await;
    println!("   Best executors in order:");
    for (i, executor_id) in best_executors.iter().enumerate() {
        println!("   {}. {}", i + 1, executor_id);
    }

    println!("\n=== Demo Complete ===");
    println!("Key takeaways:");
    println!("• Overloaded executors are automatically avoided");
    println!("• Tasks are scheduled to executors with better resource availability");
    println!("• Locality preferences are still respected when possible");
    println!("• Resource scores help make optimal scheduling decisions");

    Ok(())
}
