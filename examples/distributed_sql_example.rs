//! Distributed SQL Execution Example
//!
//! This example demonstrates the distributed SQL execution capabilities
//! of Barks, showing how DataFusion LogicalPlans can be serialized and
//! executed across a cluster using the SqlTask implementation.

use barks_core::context::DistributedContext;
use barks_core::distributed::task::Task;
use barks_sql_api::dataframe::{DistributedDataFrame, SqlTask};
use datafusion::arrow::array::{Int32Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::context::SessionContext;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    println!("🚀 Barks Distributed SQL Execution Example");
    println!("==========================================");

    // Example 1: Basic SqlTask execution
    basic_sql_task_example().await?;

    // Example 2: DistributedDataFrame with local execution
    distributed_dataframe_example().await?;

    // Example 3: Demonstrate distributed context integration
    distributed_context_example().await?;

    println!("\n✅ All examples completed successfully!");
    Ok(())
}

async fn basic_sql_task_example() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n📊 Example 1: Basic SqlTask Execution");
    println!("-------------------------------------");

    // Create sample data
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("score", DataType::Int32, false),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])),
            Arc::new(StringArray::from(vec![
                "Alice", "Bob", "Charlie", "David", "Eve",
            ])),
            Arc::new(Int32Array::from(vec![95, 87, 92, 78, 88])),
        ],
    )?;

    println!("Created sample data with {} rows", batch.num_rows());

    // Create a SessionContext and register the data
    let ctx = SessionContext::new();
    ctx.register_batch("students", batch.clone())?;

    // Create a logical plan for a simple query
    let df = ctx
        .sql("SELECT name, score FROM students WHERE score > 85")
        .await?;
    let logical_plan = df.logical_plan().clone();

    println!("Created logical plan: {:?}", logical_plan);

    // Create and execute SqlTask
    let table_name = "students".to_string();
    let sql_query = "SELECT * FROM students WHERE age > 20";
    let sql_task = SqlTask::new(sql_query, batch, table_name)?;

    println!("Created SqlTask, executing...");

    // Execute the task
    let block_manager = barks_network_shuffle::shuffle::FileShuffleBlockManager::new(
        std::env::temp_dir().join("example_sql_shuffle"),
    )?;

    let result_bytes = sql_task.execute(0, Arc::new(block_manager)).await?;

    println!(
        "SqlTask executed successfully, result size: {} bytes",
        result_bytes.len()
    );

    // Deserialize and display results
    if !result_bytes.is_empty() {
        let cursor = std::io::Cursor::new(result_bytes);
        let reader = datafusion::arrow::ipc::reader::StreamReader::try_new(cursor, None)?;

        for batch_result in reader {
            let batch = batch_result?;
            println!(
                "Result batch: {} rows, {} columns",
                batch.num_rows(),
                batch.num_columns()
            );

            // Print the schema and data
            println!("  Schema: {:?}", batch.schema());

            // Print the data (note: SqlTask currently returns all columns)
            for i in 0..batch.num_rows() {
                let mut row_data = Vec::new();
                let schema = batch.schema();
                for col_idx in 0..batch.num_columns() {
                    let column = batch.column(col_idx);
                    let field = schema.field(col_idx);

                    let value_str = match field.data_type() {
                        datafusion::arrow::datatypes::DataType::Int32 => {
                            if let Some(array) = column.as_any().downcast_ref::<Int32Array>() {
                                array.value(i).to_string()
                            } else {
                                "null".to_string()
                            }
                        }
                        datafusion::arrow::datatypes::DataType::Utf8 => {
                            if let Some(array) = column.as_any().downcast_ref::<StringArray>() {
                                array.value(i).to_string()
                            } else {
                                "null".to_string()
                            }
                        }
                        _ => "unsupported_type".to_string(),
                    };

                    row_data.push(format!("{}: {}", field.name(), value_str));
                }
                println!("  Row {}: {}", i, row_data.join(", "));
            }
        }
    }

    // Clean up
    let _ = std::fs::remove_dir_all(std::env::temp_dir().join("example_sql_shuffle"));

    Ok(())
}

async fn distributed_dataframe_example() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n📊 Example 2: DistributedDataFrame Operations");
    println!("---------------------------------------------");

    let ctx = Arc::new(SessionContext::new());

    // Create sample sales data
    let schema = Arc::new(Schema::new(vec![
        Field::new("product_id", DataType::Int32, false),
        Field::new("product_name", DataType::Utf8, false),
        Field::new("quantity", DataType::Int32, false),
        Field::new("price", DataType::Int32, false),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 1, 2])),
            Arc::new(StringArray::from(vec![
                "Laptop",
                "Mouse",
                "Keyboard",
                "Monitor",
                "Headphones",
                "Laptop",
                "Mouse",
            ])),
            Arc::new(Int32Array::from(vec![2, 5, 3, 1, 4, 1, 2])),
            Arc::new(Int32Array::from(vec![1000, 25, 75, 300, 150, 1000, 25])),
        ],
    )?;

    println!("Created sales data with {} rows", batch.num_rows());

    // Create DistributedDataFrame
    let df = DistributedDataFrame::from_record_batches(
        vec![batch],
        ctx.clone(),
        None, // Local execution for this example
    )
    .await?;

    println!("Created DistributedDataFrame");

    // Perform operations
    let filtered_df = df
        .filter(datafusion::logical_expr::col("price").gt(datafusion::logical_expr::lit(50)))?
        .select(vec![
            datafusion::logical_expr::col("product_name"),
            datafusion::logical_expr::col("quantity"),
            datafusion::logical_expr::col("price"),
        ])?;

    println!("Applied filter and select operations");

    // Collect results
    let results = filtered_df.collect().await?;

    println!("Collected results:");
    for batch in results {
        println!("  Batch with {} rows", batch.num_rows());
        for i in 0..batch.num_rows() {
            let name = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(i);
            let quantity = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(i);
            let price = batch
                .column(2)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(i);
            println!("    {} (qty: {}, price: ${})", name, quantity, price);
        }
    }

    Ok(())
}

async fn distributed_context_example() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n📊 Example 3: Distributed Context Integration");
    println!("---------------------------------------------");

    // Create a distributed context in driver mode for demonstration
    let config = barks_core::context::distributed_context::DistributedConfig::default();
    let dist_ctx = Arc::new(DistributedContext::new_driver(
        "sql_example".to_string(),
        config,
    ));
    let session_ctx = Arc::new(SessionContext::new());

    println!("Created distributed context in driver mode");

    // Create sample data
    let schema = Arc::new(Schema::new(vec![
        Field::new("user_id", DataType::Int32, false),
        Field::new("action", DataType::Utf8, false),
        Field::new("timestamp", DataType::Int32, false),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 1, 3, 2, 1, 4])),
            Arc::new(StringArray::from(vec![
                "login", "view", "purchase", "login", "purchase", "logout", "view",
            ])),
            Arc::new(Int32Array::from(vec![
                1000, 1001, 1002, 1003, 1004, 1005, 1006,
            ])),
        ],
    )?;

    println!("Created user activity data with {} rows", batch.num_rows());

    // Create DistributedDataFrame with distributed context
    let df = DistributedDataFrame::from_record_batches(
        vec![batch],
        session_ctx,
        Some(dist_ctx), // This enables distributed execution path
    )
    .await?;

    println!("Created DistributedDataFrame with distributed context");

    // Note: The current implementation falls back to local execution
    // but demonstrates the distributed execution path
    let results = df.collect().await?;

    println!("Executed query through distributed path (local fallback):");
    for batch in results {
        println!("  Result batch with {} rows", batch.num_rows());
    }

    println!("Note: This example demonstrates the distributed SQL execution framework.");
    println!("In a real cluster setup, the SqlTask would be distributed to executors.");

    Ok(())
}
