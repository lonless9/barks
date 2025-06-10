//! CSV and Parquet Connectors Example
//!
//! This example demonstrates how to use the CSV and Parquet connectors
//! to read data directly into DistributedDataFrames using DataFusion.

use barks_sql_api::dataframe::DistributedDataFrame;
use barks_sql_api::traits::{DataFusionSqlContext, SqlContext};
use barks_sql_core::SqlConfig;
use datafusion::execution::context::SessionContext;
use datafusion::prelude::CsvReadOptions;
use std::io::Write;
use std::sync::Arc;
use tempfile::NamedTempFile;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    tracing_subscriber::fmt::init();

    println!("🚀 CSV and Parquet Connectors Example");
    println!("=====================================");

    // Example 1: CSV Connector
    csv_connector_example().await?;

    // Example 2: Parquet Connector (using CSV to Parquet conversion)
    parquet_connector_example().await?;

    // Example 3: DistributedDataFrame from files
    dataframe_from_files_example().await?;

    // Example 4: Advanced SQL operations on file data
    advanced_sql_operations_example().await?;

    println!("\n✅ All examples completed successfully!");
    Ok(())
}

async fn csv_connector_example() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n📊 Example 1: CSV Connector");
    println!("---------------------------");

    // Create a sample CSV file
    let mut csv_file = NamedTempFile::new()?;
    writeln!(csv_file, "employee_id,name,department,salary,hire_date")?;
    writeln!(csv_file, "1,Alice Johnson,Engineering,75000,2023-01-15")?;
    writeln!(csv_file, "2,Bob Smith,Marketing,65000,2023-02-20")?;
    writeln!(csv_file, "3,Carol Davis,Engineering,80000,2023-01-10")?;
    writeln!(csv_file, "4,David Wilson,Sales,70000,2023-03-05")?;
    writeln!(csv_file, "5,Eve Brown,Engineering,85000,2022-12-01")?;

    let csv_path = csv_file.path().to_str().unwrap();
    println!("Created CSV file: {}", csv_path);

    // Create SQL context and register CSV table
    let config = SqlConfig::default();
    let sql_context = DataFusionSqlContext::new(config);

    sql_context
        .register_csv_table("employees", csv_path)
        .await?;
    println!("Registered CSV table 'employees'");

    // Execute various SQL queries
    println!("\n🔍 Query 1: Count all employees");
    let results = sql_context
        .sql("SELECT COUNT(*) as total_employees FROM employees")
        .await?;
    print_results(&results, "Total Employees");

    println!("\n🔍 Query 2: Engineering department employees");
    let results = sql_context
        .sql("SELECT name, salary FROM employees WHERE department = 'Engineering' ORDER BY salary DESC")
        .await?;
    print_results(&results, "Engineering Employees");

    println!("\n🔍 Query 3: Average salary by department");
    let results = sql_context
        .sql("SELECT department, AVG(salary) as avg_salary FROM employees GROUP BY department ORDER BY avg_salary DESC")
        .await?;
    print_results(&results, "Average Salary by Department");

    Ok(())
}

async fn parquet_connector_example() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n📦 Example 2: Parquet Connector");
    println!("-------------------------------");

    // Create a sample CSV file first
    let mut csv_file = NamedTempFile::new()?;
    writeln!(
        csv_file,
        "product_id,product_name,category,price,stock_quantity"
    )?;
    writeln!(csv_file, "1,Laptop Pro,Electronics,1299.99,50")?;
    writeln!(csv_file, "2,Wireless Mouse,Electronics,29.99,200")?;
    writeln!(csv_file, "3,Office Chair,Furniture,199.99,75")?;
    writeln!(csv_file, "4,Standing Desk,Furniture,399.99,30")?;
    writeln!(csv_file, "5,Smartphone,Electronics,699.99,100")?;

    let csv_path = csv_file.path().to_str().unwrap();

    // Convert CSV to Parquet using DataFusion
    let ctx = SessionContext::new();
    let df = ctx.read_csv(csv_path, CsvReadOptions::new()).await?;

    let parquet_file = NamedTempFile::with_suffix(".parquet")?;
    let parquet_path = parquet_file.path().to_str().unwrap();

    // Write to Parquet
    df.write_parquet(
        parquet_path,
        datafusion::dataframe::DataFrameWriteOptions::new(),
        None,
    )
    .await?;
    println!("Created Parquet file: {}", parquet_path);

    // Create SQL context and register Parquet table
    let config = SqlConfig::default();
    let sql_context = DataFusionSqlContext::new(config);

    sql_context
        .register_parquet_table("products", parquet_path)
        .await?;
    println!("Registered Parquet table 'products'");

    // Execute queries on Parquet data
    println!("\n🔍 Query 1: High-value products");
    let results = sql_context
        .sql("SELECT product_name, price FROM products WHERE price > 500 ORDER BY price DESC")
        .await?;
    print_results(&results, "High-Value Products");

    println!("\n🔍 Query 2: Inventory summary by category");
    let results = sql_context
        .sql("SELECT category, COUNT(*) as product_count, SUM(stock_quantity) as total_stock FROM products GROUP BY category")
        .await?;
    print_results(&results, "Inventory Summary");

    Ok(())
}

async fn dataframe_from_files_example() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n📋 Example 3: DistributedDataFrame from Files");
    println!("---------------------------------------------");

    // Create a sample CSV file
    let mut csv_file = NamedTempFile::new()?;
    writeln!(csv_file, "order_id,customer_id,product,quantity,unit_price")?;
    writeln!(csv_file, "1001,C001,Laptop,1,999.99")?;
    writeln!(csv_file, "1002,C002,Mouse,2,25.50")?;
    writeln!(csv_file, "1003,C001,Keyboard,1,79.99")?;
    writeln!(csv_file, "1004,C003,Monitor,1,299.99")?;
    writeln!(csv_file, "1005,C002,Headphones,1,149.99")?;

    let csv_path = csv_file.path().to_str().unwrap();

    // Create DistributedDataFrame directly from CSV
    let session_ctx = Arc::new(SessionContext::new());
    let dataframe = DistributedDataFrame::from_csv(csv_path, session_ctx.clone(), None).await?;

    println!("Created DistributedDataFrame from CSV");

    // Collect and display the data
    println!("\n🔍 DataFrame Contents: All orders");
    let results = dataframe.collect().await?;
    print_results(&results, "All Orders");

    // Create another DataFrame with Parquet
    let parquet_file = NamedTempFile::with_suffix(".parquet")?;
    let parquet_path = parquet_file.path().to_str().unwrap();

    // Convert CSV to Parquet first
    let df = session_ctx
        .read_csv(csv_path, CsvReadOptions::new())
        .await?;
    df.write_parquet(
        parquet_path,
        datafusion::dataframe::DataFrameWriteOptions::new(),
        None,
    )
    .await?;

    // Create DistributedDataFrame from Parquet
    let parquet_dataframe =
        DistributedDataFrame::from_parquet(parquet_path, session_ctx.clone(), None).await?;

    println!("\n🔍 Parquet DataFrame: High-value orders");
    let high_value_results = parquet_dataframe
        .filter(
            datafusion::logical_expr::col("unit_price").gt(datafusion::logical_expr::lit(100.0)),
        )?
        .collect()
        .await?;
    print_results(&high_value_results, "High-Value Orders");

    Ok(())
}

async fn advanced_sql_operations_example() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n🎯 Example 4: Advanced SQL Operations");
    println!("------------------------------------");

    // Create customers CSV
    let mut customers_file = NamedTempFile::new()?;
    writeln!(customers_file, "customer_id,name,email,city")?;
    writeln!(
        customers_file,
        "C001,Alice Johnson,alice@email.com,New York"
    )?;
    writeln!(customers_file, "C002,Bob Smith,bob@email.com,Los Angeles")?;
    writeln!(customers_file, "C003,Carol Davis,carol@email.com,Chicago")?;

    // Create orders CSV
    let mut orders_file = NamedTempFile::new()?;
    writeln!(
        orders_file,
        "order_id,customer_id,product,amount,order_date"
    )?;
    writeln!(orders_file, "1001,C001,Laptop,999.99,2024-01-15")?;
    writeln!(orders_file, "1002,C002,Mouse,25.50,2024-01-16")?;
    writeln!(orders_file, "1003,C001,Keyboard,79.99,2024-01-17")?;
    writeln!(orders_file, "1004,C003,Monitor,299.99,2024-01-18")?;
    writeln!(orders_file, "1005,C002,Headphones,149.99,2024-01-19")?;

    let customers_path = customers_file.path().to_str().unwrap();
    let orders_path = orders_file.path().to_str().unwrap();

    // Create SQL context and register both tables
    let config = SqlConfig::default();
    let sql_context = DataFusionSqlContext::new(config);

    sql_context
        .register_csv_table("customers", customers_path)
        .await?;
    sql_context
        .register_csv_table("orders", orders_path)
        .await?;

    println!("Registered customers and orders tables");

    // Complex JOIN query
    println!("\n🔍 Advanced Query: Customer order summary with JOIN");
    let results = sql_context
        .sql(
            r#"
            SELECT
                c.name,
                c.city,
                COUNT(o.order_id) as order_count,
                SUM(o.amount) as total_spent,
                AVG(o.amount) as avg_order_value
            FROM customers c
            LEFT JOIN orders o ON c.customer_id = o.customer_id
            GROUP BY c.customer_id, c.name, c.city
            ORDER BY total_spent DESC
        "#,
        )
        .await?;

    print_results(&results, "Customer Order Summary");

    // Window function query
    println!("\n🔍 Advanced Query: Orders with running totals");
    let results = sql_context
        .sql(
            r#"
            SELECT
                order_id,
                customer_id,
                product,
                amount,
                SUM(amount) OVER (PARTITION BY customer_id ORDER BY order_date) as running_total
            FROM orders
            ORDER BY customer_id, order_date
        "#,
        )
        .await?;

    print_results(&results, "Orders with Running Totals");

    Ok(())
}

fn print_results(results: &[datafusion::arrow::record_batch::RecordBatch], title: &str) {
    println!("\n📊 {}", title);
    println!("{}", "=".repeat(title.len() + 4));

    if results.is_empty() {
        println!("No results");
        return;
    }

    // Print schema
    let schema = results[0].schema();
    let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    println!("Columns: {}", field_names.join(", "));

    // Print row count
    let total_rows: usize = results.iter().map(|batch| batch.num_rows()).sum();
    println!("Rows: {}", total_rows);

    // Print first few rows for demonstration
    if total_rows > 0 {
        println!("Sample data:");
        for (i, batch) in results.iter().enumerate() {
            if i > 0 {
                break;
            } // Only show first batch
            for row in 0..std::cmp::min(batch.num_rows(), 5) {
                let mut row_data = Vec::new();
                for col in 0..batch.num_columns() {
                    let array = batch.column(col);
                    let value = format!("{:?}", array.slice(row, 1));
                    // Clean up the array formatting
                    let clean_value = value
                        .trim_start_matches('[')
                        .trim_end_matches(']')
                        .replace("\"", "");
                    row_data.push(clean_value);
                }
                println!("  {}", row_data.join(" | "));
            }
        }
    }
}
