//! DataFusion SQL Integration Example
//!
//! This example demonstrates how to use Apache DataFusion for SQL query processing
//! in the Barks distributed computing framework.

use barks_sql_api::{DataFusionQueryEngine, SqlConfig, dataframe::DistributedDataFrame};
use barks_sql_core::columnar::conversion;
use barks_sql_core::traits::SqlQueryEngine;
use datafusion::arrow::array::{Float64Array, Int32Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::context::SessionContext;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 Barks DataFusion SQL Integration Example");
    println!("============================================");

    // Example 0: RDD to DataFrame to SQL
    rdd_to_sql_example().await?;

    // Example 1: Basic SQL Query Engine
    basic_sql_example().await?;

    // Example 2: DataFrame API
    dataframe_api_example().await?;

    // Example 3: Complex Analytics
    complex_analytics_example().await?;

    // Example 4: Data Type Conversions
    data_conversion_example().await?;

    println!("\n✅ All examples completed successfully!");
    Ok(())
}

async fn rdd_to_sql_example() -> Result<(), Box<dyn std::error::Error>> {
    use barks_core::rdd::DistributedRdd;
    use barks_core::traits::RddBase;

    println!("\n📊 Example 0: RDD to DataFrame to SQL");
    println!("------------------------------------");

    // 1. Create a Barks RDD
    let data: Vec<i32> = (1..=100).collect();
    let rdd: Arc<dyn RddBase<Item = i32>> =
        Arc::new(DistributedRdd::from_vec_with_partitions(data, 4));
    println!("Created an RDD with 100 integers.");

    // 2. Create a SQL query engine
    let config = SqlConfig::default();
    let engine = DataFusionQueryEngine::new(config);

    // 3. Register the RDD as a table
    // Note: We use register_rdd_any for demonstration. In a real application, you might have
    // a more type-safe registration method if you know the RDD type.
    engine
        .register_rdd_any("my_rdd_table", rdd.as_is_rdd())
        .await?;
    println!("Registered RDD as SQL table 'my_rdd_table'.");

    // 4. Execute a SQL query on the RDD-backed table
    let sql = "SELECT MIN(value), MAX(value), AVG(value), COUNT(value) FROM my_rdd_table WHERE value > 50";
    println!("Executing SQL: {}", sql);

    let results = engine.execute_sql(sql).await?;

    // 5. Print the results
    print_results(&results);

    Ok(())
}

async fn basic_sql_example() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n📊 Example 1: Basic SQL Query Engine");
    println!("------------------------------------");

    // Create a SQL query engine with custom configuration
    let config = SqlConfig {
        batch_size: 4096,
        enable_optimization: true,
        enable_distributed_execution: false, // Local execution for this example
        max_concurrent_tasks: 4,
        ..SqlConfig::default()
    };

    let engine = DataFusionQueryEngine::new(config);

    // Create sample sales data
    let sales_schema = Arc::new(Schema::new(vec![
        Field::new("product_id", DataType::Int32, false),
        Field::new("product_name", DataType::Utf8, false),
        Field::new("category", DataType::Utf8, false),
        Field::new("price", DataType::Float64, false),
        Field::new("quantity", DataType::Int32, false),
    ]));

    let sales_batch = RecordBatch::try_new(
        sales_schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8])),
            Arc::new(StringArray::from(vec![
                "Laptop",
                "Mouse",
                "Keyboard",
                "Monitor",
                "Tablet",
                "Phone",
                "Headphones",
                "Speaker",
            ])),
            Arc::new(StringArray::from(vec![
                "Electronics",
                "Accessories",
                "Accessories",
                "Electronics",
                "Electronics",
                "Electronics",
                "Accessories",
                "Accessories",
            ])),
            Arc::new(Float64Array::from(vec![
                999.99, 29.99, 79.99, 299.99, 599.99, 799.99, 149.99, 89.99,
            ])),
            Arc::new(Int32Array::from(vec![10, 50, 30, 15, 25, 20, 40, 35])),
        ],
    )?;

    // Register the data as a table
    engine
        .register_record_batches("sales", vec![sales_batch])
        .await?;

    // Execute various SQL queries
    println!("📋 Total products in inventory:");
    let results = engine
        .execute_sql("SELECT COUNT(*) as total_products FROM sales")
        .await?;
    print_results(&results);

    println!("\n💰 Revenue by category:");
    let results = engine
        .execute_sql(
            "SELECT
            category,
            SUM(price * quantity) as total_revenue,
            COUNT(*) as product_count
         FROM sales
         GROUP BY category
         ORDER BY total_revenue DESC",
        )
        .await?;
    print_results(&results);

    println!("\n🔝 Top 3 most expensive products:");
    let results = engine
        .execute_sql(
            "SELECT product_name, price
         FROM sales
         ORDER BY price DESC
         LIMIT 3",
        )
        .await?;
    print_results(&results);

    Ok(())
}

async fn dataframe_api_example() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n📈 Example 2: DataFrame API");
    println!("---------------------------");

    let ctx = Arc::new(SessionContext::new());

    // Create employee data
    let employee_data = vec![
        (1, "Alice".to_string(), "Engineering".to_string(), 75000.0),
        (2, "Bob".to_string(), "Sales".to_string(), 65000.0),
        (3, "Charlie".to_string(), "Engineering".to_string(), 80000.0),
        (4, "Diana".to_string(), "Marketing".to_string(), 70000.0),
        (5, "Eve".to_string(), "Sales".to_string(), 68000.0),
        (6, "Frank".to_string(), "Engineering".to_string(), 85000.0),
    ];

    // Convert to RecordBatch using our columnar utilities
    let (ids, names, departments, salaries): (Vec<_>, Vec<_>, Vec<_>, Vec<_>) =
        employee_data.unzip4();

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("department", DataType::Utf8, false),
        Field::new("salary", DataType::Float64, false),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(ids)),
            Arc::new(StringArray::from(names)),
            Arc::new(StringArray::from(departments)),
            Arc::new(Float64Array::from(salaries)),
        ],
    )?;

    // Create DataFrame
    let df = DistributedDataFrame::from_record_batches(vec![batch], ctx.clone(), None).await?;

    println!("👥 All employees:");
    let all_employees = df.clone();
    all_employees.show().await?;

    println!("\n💼 Engineering department only:");
    let engineering_df = df.clone().filter(
        datafusion::logical_expr::col("department")
            .eq(datafusion::logical_expr::lit("Engineering")),
    )?;
    engineering_df.show().await?;

    println!("\n📊 Average salary by department:");
    let avg_salary_df = df
        .group_by(vec![datafusion::logical_expr::col("department")])?
        .avg("salary")?;
    avg_salary_df.show().await?;

    Ok(())
}

async fn complex_analytics_example() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n🔍 Example 3: Complex Analytics");
    println!("-------------------------------");

    let config = SqlConfig::default();
    let engine = DataFusionQueryEngine::new(config);

    // Create order data
    let orders_schema = Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int32, false),
        Field::new("customer_id", DataType::Int32, false),
        Field::new("product_id", DataType::Int32, false),
        Field::new("quantity", DataType::Int32, false),
        Field::new("unit_price", DataType::Float64, false),
        Field::new("order_date", DataType::Utf8, false),
    ]));

    let orders_batch = RecordBatch::try_new(
        orders_schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10])),
            Arc::new(Int32Array::from(vec![
                101, 102, 103, 101, 104, 102, 105, 103, 104, 101,
            ])),
            Arc::new(Int32Array::from(vec![1, 2, 1, 3, 2, 1, 4, 3, 4, 2])),
            Arc::new(Int32Array::from(vec![2, 1, 3, 1, 2, 1, 1, 2, 1, 3])),
            Arc::new(Float64Array::from(vec![
                999.99, 29.99, 999.99, 79.99, 29.99, 999.99, 149.99, 79.99, 149.99, 29.99,
            ])),
            Arc::new(StringArray::from(vec![
                "2024-01-15",
                "2024-01-16",
                "2024-01-17",
                "2024-01-18",
                "2024-01-19",
                "2024-01-20",
                "2024-01-21",
                "2024-01-22",
                "2024-01-23",
                "2024-01-24",
            ])),
        ],
    )?;

    engine
        .register_record_batches("orders", vec![orders_batch])
        .await?;

    println!("🛒 Customer purchase analysis:");
    let results = engine
        .execute_sql(
            "SELECT
            customer_id,
            COUNT(*) as order_count,
            SUM(quantity * unit_price) as total_spent,
            AVG(quantity * unit_price) as avg_order_value,
            MIN(order_date) as first_order,
            MAX(order_date) as last_order
         FROM orders
         GROUP BY customer_id
         ORDER BY total_spent DESC",
        )
        .await?;
    print_results(&results);

    println!("\n📈 Product popularity ranking:");
    let results = engine
        .execute_sql(
            "SELECT
            product_id,
            SUM(quantity) as total_quantity_sold,
            COUNT(*) as order_frequency,
            SUM(quantity * unit_price) as total_revenue,
            ROW_NUMBER() OVER (ORDER BY SUM(quantity * unit_price) DESC) as revenue_rank
         FROM orders
         GROUP BY product_id
         ORDER BY total_revenue DESC",
        )
        .await?;
    print_results(&results);

    Ok(())
}

async fn data_conversion_example() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n🔄 Example 4: Data Type Conversions");
    println!("-----------------------------------");

    // Demonstrate columnar data conversion utilities
    let tuples = vec![
        (1, "Product A".to_string()),
        (2, "Product B".to_string()),
        (3, "Product C".to_string()),
    ];

    println!("📦 Converting tuples to RecordBatch:");
    let batch = conversion::tuples_to_record_batch(tuples.clone(), "id", "name")?;
    println!(
        "Created RecordBatch with {} rows and {} columns",
        batch.num_rows(),
        batch.num_columns()
    );

    println!("\n🔙 Converting RecordBatch back to tuples:");
    let converted_tuples: Vec<(i32, String)> = conversion::record_batch_to_tuples(batch)?;
    for (id, name) in &converted_tuples {
        println!("  ID: {}, Name: {}", id, name);
    }

    // Verify round-trip conversion
    assert_eq!(tuples, converted_tuples);
    println!("✅ Round-trip conversion successful!");

    // Single column example
    println!("\n📊 Single column batch:");
    let values = vec![10, 20, 30, 40, 50];
    let single_batch = conversion::single_column_batch(values.clone(), "values")?;
    println!(
        "Created single-column RecordBatch with {} rows",
        single_batch.num_rows()
    );

    Ok(())
}

// Helper function to print query results
fn print_results(batches: &[RecordBatch]) {
    for batch in batches {
        println!(
            "{}",
            datafusion::arrow::util::pretty::pretty_format_batches(&[batch.clone()]).unwrap()
        );
    }
}

// Helper trait for unzipping 4-tuples
trait Unzip4<A, B, C, D> {
    fn unzip4(self) -> (Vec<A>, Vec<B>, Vec<C>, Vec<D>);
}

impl<A, B, C, D> Unzip4<A, B, C, D> for Vec<(A, B, C, D)> {
    fn unzip4(self) -> (Vec<A>, Vec<B>, Vec<C>, Vec<D>) {
        let mut a = Vec::new();
        let mut b = Vec::new();
        let mut c = Vec::new();
        let mut d = Vec::new();

        for (ai, bi, ci, di) in self {
            a.push(ai);
            b.push(bi);
            c.push(ci);
            d.push(di);
        }

        (a, b, c, d)
    }
}
