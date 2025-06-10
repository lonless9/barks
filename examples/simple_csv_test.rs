//! Simple CSV Test
//!
//! A minimal test to verify CSV connector functionality

use barks_sql_api::traits::{DataFusionSqlContext, SqlContext};
use barks_sql_core::SqlConfig;
use std::io::Write;
use tempfile::NamedTempFile;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 Simple CSV Test");
    println!("==================");

    // Create a simple CSV file
    let mut csv_file = NamedTempFile::new()?;
    writeln!(csv_file, "id,name")?;
    writeln!(csv_file, "1,Alice")?;
    writeln!(csv_file, "2,Bob")?;
    csv_file.flush()?;

    let csv_path = csv_file.path().to_str().unwrap();
    println!("Created CSV file: {}", csv_path);

    // Create SQL context
    let config = SqlConfig::default();
    let sql_context = DataFusionSqlContext::new(config);

    // Register CSV table
    sql_context.register_csv_table("people", csv_path).await?;
    println!("Registered CSV table 'people'");

    // Execute a simple query
    let results = sql_context.sql("SELECT * FROM people").await?;
    println!("Query executed successfully!");

    // Print results
    for (i, batch) in results.iter().enumerate() {
        println!(
            "Batch {}: {} rows, {} columns",
            i,
            batch.num_rows(),
            batch.num_columns()
        );
    }

    println!("✅ CSV connector test completed successfully!");
    Ok(())
}
