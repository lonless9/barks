//! Demonstration of the fixed (String, String) RDD operations
//!
//! This example shows that the (String, String) RddDataType implementation
//! now works correctly with proper type-safe operations instead of the
//! previous placeholder implementation.

use barks_core::operations::{
    ConcatStringTupleOperation, FirstContainsSecondPredicate, SplitAndCombineOperation,
    SwapStringTupleOperation,
};
use barks_core::rdd::DistributedRdd;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🎯 Demonstrating Fixed (String, String) RDD Operations");
    println!("=====================================================\n");

    // Create test data with (String, String) tuples
    let data = vec![
        ("hello world".to_string(), "world".to_string()),
        ("foo bar baz".to_string(), "bar".to_string()),
        ("rust programming".to_string(), "python".to_string()),
        ("apache spark".to_string(), "spark".to_string()),
    ];

    println!("📊 Original data:");
    for (i, item) in data.iter().enumerate() {
        println!("  {}: ({:?}, {:?})", i + 1, item.0, item.1);
    }
    println!();

    // Create RDD from the data
    let rdd: DistributedRdd<(String, String)> =
        DistributedRdd::from_vec_with_partitions(data.clone(), 2);

    // Test 1: Swap operation
    println!("🔄 Test 1: Swap Operation");
    let swapped_rdd = rdd.clone().map(Box::new(SwapStringTupleOperation));
    let swapped_result = swapped_rdd.collect()?;
    println!("  Result after swapping:");
    for (i, item) in swapped_result.iter().enumerate() {
        println!("    {}: ({:?}, {:?})", i + 1, item.0, item.1);
    }
    println!();

    // Test 2: Concatenation operation
    println!("🔗 Test 2: Concatenation Operation");
    let concat_rdd = rdd.clone().map(Box::new(ConcatStringTupleOperation {
        separator: " -> ".to_string(),
    }));
    let concat_result = concat_rdd.collect()?;
    println!("  Result after concatenation:");
    for (i, item) in concat_result.iter().enumerate() {
        println!("    {}: ({:?}, {:?})", i + 1, item.0, item.1);
    }
    println!();

    // Test 3: Filter operation
    println!("🔍 Test 3: Filter Operation (first contains second)");
    let filtered_rdd = rdd.clone().filter(Box::new(FirstContainsSecondPredicate));
    let filtered_result = filtered_rdd.collect()?;
    println!("  Result after filtering:");
    for (i, item) in filtered_result.iter().enumerate() {
        println!("    {}: ({:?}, {:?})", i + 1, item.0, item.1);
    }
    println!();

    // Test 4: FlatMap operation
    println!("🌟 Test 4: FlatMap Operation (split and combine)");
    let test_data = vec![("hello world".to_string(), "foo bar".to_string())];
    let flatmap_rdd: DistributedRdd<(String, String)> = DistributedRdd::from_vec(test_data);
    let flatmapped_rdd = flatmap_rdd.flat_map(Box::new(SplitAndCombineOperation));
    let flatmap_result = flatmapped_rdd.collect()?;
    println!("  Input: (\"hello world\", \"foo bar\")");
    println!("  Result after flatMap:");
    for (i, item) in flatmap_result.iter().enumerate() {
        println!("    {}: ({:?}, {:?})", i + 1, item.0, item.1);
    }
    println!();

    // Test 5: Chained operations
    println!("⛓️  Test 5: Chained Operations");
    let chained_rdd = rdd
        .filter(Box::new(FirstContainsSecondPredicate)) // Filter where first contains second
        .map(Box::new(SwapStringTupleOperation)); // Then swap them

    let chained_result = chained_rdd.collect()?;
    println!("  Result after filter + swap:");
    for (i, item) in chained_result.iter().enumerate() {
        println!("    {}: ({:?}, {:?})", i + 1, item.0, item.1);
    }
    println!();

    println!("✅ All operations completed successfully!");
    println!("🎉 The (String, String) RddDataType implementation is now fully functional!");

    Ok(())
}
