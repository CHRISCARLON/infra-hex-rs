use arrow_array::{StringArray, UInt32Array};
use infra_hex_rs::{
    BBox, CadentClient, InfraClient, InfraHexError, to_hex_summary, write_geoparquet
};

#[tokio::main]
async fn main() -> Result<(), InfraHexError> {
    let client = CadentClient::new()?;

    // North London - split into 3 horizontal bands
    let bboxes = [
        BBox::new(51.54, -0.20, 51.58, 0.00),  // South
        BBox::new(51.58, -0.20, 51.62, 0.00),  // Central
        BBox::new(51.62, -0.20, 51.66, 0.00),  // North
    ];

    println!("Fetching pipelines for North London (3 tiles)...");
    let mut pipelines = Vec::new();
    let mut total_errors = 0;

    for (i, bbox) in bboxes.iter().enumerate() {
        println!("  Fetching tile {}...", i + 1);
        let result = client.fetch_all_by_bbox(bbox).await;
        total_errors += result.errors.len();
        pipelines.extend(result.records);
    }

    if total_errors > 0 {
        eprintln!("Warning: {} fetch errors occurred", total_errors);
    }
    println!("Got {} pipelines", pipelines.len());

    println!("Computing hex summary...");
    let summary = to_hex_summary(&pipelines, 10)?;

    // Print Arrow stats
    println!("\n=== Arrow RecordBatch Stats ===");
    println!("Rows: {}", summary.num_rows());
    println!("Columns: {}", summary.num_columns());
    for (i, field) in summary.schema().fields().iter().enumerate() {
        let col = summary.column(i);
        println!(
            "  {}: {} (nulls: {})",
            field.name(),
            field.data_type(),
            col.null_count()
        );
    }

    let mem_size: usize = summary.columns().iter().map(|c| c.get_array_memory_size()).sum();
    println!("Memory size: {:.2} KB", mem_size as f64 / 1024.0);

    if summary.num_rows() > 0 {
        let hex_ids = summary
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let pipe_counts = summary
            .column(1)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap();
        println!(
            "\nTop hex: {} with {} pipes",
            hex_ids.value(0),
            pipe_counts.value(0)
        );
    }

    write_geoparquet(&summary, "north_london_hex.parquet")?;
    println!("\nWrote north_london_hex.parquet");

    Ok(())
}
