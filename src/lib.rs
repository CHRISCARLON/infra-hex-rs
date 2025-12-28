pub mod client;
pub mod core;
pub mod error;

pub use client::{
    ApiResponse, BBox, BuiltUpArea, BuiltUpAreaClient, CadentClient, GeoPoint2d, InfraClient,
    InfraResult, PipelineRecord, polygon_to_geojson,
};
pub use core::{
    get_hex_cells, to_hex_summary, to_hex_summary_for_multipolygon,
    to_hex_summary_for_multipolygon_no_geom, to_hex_summary_for_polygon,
    to_hex_summary_for_polygon_no_geom, to_hex_summary_no_geom, to_record_batch,
    to_record_batch_for_multipolygon, to_record_batch_for_multipolygon_no_geom,
    to_record_batch_for_polygon, to_record_batch_for_polygon_no_geom, to_record_batch_no_geom,
    write_geoparquet, FromGeoJson, ToGeoJson,
};
pub use error::InfraHexError;

pub use n3gb_rs::{HexCell, HexCellsToArrow, HexGrid};

#[cfg(test)]
mod tests {
    use arrow_array::{StringArray, UInt32Array};
    use geo::BoundingRect;

    use crate::{
        BBox, BuiltUpAreaClient, CadentClient, InfraClient, InfraHexError,
        to_hex_summary_for_multipolygon, write_geoparquet,
    };

    #[tokio::test]
    #[ignore = "requires network access and CADENT_API_KEY"]
    async fn test_manchester_pipeline() -> Result<(), InfraHexError> {
        // Fetch Manchester Built-Up Area boundary (OBJECTID 1310)
        println!("Fetching Manchester BUA boundary...");
        let bua_client = BuiltUpAreaClient::new();
        let manchester = bua_client.fetch_by_object_id(1310).await?;
        println!(
            "Got {} ({}) with {} polygons",
            manchester.name,
            manchester.code,
            manchester.geometry.0.len()
        );

        // Compute bounding box from Manchester geometry
        let bounds = manchester
            .geometry
            .bounding_rect()
            .ok_or_else(|| InfraHexError::Geometry("Could not compute bounding rect".into()))?;

        let bbox = BBox::new(
            bounds.min().y,
            bounds.min().x,
            bounds.max().y,
            bounds.max().x,
        );
        println!(
            "Bounding box: ({:.4}, {:.4}) to ({:.4}, {:.4})",
            bbox.min_lat, bbox.min_lon, bbox.max_lat, bbox.max_lon
        );

        // Fetch pipelines within Manchester's bounding box
        println!("Fetching pipelines within Manchester bbox...");
        let cadent_client = CadentClient::new()?;
        let result = cadent_client.fetch_all_by_bbox(&bbox).await;

        if !result.errors.is_empty() {
            eprintln!("Warning: {} fetch errors occurred", result.errors.len());
        }
        println!("Got {} pipelines", result.records.len());

        // Compute hex summary filtered to Manchester boundary at zoom 8
        println!("Computing hex summary at zoom 8...");
        let summary = to_hex_summary_for_multipolygon(&result.records, 8, &manchester.geometry)?;

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

        let mem_size: usize = summary
            .columns()
            .iter()
            .map(|c| c.get_array_memory_size())
            .sum();
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

        write_geoparquet(&summary, "manchester_hex.parquet")?;
        println!("\nWrote manchester_hex.parquet");

        Ok(())
    }
}
