use arrow_array::RecordBatch;
use geoparquet::writer::{
    GeoParquetRecordBatchEncoder, GeoParquetWriterEncoding, GeoParquetWriterOptionsBuilder,
};
use parquet::arrow::ArrowWriter;
use std::fs::File;
use std::path::Path;

use crate::error::InfraHexError;

/// Write a RecordBatch to GeoParquet with EPSG:27700 CRS
pub fn write_geoparquet(batch: &RecordBatch, path: impl AsRef<Path>) -> Result<(), InfraHexError> {
    let schema = batch.schema();

    let options = GeoParquetWriterOptionsBuilder::default()
        .set_encoding(GeoParquetWriterEncoding::WKB)
        .build();

    let mut encoder = GeoParquetRecordBatchEncoder::try_new(&schema, &options)
        .map_err(|e| InfraHexError::Geometry(e.to_string()))?;

    let file = File::create(path).map_err(|e| InfraHexError::Geometry(e.to_string()))?;
    let mut writer = ArrowWriter::try_new(file, encoder.target_schema(), None)
        .map_err(|e| InfraHexError::Geometry(e.to_string()))?;

    let encoded_batch = encoder
        .encode_record_batch(batch)
        .map_err(|e| InfraHexError::Geometry(e.to_string()))?;

    writer
        .write(&encoded_batch)
        .map_err(|e| InfraHexError::Geometry(e.to_string()))?;

    let kv_metadata = encoder
        .into_keyvalue()
        .map_err(|e| InfraHexError::Geometry(e.to_string()))?;

    writer.append_key_value_metadata(kv_metadata);
    writer
        .finish()
        .map_err(|e| InfraHexError::Geometry(e.to_string()))?;

    Ok(())
}
