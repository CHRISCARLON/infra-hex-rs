use arrow_array::builder::ListBuilder;
use arrow_array::builder::StringBuilder;
use arrow_array::{RecordBatch, StringArray, UInt32Array};
use arrow_schema::{DataType, Field, Schema};
use geo_types::{MultiPolygon, Polygon};
use geoarrow_array::IntoArrow;
use geoarrow_array::array::{MultiPolygonArray, PolygonArray};
use geoarrow_array::builder::{MultiPolygonBuilder, PolygonBuilder};
use geoarrow_schema::{Crs, Dimension, Metadata, MultiPolygonType, PolygonType};
use n3gb_rs::{HexCell, HexGrid};
use rayon::prelude::*;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::client::PipelineData;
use crate::error::InfraHexError;

use super::hex::get_hex_cells;

// =============================================================================
// Boundary Filter Trait
// =============================================================================

/// Trait for types that can filter hex cells to a geographic boundary.
///
/// This is typically used to further restrict data that was first selected
/// using a bounding box (bbox). Implementors return a set of hex cell IDs
/// that intersect the boundary geometry at a given zoom level. Downstream
/// code can then use these IDs to filter associated pipeline or asset data
/// to only those cells that fall within the boundary.
pub trait BoundaryFilter {
    /// Returns the set of valid hex cell IDs within this boundary at the given
    /// `zoom` level, or `None` to indicate that no boundary-based filtering
    /// should be applied.
    ///
    /// When `Some(HashSet<String>)` is returned, each string is the ID of a
    /// hex cell that intersects the boundary. Consumers can match these IDs
    /// against the hex IDs used in their data model and discard any records
    /// whose hex IDs are not in this set - use Intersect for this.
    fn valid_cell_ids(&self, zoom: u8) -> Result<Option<HashSet<String>>, InfraHexError>;
}

/// No boundary filtering - include all the hex cells.
///
/// This is effectively a "pass-through" implementation: it signals to
/// downstream code that every hex cell from the bbox (or other upstream
/// source) should be considered valid.
impl BoundaryFilter for () {
    fn valid_cell_ids(&self, _zoom: u8) -> Result<Option<HashSet<String>>, InfraHexError> {
        // `None` means "no filtering": keep all cells.
        Ok(None)
    }
}

/// Filter hex cells intersecting a polygon boundary.
///
/// Constructs a `HexGrid` from the provided `Polygon` at the specified
/// `zoom` level and returns the IDs of all hex cells whose geometry
/// intersects the polygon. These IDs can then be used to filter
/// pipeline or asset records to only those within the polygon.
impl BoundaryFilter for Polygon<f64> {
    fn valid_cell_ids(&self, zoom: u8) -> Result<Option<HashSet<String>>, InfraHexError> {
        let grid = HexGrid::from_wgs84_polygon(self, zoom)?;
        let ids: HashSet<String> = grid.cells().iter().map(|c| c.id.clone()).collect();
        Ok(Some(ids))
    }
}

/// Filter hex cells intersecting a multipolygon boundary.
///
/// Similar to the `Polygon` implementation, but supports complex
/// geometries composed of multiple polygons. All hex cells that
/// intersect any polygon in the `MultiPolygon` at the given `zoom`
/// level are included in the returned ID set.
impl BoundaryFilter for MultiPolygon<f64> {
    fn valid_cell_ids(&self, zoom: u8) -> Result<Option<HashSet<String>>, InfraHexError> {
        let grid = HexGrid::from_wgs84_multipolygon(self, zoom)?;
        let ids: HashSet<String> = grid.cells().iter().map(|c| c.id.clone()).collect();
        Ok(Some(ids))
    }
}

// =============================================================================
// Helper Functions
// =============================================================================

/// CRS object
fn bng_metadata() -> Arc<Metadata> {
    let crs = Crs::from_authority_code("EPSG:27700".to_string());
    Arc::new(Metadata::new(crs, None))
}

/// Extracts hex cells for each pipeline, optionally filtering by boundary.
/// If `valid_ids` is:
/// - `Some(set)`: only hex cells whose IDs are in `set` are kept for each pipeline.
/// - `None`: no boundary filtering is applied and all computed cells are returned.
/// - This is the boundary filter logic in practice.
fn extract_cells_per_pipeline<T: PipelineData>(
    records: &[T],
    zoom: u8,
    valid_ids: &Option<HashSet<String>>,
) -> Result<Vec<Vec<HexCell>>, InfraHexError> {
    let cells_per_pipe: Result<Vec<Vec<HexCell>>, InfraHexError> = records
        .par_iter()
        .map(|record| get_hex_cells(record, zoom))
        .collect();

    let cells_per_pipe = cells_per_pipe?;

    match valid_ids {
        Some(valid) => Ok(cells_per_pipe
            .into_iter()
            .map(|cells| {
                cells
                    .into_iter()
                    .filter(|c| valid.contains(&c.id))
                    .collect()
            })
            .collect()),
        None => Ok(cells_per_pipe),
    }
}

/// Builds the pipeline attribute arrays (asset_id, pipe_type, material, pressure).
fn build_pipeline_attributes<T: PipelineData>(
    records: &[T],
) -> (StringArray, StringArray, StringArray, StringArray) {
    let asset_ids: StringArray = records.iter().map(|r| r.asset_id()).collect();
    let pipe_types: StringArray = records.iter().map(|r| r.pipe_type()).collect();
    let materials: StringArray = records.iter().map(|r| r.material()).collect();
    let pressures: StringArray = records.iter().map(|r| r.pressure()).collect();
    (asset_ids, pipe_types, materials, pressures)
}

/// Builds a List<Utf8> array of hex IDs for each pipeline.
fn build_hex_ids_list(cells_per_pipe: &[Vec<HexCell>]) -> arrow_array::ListArray {
    let mut list_builder = ListBuilder::new(StringBuilder::new());
    for cells in cells_per_pipe {
        let values = list_builder.values();
        for cell in cells {
            values.append_value(&cell.id);
        }
        list_builder.append(true);
    }
    list_builder.finish()
}

/// Builds a MultiPolygon geometry array from cells per pipeline.
fn build_multipolygon_geometry(cells_per_pipe: &[Vec<HexCell>]) -> (MultiPolygonArray, Field) {
    let multi_polygons: Vec<MultiPolygon<f64>> = cells_per_pipe
        .iter()
        .map(|cells| {
            let polygons: Vec<_> = cells.iter().map(|c| c.to_polygon()).collect();
            MultiPolygon::new(polygons)
        })
        .collect();

    let mp_type = MultiPolygonType::new(Dimension::XY, bng_metadata());
    let geometry_array =
        MultiPolygonBuilder::from_multi_polygons(&multi_polygons, mp_type).finish();
    let geometry_field = geometry_array.extension_type().to_field("geometry", false);
    (geometry_array, geometry_field)
}

/// Builds a Polygon geometry array from a list of hex cells.
fn build_polygon_geometry(cells: &[&HexCell]) -> (PolygonArray, Field) {
    let polygons: Vec<_> = cells.iter().map(|c| c.to_polygon()).collect();
    let poly_type = PolygonType::new(Dimension::XY, bng_metadata());
    let geometry_array = PolygonBuilder::from_polygons(&polygons, poly_type).finish();
    let geometry_field = geometry_array.extension_type().to_field("geometry", false);
    (geometry_array, geometry_field)
}

/// Aggregates hex cells across pipelines, counting unique cells per pipeline.
/// Returns sorted (by count descending) vec of (hex_id, count) and a map of id -> HexCell.
fn aggregate_hex_counts(
    cells_per_pipe: Vec<Vec<HexCell>>,
) -> (Vec<(String, usize)>, HashMap<String, HexCell>) {
    let mut counts: HashMap<String, usize> = HashMap::new();
    let mut cells_map: HashMap<String, HexCell> = HashMap::new();

    for cells in cells_per_pipe {
        let mut seen_in_pipe = HashSet::new();
        for cell in cells {
            if seen_in_pipe.insert(cell.id.clone()) {
                *counts.entry(cell.id.clone()).or_insert(0) += 1;
                cells_map.entry(cell.id.clone()).or_insert(cell);
            }
        }
    }

    let mut sorted: Vec<_> = counts.into_iter().collect();
    sorted.sort_by(|a, b| b.1.cmp(&a.1));

    (sorted, cells_map)
}

// =============================================================================
// Record Batch Functions (one row per pipeline)
// =============================================================================

fn to_record_batch_impl<T: PipelineData, F: BoundaryFilter>(
    records: &[T],
    zoom: u8,
    filter: &F,
    include_geom: bool,
) -> Result<RecordBatch, InfraHexError> {
    let valid_ids = filter.valid_cell_ids(zoom)?;
    let cells_per_pipe = extract_cells_per_pipeline(records, zoom, &valid_ids)?;

    let (asset_ids, pipe_types, materials, pressures) = build_pipeline_attributes(records);
    let hex_ids_list = build_hex_ids_list(&cells_per_pipe);

    let base_fields = vec![
        Field::new("asset_id", DataType::Utf8, true),
        Field::new("pipe_type", DataType::Utf8, true),
        Field::new("material", DataType::Utf8, true),
        Field::new("pressure", DataType::Utf8, true),
        Field::new(
            "hex_ids",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            false,
        ),
    ];

    let base_columns: Vec<Arc<dyn arrow_array::Array>> = vec![
        Arc::new(asset_ids),
        Arc::new(pipe_types),
        Arc::new(materials),
        Arc::new(pressures),
        Arc::new(hex_ids_list),
    ];

    if include_geom {
        let (geometry_array, geometry_field) = build_multipolygon_geometry(&cells_per_pipe);
        let mut fields = base_fields;
        fields.push(geometry_field);
        let mut columns = base_columns;
        columns.push(Arc::new(geometry_array.into_arrow()));

        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
            .map_err(|e| InfraHexError::Geometry(e.to_string()))
    } else {
        RecordBatch::try_new(Arc::new(Schema::new(base_fields)), base_columns)
            .map_err(|e| InfraHexError::Geometry(e.to_string()))
    }
}

// -----------------------------------------------------------------------------
// Public Record Batch API
// -----------------------------------------------------------------------------

pub fn to_record_batch_no_geom<T: PipelineData>(
    records: &[T],
    zoom: u8,
) -> Result<RecordBatch, InfraHexError> {
    to_record_batch_impl(records, zoom, &(), false)
}

pub fn to_record_batch<T: PipelineData>(
    records: &[T],
    zoom: u8,
) -> Result<RecordBatch, InfraHexError> {
    to_record_batch_impl(records, zoom, &(), true)
}

pub fn to_record_batch_for_polygon_no_geom<T: PipelineData>(
    records: &[T],
    zoom: u8,
    polygon: &Polygon<f64>,
) -> Result<RecordBatch, InfraHexError> {
    to_record_batch_impl(records, zoom, polygon, false)
}

pub fn to_record_batch_for_polygon<T: PipelineData>(
    records: &[T],
    zoom: u8,
    polygon: &Polygon<f64>,
) -> Result<RecordBatch, InfraHexError> {
    to_record_batch_impl(records, zoom, polygon, true)
}

pub fn to_record_batch_for_multipolygon_no_geom<T: PipelineData>(
    records: &[T],
    zoom: u8,
    multipolygon: &MultiPolygon<f64>,
) -> Result<RecordBatch, InfraHexError> {
    to_record_batch_impl(records, zoom, multipolygon, false)
}

pub fn to_record_batch_for_multipolygon<T: PipelineData>(
    records: &[T],
    zoom: u8,
    multipolygon: &MultiPolygon<f64>,
) -> Result<RecordBatch, InfraHexError> {
    to_record_batch_impl(records, zoom, multipolygon, true)
}

// =============================================================================
// Hex Summary Functions (one row per hex cell, aggregated counts)
// =============================================================================

fn to_hex_summary_impl<T: PipelineData, F: BoundaryFilter>(
    records: &[T],
    zoom: u8,
    filter: &F,
    include_geom: bool,
) -> Result<RecordBatch, InfraHexError> {
    let valid_ids = filter.valid_cell_ids(zoom)?;
    let cells_per_pipe = extract_cells_per_pipeline(records, zoom, &valid_ids)?;

    let (sorted, cells_map) = aggregate_hex_counts(cells_per_pipe);

    let hex_ids: StringArray = sorted.iter().map(|(id, _)| Some(id.as_str())).collect();
    let pipe_counts: UInt32Array = sorted.iter().map(|(_, c)| Some(*c as u32)).collect();

    let base_fields = vec![
        Field::new("hex_id", DataType::Utf8, false),
        Field::new("pipe_count", DataType::UInt32, false),
    ];

    let base_columns: Vec<Arc<dyn arrow_array::Array>> =
        vec![Arc::new(hex_ids), Arc::new(pipe_counts)];

    if include_geom {
        let cells: Vec<&HexCell> = sorted
            .iter()
            .map(|(id, _)| cells_map.get(id).unwrap())
            .collect();

        let (geometry_array, geometry_field) = build_polygon_geometry(&cells);
        let mut fields = base_fields;
        fields.push(geometry_field);
        let mut columns = base_columns;
        columns.push(Arc::new(geometry_array.into_arrow()));

        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
            .map_err(|e| InfraHexError::Geometry(e.to_string()))
    } else {
        RecordBatch::try_new(Arc::new(Schema::new(base_fields)), base_columns)
            .map_err(|e| InfraHexError::Geometry(e.to_string()))
    }
}

// -----------------------------------------------------------------------------
// Public Hex Summary API
// -----------------------------------------------------------------------------

pub fn to_hex_summary_no_geom<T: PipelineData>(
    records: &[T],
    zoom: u8,
) -> Result<RecordBatch, InfraHexError> {
    to_hex_summary_impl(records, zoom, &(), false)
}

pub fn to_hex_summary<T: PipelineData>(
    records: &[T],
    zoom: u8,
) -> Result<RecordBatch, InfraHexError> {
    to_hex_summary_impl(records, zoom, &(), true)
}

pub fn to_hex_summary_for_polygon_no_geom<T: PipelineData>(
    records: &[T],
    zoom: u8,
    polygon: &Polygon<f64>,
) -> Result<RecordBatch, InfraHexError> {
    to_hex_summary_impl(records, zoom, polygon, false)
}

pub fn to_hex_summary_for_polygon<T: PipelineData>(
    records: &[T],
    zoom: u8,
    polygon: &Polygon<f64>,
) -> Result<RecordBatch, InfraHexError> {
    to_hex_summary_impl(records, zoom, polygon, true)
}

pub fn to_hex_summary_for_multipolygon_no_geom<T: PipelineData>(
    records: &[T],
    zoom: u8,
    multipolygon: &MultiPolygon<f64>,
) -> Result<RecordBatch, InfraHexError> {
    to_hex_summary_impl(records, zoom, multipolygon, false)
}

pub fn to_hex_summary_for_multipolygon<T: PipelineData>(
    records: &[T],
    zoom: u8,
    multipolygon: &MultiPolygon<f64>,
) -> Result<RecordBatch, InfraHexError> {
    to_hex_summary_impl(records, zoom, multipolygon, true)
}
