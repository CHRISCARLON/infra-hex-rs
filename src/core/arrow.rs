use arrow_array::builder::ListBuilder;
use arrow_array::builder::StringBuilder;
use arrow_array::{RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use geo_types::MultiPolygon;
use geoarrow_array::IntoArrow;
use geoarrow_array::builder::MultiPolygonBuilder;
use geoarrow_array::builder::PolygonBuilder;
use geoarrow_schema::PolygonType;
use geoarrow_schema::{Crs, Dimension, Metadata, MultiPolygonType};
use n3gb_rs::HexCell;
use rayon::prelude::*;
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;

use crate::client::PipelineRecord;
use crate::error::InfraHexError;

use super::hex::get_hex_cells;

fn bng_metadata() -> Arc<Metadata> {
    let crs = Crs::from_projjson(json!({
        "type": "ProjectedCRS",
        "name": "OSGB 1936 / British National Grid",
        "id": {"authority": "EPSG", "code": 27700}
    }));
    Arc::new(Metadata::new(crs, None))
}

pub fn to_record_batch_no_geom(
    records: &[PipelineRecord],
    zoom: u8,
) -> Result<RecordBatch, InfraHexError> {
    let cells_per_pipe: Result<Vec<Vec<HexCell>>, InfraHexError> = records
        .par_iter()
        .map(|record| get_hex_cells(record, zoom))
        .collect();
    let cells_per_pipe = cells_per_pipe?;

    let asset_ids: StringArray = records.iter().map(|r| r.asset_id.as_deref()).collect();
    let pipe_types: StringArray = records.iter().map(|r| r.pipe_type.as_deref()).collect();
    let materials: StringArray = records.iter().map(|r| r.material.as_deref()).collect();
    let pressures: StringArray = records.iter().map(|r| r.pressure.as_deref()).collect();

    let mut list_builder = ListBuilder::new(StringBuilder::new());

    for cells in &cells_per_pipe {
        let values = list_builder.values();
        for cell in cells {
            values.append_value(&cell.id);
        }
        list_builder.append(true);
    }
    let hex_ids_list = list_builder.finish();

    let schema = Schema::new(vec![
        Field::new("asset_id", DataType::Utf8, true),
        Field::new("pipe_type", DataType::Utf8, true),
        Field::new("material", DataType::Utf8, true),
        Field::new("pressure", DataType::Utf8, true),
        Field::new(
            "hex_ids",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            false,
        ),
    ]);

    RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(asset_ids),
            Arc::new(pipe_types),
            Arc::new(materials),
            Arc::new(pressures),
            Arc::new(hex_ids_list),
        ],
    )
    .map_err(|e| InfraHexError::Geometry(e.to_string()))
}

pub fn to_record_batch(records: &[PipelineRecord], zoom: u8) -> Result<RecordBatch, InfraHexError> {
    let cells_per_pipe: Result<Vec<Vec<HexCell>>, InfraHexError> = records
        .par_iter()
        .map(|record| get_hex_cells(record, zoom))
        .collect();
    let cells_per_pipe = cells_per_pipe?;

    let asset_ids: StringArray = records.iter().map(|r| r.asset_id.as_deref()).collect();
    let pipe_types: StringArray = records.iter().map(|r| r.pipe_type.as_deref()).collect();
    let materials: StringArray = records.iter().map(|r| r.material.as_deref()).collect();
    let pressures: StringArray = records.iter().map(|r| r.pressure.as_deref()).collect();

    let mut list_builder = ListBuilder::new(StringBuilder::new());
    for cells in &cells_per_pipe {
        let values = list_builder.values();
        for cell in cells {
            values.append_value(&cell.id);
        }
        list_builder.append(true);
    }
    let hex_ids_list = list_builder.finish();

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

    let schema = Schema::new(vec![
        Field::new("asset_id", DataType::Utf8, true),
        Field::new("pipe_type", DataType::Utf8, true),
        Field::new("material", DataType::Utf8, true),
        Field::new("pressure", DataType::Utf8, true),
        Field::new(
            "hex_ids",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            false,
        ),
        geometry_field,
    ]);

    RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(asset_ids),
            Arc::new(pipe_types),
            Arc::new(materials),
            Arc::new(pressures),
            Arc::new(hex_ids_list),
            Arc::new(geometry_array.into_arrow()),
        ],
    )
    .map_err(|e| InfraHexError::Geometry(e.to_string()))
}

pub fn to_hex_summary_no_geom(
    records: &[PipelineRecord],
    zoom: u8,
) -> Result<RecordBatch, InfraHexError> {
    let cell_sets: Result<Vec<Vec<HexCell>>, InfraHexError> = records
        .par_iter()
        .map(|record| get_hex_cells(record, zoom))
        .collect();

    let mut counts: HashMap<String, usize> = HashMap::new();

    for cells in cell_sets? {
        let mut seen_in_pipe = std::collections::HashSet::new();
        for cell in cells {
            if seen_in_pipe.insert(cell.id.clone()) {
                *counts.entry(cell.id.clone()).or_insert(0) += 1;
            }
        }
    }

    let mut sorted: Vec<_> = counts.into_iter().collect();
    sorted.sort_by(|a, b| b.1.cmp(&a.1));

    let hex_ids: StringArray = sorted.iter().map(|(id, _)| Some(id.as_str())).collect();
    let pipe_counts: arrow_array::UInt32Array =
        sorted.iter().map(|(_, c)| Some(*c as u32)).collect();

    let schema = Schema::new(vec![
        Field::new("hex_id", DataType::Utf8, false),
        Field::new("pipe_count", DataType::UInt32, false),
    ]);

    RecordBatch::try_new(
        Arc::new(schema),
        vec![Arc::new(hex_ids), Arc::new(pipe_counts)],
    )
    .map_err(|e| InfraHexError::Geometry(e.to_string()))
}

pub fn to_hex_summary(records: &[PipelineRecord], zoom: u8) -> Result<RecordBatch, InfraHexError> {
    let cell_sets: Result<Vec<Vec<HexCell>>, InfraHexError> = records
        .par_iter()
        .map(|record| get_hex_cells(record, zoom))
        .collect();

    let mut counts: HashMap<String, usize> = HashMap::new();
    let mut cells_map: HashMap<String, HexCell> = HashMap::new();

    for cells in cell_sets? {
        let mut seen_in_pipe = std::collections::HashSet::new();
        for cell in cells {
            if seen_in_pipe.insert(cell.id.clone()) {
                *counts.entry(cell.id.clone()).or_insert(0) += 1;
                cells_map.entry(cell.id.clone()).or_insert(cell);
            }
        }
    }

    let mut sorted: Vec<_> = counts.into_iter().collect();
    sorted.sort_by(|a, b| b.1.cmp(&a.1));

    let hex_ids: StringArray = sorted.iter().map(|(id, _)| Some(id.as_str())).collect();
    let pipe_counts: arrow_array::UInt32Array =
        sorted.iter().map(|(_, c)| Some(*c as u32)).collect();

    let cells: Vec<&HexCell> = sorted
        .iter()
        .map(|(id, _)| cells_map.get(id).unwrap())
        .collect();

    let polygons: Vec<_> = cells.iter().map(|c| c.to_polygon()).collect();

    let poly_type = PolygonType::new(Dimension::XY, bng_metadata());
    let geometry_array = PolygonBuilder::from_polygons(&polygons, poly_type).finish();
    let geometry_field = geometry_array.extension_type().to_field("geometry", false);

    let schema = Schema::new(vec![
        Field::new("hex_id", DataType::Utf8, false),
        Field::new("pipe_count", DataType::UInt32, false),
        geometry_field,
    ]);

    RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(hex_ids),
            Arc::new(pipe_counts),
            Arc::new(geometry_array.into_arrow()),
        ],
    )
    .map_err(|e| InfraHexError::Geometry(e.to_string()))
}
