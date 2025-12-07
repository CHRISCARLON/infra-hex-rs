use geo_types::{Coord, LineString};
use geojson::{Geometry, Value};

use crate::error::InfraHexError;

pub fn extract_line_string(geometry: &Geometry) -> Result<LineString<f64>, InfraHexError> {
    match &geometry.value {
        Value::LineString(coords) => {
            let points: Vec<Coord<f64>> =
                coords.iter().map(|c| Coord { x: c[0], y: c[1] }).collect();
            Ok(LineString::new(points))
        }
        Value::MultiLineString(lines) => {
            let points: Vec<Coord<f64>> = lines
                .iter()
                .flat_map(|line| line.iter().map(|c| Coord { x: c[0], y: c[1] }))
                .collect();
            Ok(LineString::new(points))
        }
        other => Err(InfraHexError::Geometry(format!(
            "Expected LineString or MultiLineString, got {:?}",
            other
        ))),
    }
}
