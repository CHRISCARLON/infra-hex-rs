use geo_types::LineString;
use n3gb_rs::HexCell;

use crate::client::PipelineData;
use crate::error::InfraHexError;

use super::geometry::FromGeoJson;

/// Extract hex cells from any pipeline record that implements PipelineData.
/// This works with pipeline linestrings from different infrastructure clients.
/// This currenty assumes the data will be in wgs84 for NUAR client needs to be BNG too
/// TODO: Add flag for CRS system that tiggers correct method
/// let cells = HexCell::from_line_string_bng(&line, zoom)?;
pub fn get_hex_cells<T: PipelineData>(record: &T, zoom: u8) -> Result<Vec<HexCell>, InfraHexError> {
    let geometry = record
        .geo_shape()
        .geometry
        .as_ref()
        .ok_or_else(|| InfraHexError::Geometry("Feature has no geometry".to_string()))?;

    let line = LineString::from_geojson(geometry)?;
    let cells = HexCell::from_line_string_wgs84(&line, zoom)?;
    Ok(cells)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::{CadentPipelineRecord, GeoPoint2d};
    use geojson::{Feature, Geometry, Value};

    fn make_test_record() -> CadentPipelineRecord {
        let geom = Geometry::new(Value::LineString(vec![
            vec![-2.248423716278411, 53.4804537960769],
            vec![-2.248817614533952, 53.480510340167925],
            vec![-2.249255070278722, 53.480573578320396],
            vec![-2.249632113002486, 53.48061535991179],
            vec![-2.250244759514899, 53.48066909573824],
        ]));

        CadentPipelineRecord {
            geo_point_2d: GeoPoint2d {
                lon: -2.248,
                lat: 53.480,
            },
            geo_shape: Feature {
                geometry: Some(geom),
                ..Default::default()
            },
            pipe_type: Some("MP".to_string()),
            pressure: None,
            material: None,
            diameter: None,
            diam_unit: None,
            carr_mat: None,
            carr_dia: None,
            carr_di_un: None,
            asset_id: Some("TEST-001".to_string()),
            depth: None,
            ag_ind: None,
            inst_date: None,
        }
    }

    #[test]
    fn test_get_hex_cells() {
        let record = make_test_record();
        let cells = get_hex_cells(&record, 12).unwrap();

        assert!(!cells.is_empty());
        for cell in &cells {
            println!("{}", cell.id);
        }
    }
}
