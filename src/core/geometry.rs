use geo_types::{Coord, LineString, MultiPolygon, Polygon};
use geojson::{Geometry as GeoJsonGeometry, Value as GeoJsonValue};

use crate::error::InfraHexError;

// =============================================================================
// ToGeoJson Trait - Convert geo_types to GeoJSON
// =============================================================================

/// Trait for converting geo_types geometries to GeoJSON.
pub trait ToGeoJson {
    /// Converts this geometry to a GeoJSON Geometry.
    fn to_geojson(&self) -> GeoJsonGeometry;
}

impl ToGeoJson for Polygon<f64> {
    fn to_geojson(&self) -> GeoJsonGeometry {
        let rings = polygon_to_rings(self);
        GeoJsonGeometry::new(GeoJsonValue::Polygon(rings))
    }
}

impl ToGeoJson for MultiPolygon<f64> {
    fn to_geojson(&self) -> GeoJsonGeometry {
        let coords: Vec<Vec<Vec<Vec<f64>>>> = self.0.iter().map(polygon_to_rings).collect();
        GeoJsonGeometry::new(GeoJsonValue::MultiPolygon(coords))
    }
}

/// Helper to convert a polygon's rings to GeoJSON coordinate format.
fn polygon_to_rings(polygon: &Polygon<f64>) -> Vec<Vec<Vec<f64>>> {
    let exterior: Vec<Vec<f64>> = polygon
        .exterior()
        .coords()
        .map(|c| vec![c.x, c.y])
        .collect();

    let mut rings = vec![exterior];

    for interior in polygon.interiors() {
        let ring: Vec<Vec<f64>> = interior.coords().map(|c| vec![c.x, c.y]).collect();
        rings.push(ring);
    }

    rings
}

// =============================================================================
// FromGeoJson Trait - Convert GeoJSON to geo_types
// =============================================================================

/// Trait for parsing GeoJSON geometries into geo_types.
pub trait FromGeoJson: Sized {
    /// The GeoJSON value types this type can be parsed from.
    fn from_geojson(geometry: &GeoJsonGeometry) -> Result<Self, InfraHexError>;
}

impl FromGeoJson for LineString<f64> {
    fn from_geojson(geometry: &GeoJsonGeometry) -> Result<Self, InfraHexError> {
        match &geometry.value {
            GeoJsonValue::LineString(coords) => Ok(coords_to_linestring(coords)),
            GeoJsonValue::MultiLineString(lines) => {
                // Flatten all lines into a single LineString
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
}

impl FromGeoJson for Polygon<f64> {
    fn from_geojson(geometry: &GeoJsonGeometry) -> Result<Self, InfraHexError> {
        match &geometry.value {
            GeoJsonValue::Polygon(rings) => rings_to_polygon(rings),
            other => Err(InfraHexError::Geometry(format!(
                "Expected Polygon, got {:?}",
                other
            ))),
        }
    }
}

impl FromGeoJson for MultiPolygon<f64> {
    fn from_geojson(geometry: &GeoJsonGeometry) -> Result<Self, InfraHexError> {
        match &geometry.value {
            GeoJsonValue::Polygon(rings) => {
                let polygon = rings_to_polygon(rings)?;
                Ok(MultiPolygon::new(vec![polygon]))
            }
            GeoJsonValue::MultiPolygon(polygons) => {
                let mut result = Vec::with_capacity(polygons.len());
                for rings in polygons {
                    result.push(rings_to_polygon(rings)?);
                }
                Ok(MultiPolygon::new(result))
            }
            other => Err(InfraHexError::Geometry(format!(
                "Expected Polygon or MultiPolygon, got {:?}",
                other
            ))),
        }
    }
}

// =============================================================================
// Helper Functions
// =============================================================================

/// Converts GeoJSON coordinate array to a LineString.
fn coords_to_linestring(coords: &[Vec<f64>]) -> LineString<f64> {
    let points: Vec<Coord<f64>> = coords
        .iter()
        .filter_map(|c| {
            if c.len() >= 2 {
                Some(Coord { x: c[0], y: c[1] })
            } else {
                None
            }
        })
        .collect();
    LineString::new(points)
}

/// Converts GeoJSON polygon rings to a geo_types Polygon.
fn rings_to_polygon(rings: &[Vec<Vec<f64>>]) -> Result<Polygon<f64>, InfraHexError> {
    if rings.is_empty() {
        return Err(InfraHexError::Geometry("No rings in polygon".to_string()));
    }

    let exterior = coords_to_linestring(&rings[0]);
    if exterior.0.is_empty() {
        return Err(InfraHexError::Geometry(
            "No valid coordinates in exterior ring".to_string(),
        ));
    }

    let holes: Vec<LineString<f64>> = rings[1..]
        .iter()
        .map(|ring| coords_to_linestring(ring))
        .filter(|ls| !ls.0.is_empty())
        .collect();

    Ok(Polygon::new(exterior, holes))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_linestring_from_geojson() {
        let geom = GeoJsonGeometry::new(GeoJsonValue::LineString(vec![
            vec![0.0, 0.0],
            vec![1.0, 1.0],
            vec![2.0, 0.0],
        ]));

        let ls = LineString::from_geojson(&geom).unwrap();
        assert_eq!(ls.0.len(), 3);
        assert_eq!(ls.0[0], Coord { x: 0.0, y: 0.0 });
    }

    #[test]
    fn test_multilinestring_to_linestring() {
        let geom = GeoJsonGeometry::new(GeoJsonValue::MultiLineString(vec![
            vec![vec![0.0, 0.0], vec![1.0, 1.0]],
            vec![vec![2.0, 2.0], vec![3.0, 3.0]],
        ]));

        let ls = LineString::from_geojson(&geom).unwrap();
        assert_eq!(ls.0.len(), 4);
    }

    #[test]
    fn test_polygon_from_geojson() {
        let geom = GeoJsonGeometry::new(GeoJsonValue::Polygon(vec![vec![
            vec![0.0, 0.0],
            vec![1.0, 0.0],
            vec![1.0, 1.0],
            vec![0.0, 0.0],
        ]]));

        let poly = Polygon::from_geojson(&geom).unwrap();
        assert_eq!(poly.exterior().0.len(), 4);
        assert!(poly.interiors().is_empty());
    }

    #[test]
    fn test_multipolygon_from_polygon_geojson() {
        let geom = GeoJsonGeometry::new(GeoJsonValue::Polygon(vec![vec![
            vec![0.0, 0.0],
            vec![1.0, 0.0],
            vec![1.0, 1.0],
            vec![0.0, 0.0],
        ]]));

        let mp = MultiPolygon::from_geojson(&geom).unwrap();
        assert_eq!(mp.0.len(), 1);
    }

    #[test]
    fn test_multipolygon_from_multipolygon_geojson() {
        let geom = GeoJsonGeometry::new(GeoJsonValue::MultiPolygon(vec![
            vec![vec![
                vec![0.0, 0.0],
                vec![1.0, 0.0],
                vec![1.0, 1.0],
                vec![0.0, 0.0],
            ]],
            vec![vec![
                vec![2.0, 2.0],
                vec![3.0, 2.0],
                vec![3.0, 3.0],
                vec![2.0, 2.0],
            ]],
        ]));

        let mp = MultiPolygon::from_geojson(&geom).unwrap();
        assert_eq!(mp.0.len(), 2);
    }

    #[test]
    fn test_polygon_to_geojson() {
        let poly = Polygon::new(
            LineString::new(vec![
                Coord { x: 0.0, y: 0.0 },
                Coord { x: 1.0, y: 0.0 },
                Coord { x: 1.0, y: 1.0 },
                Coord { x: 0.0, y: 0.0 },
            ]),
            vec![],
        );

        let geom = poly.to_geojson();
        match geom.value {
            GeoJsonValue::Polygon(rings) => {
                assert_eq!(rings.len(), 1);
                assert_eq!(rings[0].len(), 4);
            }
            _ => panic!("Expected Polygon"),
        }
    }

    #[test]
    fn test_multipolygon_to_geojson() {
        let mp = MultiPolygon::new(vec![
            Polygon::new(
                LineString::new(vec![
                    Coord { x: 0.0, y: 0.0 },
                    Coord { x: 1.0, y: 0.0 },
                    Coord { x: 0.0, y: 0.0 },
                ]),
                vec![],
            ),
            Polygon::new(
                LineString::new(vec![
                    Coord { x: 2.0, y: 2.0 },
                    Coord { x: 3.0, y: 2.0 },
                    Coord { x: 2.0, y: 2.0 },
                ]),
                vec![],
            ),
        ]);

        let geom = mp.to_geojson();
        match geom.value {
            GeoJsonValue::MultiPolygon(polys) => {
                assert_eq!(polys.len(), 2);
            }
            _ => panic!("Expected MultiPolygon"),
        }
    }

    #[test]
    fn test_polygon_with_hole_roundtrip() {
        let exterior = LineString::new(vec![
            Coord { x: 0.0, y: 0.0 },
            Coord { x: 10.0, y: 0.0 },
            Coord { x: 10.0, y: 10.0 },
            Coord { x: 0.0, y: 10.0 },
            Coord { x: 0.0, y: 0.0 },
        ]);
        let hole = LineString::new(vec![
            Coord { x: 2.0, y: 2.0 },
            Coord { x: 8.0, y: 2.0 },
            Coord { x: 8.0, y: 8.0 },
            Coord { x: 2.0, y: 8.0 },
            Coord { x: 2.0, y: 2.0 },
        ]);
        let poly = Polygon::new(exterior, vec![hole]);

        let geom = poly.to_geojson();
        let parsed = Polygon::from_geojson(&geom).unwrap();

        assert_eq!(parsed.exterior().0.len(), 5);
        assert_eq!(parsed.interiors().len(), 1);
        assert_eq!(parsed.interiors()[0].0.len(), 5);
    }

    #[test]
    fn test_rejects_point_geometry() {
        let geom = GeoJsonGeometry::new(GeoJsonValue::Point(vec![0.0, 0.0]));

        assert!(LineString::from_geojson(&geom).is_err());
        assert!(Polygon::from_geojson(&geom).is_err());
        assert!(MultiPolygon::from_geojson(&geom).is_err());
    }
}
