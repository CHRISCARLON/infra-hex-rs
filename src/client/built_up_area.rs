use geo_types::{MultiPolygon, Polygon};
use geojson::{Feature, FeatureCollection, Geometry as GeoJsonGeometry};

use crate::core::{FromGeoJson, ToGeoJson};
use crate::error::InfraHexError;

use super::types::HttpClient;

const BASE_URL: &str = "https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/main_ONS_BUA_2024_EW/FeatureServer/0/query";

/// Represents a Built-Up Area (BUA) boundary from the ONS 2024 dataset.
///
/// # Fields
///
/// * `object_id` - Unique identifier from the ONS dataset (used for API queries)
/// * `code` - ONS statistical code
/// * `name` - English name of the built-up area
/// * `name_welsh` - Welsh name (if applicable, mainly for Welsh areas)
/// * `area_hectares` - Total area in hectares
/// * `geometry` - MultiPolygon boundary in WGS84 (EPSG:4326) coordinates
#[derive(Debug, Clone)]
pub struct BuiltUpArea {
    pub object_id: i64,
    pub code: String,
    pub name: String,
    pub name_welsh: Option<String>,
    pub area_hectares: Option<f64>,
    pub geometry: MultiPolygon<f64>,
}

impl BuiltUpArea {
    /// Converts the built-up area to a GeoJSON [`Feature`].
    ///
    /// The resulting feature includes all metadata as properties:
    /// - `object_id`: ONS unique identifier
    /// - `code`: Statistical code
    /// - `name`: English name
    /// - `name_welsh`: Welsh name (if present)
    /// - `area_hectares`: Area (if present)
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use infra_hex_rs::BuiltUpAreaClient;
    /// # async fn example() -> Result<(), infra_hex_rs::InfraHexError> {
    /// let client = BuiltUpAreaClient::new();
    /// let area = client.fetch_by_object_id(1310).await?;
    /// let feature = area.to_geojson_feature();
    /// assert!(feature.geometry.is_some());
    /// # Ok(())
    /// # }
    /// ```
    pub fn to_geojson_feature(&self) -> Feature {
        let geometry = self.geometry.to_geojson();

        let mut properties = serde_json::Map::new();
        properties.insert("object_id".to_string(), serde_json::json!(self.object_id));
        properties.insert("code".to_string(), serde_json::json!(self.code));
        properties.insert("name".to_string(), serde_json::json!(self.name));
        if let Some(ref name_welsh) = self.name_welsh {
            properties.insert("name_welsh".to_string(), serde_json::json!(name_welsh));
        }
        if let Some(area) = self.area_hectares {
            properties.insert("area_hectares".to_string(), serde_json::json!(area));
        }

        Feature {
            bbox: None,
            geometry: Some(geometry),
            id: None,
            properties: Some(properties),
            foreign_members: None,
        }
    }

    /// Serializes the built-up area to a GeoJSON string.
    ///
    /// # Errors
    ///
    /// Returns [`InfraHexError::Json`] if serialization fails.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use infra_hex_rs::BuiltUpAreaClient;
    /// # async fn example() -> Result<(), infra_hex_rs::InfraHexError> {
    /// let client = BuiltUpAreaClient::new();
    /// let area = client.fetch_by_object_id(1310).await?;
    /// let geojson_str = area.to_geojson()?;
    /// println!("{}", geojson_str);
    /// # Ok(())
    /// # }
    /// ```
    pub fn to_geojson(&self) -> Result<String, InfraHexError> {
        let feature = self.to_geojson_feature();
        serde_json::to_string(&feature).map_err(InfraHexError::Json)
    }
}

/// Converts a [`geo_types::Polygon`] to a GeoJSON [`Geometry`](GeoJsonGeometry).
///
/// This utility function handles both the exterior ring and any interior holes
/// in the polygon, producing valid GeoJSON output.
///
/// # Arguments
///
/// * `polygon` - A polygon with WGS84 coordinates
///
/// # Example
///
/// ```
/// use geo_types::{Coord, LineString, Polygon};
/// use infra_hex_rs::polygon_to_geojson;
///
/// let exterior = LineString::new(vec![
///     Coord { x: -0.1, y: 51.5 },
///     Coord { x: -0.1, y: 51.6 },
///     Coord { x: 0.0, y: 51.6 },
///     Coord { x: 0.0, y: 51.5 },
///     Coord { x: -0.1, y: 51.5 },
/// ]);
/// let polygon = Polygon::new(exterior, vec![]);
/// let geojson = polygon_to_geojson(&polygon);
/// ```
pub fn polygon_to_geojson(polygon: &Polygon<f64>) -> GeoJsonGeometry {
    polygon.to_geojson()
}

/// HTTP client for fetching Built-Up Area boundaries from the ONS Open Geography Portal.
///
/// This client queries the ONS ArcGIS Feature Service for the 2024 Built-Up Areas
/// dataset covering England and Wales. No authentication is required.
///
/// # Data Source
///
/// - **Dataset**: Built-Up Areas (December 2024) EW
/// - **Provider**: Office for National Statistics (ONS)
/// - **CRS**: WGS84 (EPSG:4326)
/// - **Coverage**: England and Wales
///
/// # Example
///
/// ```no_run
/// use infra_hex_rs::BuiltUpAreaClient;
///
/// # async fn example() -> Result<(), infra_hex_rs::InfraHexError> {
/// let client = BuiltUpAreaClient::new();
///
/// // Manchester has OBJECTID 1310
/// let manchester = client.fetch_by_object_id(1310).await?;
/// println!("Area: {} hectares", manchester.area_hectares.unwrap_or(0.0));
/// # Ok(())
/// # }
/// ```
pub struct BuiltUpAreaClient {
    http: HttpClient,
}

impl BuiltUpAreaClient {
    /// Creates a new client instance.
    ///
    /// No configuration or API keys are required as the ONS API is publicly accessible.
    pub fn new() -> Self {
        Self {
            http: HttpClient::new(),
        }
    }

    /// Fetches a built-up area by its ONS OBJECTID.
    ///
    /// The OBJECTID is a unique identifier assigned by the ONS Feature Server.
    /// You can find OBJECTIDs by querying the ONS Open Geography Portal directly
    /// or using their web interface.
    ///
    /// # Arguments
    ///
    /// * `object_id` - The ONS OBJECTID for the built-up area
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The HTTP request fails ([`InfraHexError::Http`])
    /// - No area exists with the given OBJECTID ([`InfraHexError::Api`])
    /// - The response geometry is invalid ([`InfraHexError::Geometry`])
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use infra_hex_rs::BuiltUpAreaClient;
    /// # async fn example() -> Result<(), infra_hex_rs::InfraHexError> {
    /// let client = BuiltUpAreaClient::new();
    ///
    /// // Some known OBJECTIDs:
    /// // - 1310: Manchester
    /// let area = client.fetch_by_object_id(1310).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn fetch_by_object_id(&self, object_id: i64) -> Result<BuiltUpArea, InfraHexError> {
        let url = format!(
            "{}?where=OBJECTID%3D{}&outFields=*&f=geojson",
            BASE_URL, object_id
        );

        let fc: FeatureCollection = self.http.fetch_json(&url).await?;

        if fc.features.is_empty() {
            return Err(InfraHexError::Api(format!(
                "No built-up area found with OBJECTID: {}",
                object_id
            )));
        }

        parse_feature(&fc.features[0])
    }
}

impl Default for BuiltUpAreaClient {
    fn default() -> Self {
        Self::new()
    }
}

fn parse_feature(feature: &Feature) -> Result<BuiltUpArea, InfraHexError> {
    let properties = feature
        .properties
        .as_ref()
        .ok_or_else(|| InfraHexError::Geometry("Feature has no properties".to_string()))?;

    let object_id = properties
        .get("OBJECTID")
        .and_then(|v| v.as_i64())
        .ok_or_else(|| InfraHexError::Geometry("Missing OBJECTID".to_string()))?;

    let code = properties
        .get("BUA24CD")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    let name = properties
        .get("BUA24NM")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    let name_welsh = properties
        .get("BUA24NMW")
        .and_then(|v| v.as_str())
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string());

    let area_hectares = properties.get("areahectar").and_then(|v| v.as_f64());

    let geometry = feature
        .geometry
        .as_ref()
        .ok_or_else(|| InfraHexError::Geometry("Feature has no geometry".to_string()))?;

    let multipolygon = MultiPolygon::from_geojson(geometry)?;

    Ok(BuiltUpArea {
        object_id,
        code,
        name,
        name_welsh,
        area_hectares,
        geometry: multipolygon,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use geo_types::{Coord, LineString};
    use geojson::Value as GeoJsonValue;

    // ==================== Unit Tests ====================

    /// Test polygon_to_geojson with a simple square
    #[test]
    fn test_polygon_to_geojson_simple() {
        let exterior = LineString::new(vec![
            Coord { x: 0.0, y: 0.0 },
            Coord { x: 1.0, y: 0.0 },
            Coord { x: 1.0, y: 1.0 },
            Coord { x: 0.0, y: 1.0 },
            Coord { x: 0.0, y: 0.0 },
        ]);
        let polygon = Polygon::new(exterior, vec![]);

        let geojson = polygon_to_geojson(&polygon);

        match geojson.value {
            GeoJsonValue::Polygon(rings) => {
                assert_eq!(rings.len(), 1, "Should have one ring (exterior)");
                assert_eq!(rings[0].len(), 5, "Exterior should have 5 coordinates");
                assert_eq!(rings[0][0], vec![0.0, 0.0]);
                assert_eq!(rings[0][1], vec![1.0, 0.0]);
            }
            _ => panic!("Expected Polygon geometry"),
        }
    }

    /// Test polygon_to_geojson with interior holes
    #[test]
    fn test_polygon_to_geojson_with_hole() {
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
        let polygon = Polygon::new(exterior, vec![hole]);

        let geojson = polygon_to_geojson(&polygon);

        match geojson.value {
            GeoJsonValue::Polygon(rings) => {
                assert_eq!(rings.len(), 2, "Should have exterior + 1 hole");
                assert_eq!(rings[0].len(), 5, "Exterior ring");
                assert_eq!(rings[1].len(), 5, "Interior hole");
            }
            _ => panic!("Expected Polygon geometry"),
        }
    }

    /// Test FromGeoJson with Polygon input (wraps to MultiPolygon)
    #[test]
    fn test_multipolygon_from_geojson_polygon() {
        let geojson = GeoJsonGeometry::new(GeoJsonValue::Polygon(vec![vec![
            vec![0.0, 0.0],
            vec![1.0, 0.0],
            vec![1.0, 1.0],
            vec![0.0, 0.0],
        ]]));

        let result = MultiPolygon::from_geojson(&geojson);
        assert!(result.is_ok());

        let mp = result.unwrap();
        assert_eq!(mp.0.len(), 1, "Should wrap single polygon in MultiPolygon");
    }

    /// Test FromGeoJson with MultiPolygon input
    #[test]
    fn test_multipolygon_from_geojson_multipolygon() {
        let geojson = GeoJsonGeometry::new(GeoJsonValue::MultiPolygon(vec![
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

        let result = MultiPolygon::from_geojson(&geojson);
        assert!(result.is_ok());

        let mp = result.unwrap();
        assert_eq!(mp.0.len(), 2, "Should have 2 polygons");
    }

    /// Test FromGeoJson rejects Point geometry
    #[test]
    fn test_multipolygon_from_geojson_rejects_point() {
        let geojson = GeoJsonGeometry::new(GeoJsonValue::Point(vec![0.0, 0.0]));
        let result = MultiPolygon::from_geojson(&geojson);
        assert!(result.is_err());
    }

    /// Test BuiltUpArea::to_geojson_feature
    #[test]
    fn test_built_up_area_to_geojson_feature() {
        let area = BuiltUpArea {
            object_id: 123,
            code: "E34000001".to_string(),
            name: "Test Area".to_string(),
            name_welsh: Some("Ardal Prawf".to_string()),
            area_hectares: Some(1000.5),
            geometry: MultiPolygon::new(vec![Polygon::new(
                LineString::new(vec![
                    Coord { x: 0.0, y: 0.0 },
                    Coord { x: 1.0, y: 0.0 },
                    Coord { x: 1.0, y: 1.0 },
                    Coord { x: 0.0, y: 0.0 },
                ]),
                vec![],
            )]),
        };

        let feature = area.to_geojson_feature();

        assert!(feature.geometry.is_some());
        let props = feature.properties.unwrap();
        assert_eq!(props.get("object_id").unwrap(), 123);
        assert_eq!(props.get("code").unwrap(), "E34000001");
        assert_eq!(props.get("name").unwrap(), "Test Area");
        assert_eq!(props.get("name_welsh").unwrap(), "Ardal Prawf");
        assert_eq!(props.get("area_hectares").unwrap(), 1000.5);
    }

    /// Test BuiltUpArea::to_geojson_feature without optional fields
    #[test]
    fn test_built_up_area_to_geojson_feature_minimal() {
        let area = BuiltUpArea {
            object_id: 456,
            code: "E34000002".to_string(),
            name: "Minimal Area".to_string(),
            name_welsh: None,
            area_hectares: None,
            geometry: MultiPolygon::new(vec![Polygon::new(
                LineString::new(vec![
                    Coord { x: 0.0, y: 0.0 },
                    Coord { x: 1.0, y: 0.0 },
                    Coord { x: 0.0, y: 0.0 },
                ]),
                vec![],
            )]),
        };

        let feature = area.to_geojson_feature();
        let props = feature.properties.unwrap();

        assert!(props.get("name_welsh").is_none());
        assert!(props.get("area_hectares").is_none());
    }

    /// Test BuiltUpArea::to_geojson serialization
    #[test]
    fn test_built_up_area_to_geojson_string() {
        let area = BuiltUpArea {
            object_id: 789,
            code: "E34000003".to_string(),
            name: "JSON Test".to_string(),
            name_welsh: None,
            area_hectares: Some(500.0),
            geometry: MultiPolygon::new(vec![Polygon::new(
                LineString::new(vec![
                    Coord { x: -0.1, y: 51.5 },
                    Coord { x: 0.0, y: 51.5 },
                    Coord { x: 0.0, y: 51.6 },
                    Coord { x: -0.1, y: 51.5 },
                ]),
                vec![],
            )]),
        };

        let json_str = area.to_geojson().unwrap();

        assert!(json_str.contains("\"type\":\"Feature\""));
        assert!(json_str.contains("\"MultiPolygon\""));
        assert!(json_str.contains("\"name\":\"JSON Test\""));
    }

    /// Test BuiltUpAreaClient::default
    #[test]
    fn test_client_default() {
        let client1 = BuiltUpAreaClient::new();
        let client2 = BuiltUpAreaClient::default();

        // Both should be valid clients (we can't compare internals easily)
        assert!(std::mem::size_of_val(&client1) == std::mem::size_of_val(&client2));
    }

    // ==================== Integration Tests ====================
    // These tests require network access and are marked with #[ignore]

    /// Integration test: fetch Manchester by OBJECTID
    #[tokio::test]
    #[ignore = "requires network access"]
    async fn test_fetch_manchester() {
        let client = BuiltUpAreaClient::new();
        let result = client.fetch_by_object_id(1310).await;

        assert!(result.is_ok(), "Should fetch Manchester successfully");

        let area = result.unwrap();
        assert_eq!(area.object_id, 1310);
        assert!(!area.name.is_empty(), "Should have a name");
        assert!(!area.code.is_empty(), "Should have a code");
        assert!(!area.geometry.0.is_empty(), "Should have geometry");

        println!("Fetched: {} ({})", area.name, area.code);
        println!("Polygons: {}", area.geometry.0.len());
        if let Some(hectares) = area.area_hectares {
            println!("Area: {:.2} hectares", hectares);
        }
    }

    /// Integration test: non-existent OBJECTID returns error
    #[tokio::test]
    #[ignore = "requires network access"]
    async fn test_fetch_nonexistent_object_id() {
        let client = BuiltUpAreaClient::new();
        let result = client.fetch_by_object_id(999999999).await;

        assert!(result.is_err(), "Should fail for non-existent OBJECTID");
    }

    /// Integration test: verify GeoJSON roundtrip
    #[tokio::test]
    #[ignore = "requires network access"]
    async fn test_geojson_roundtrip() {
        let client = BuiltUpAreaClient::new();
        let area = client.fetch_by_object_id(1310).await.unwrap();

        // Convert to GeoJSON and back (parse as generic JSON)
        let geojson_str = area.to_geojson().unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&geojson_str).unwrap();

        assert_eq!(parsed["type"], "Feature");
        assert!(parsed["geometry"].is_object());
        assert!(parsed["properties"].is_object());
        assert_eq!(parsed["properties"]["object_id"], area.object_id);
    }
}
