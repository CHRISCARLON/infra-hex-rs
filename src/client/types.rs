use geo_types::Point;
use serde::Deserialize;
use serde::de::DeserializeOwned;

use crate::error::InfraHexError;

#[derive(Debug, Deserialize)]
pub struct ApiResponse<T> {
    pub total_count: u64,
    pub results: Vec<T>,
}

#[derive(Debug)]
pub struct InfraResult<T> {
    pub records: Vec<T>,
    pub errors: Vec<InfraHexError>,
}

impl<T> InfraResult<T> {
    pub fn new() -> Self {
        Self {
            records: Vec::new(),
            errors: Vec::new(),
        }
    }

    pub fn has_errors(&self) -> bool {
        !self.errors.is_empty()
    }

    pub fn is_complete(&self) -> bool {
        self.errors.is_empty()
    }
}

impl<T> Default for InfraResult<T> {
    fn default() -> Self {
        Self::new()
    }
}

pub struct HttpClient {
    client: reqwest::Client,
    api_key: Option<String>,
}

impl HttpClient {
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::new(),
            api_key: None,
        }
    }

    pub fn with_api_key(mut self, key: impl Into<String>) -> Self {
        self.api_key = Some(key.into());
        self
    }

    pub async fn fetch_json<T: DeserializeOwned>(&self, url: &str) -> Result<T, InfraHexError> {
        let mut request = self.client.get(url);

        if let Some(key) = &self.api_key {
            request = request.header("Authorization", format!("Apikey {}", key));
        }

        let response = request.send().await?;

        if !response.status().is_success() {
            return Err(InfraHexError::Api(format!(
                "API returned status {}",
                response.status()
            )));
        }

        let data: T = response.json().await?;
        Ok(data)
    }
}

impl Default for HttpClient {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Deserialize, Clone, Copy)]
pub struct GeoPoint2d {
    pub lon: f64,
    pub lat: f64,
}

impl From<GeoPoint2d> for Point<f64> {
    fn from(p: GeoPoint2d) -> Self {
        Point::new(p.lon, p.lat)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct BBox {
    pub min_lat: f64,
    pub min_lon: f64,
    pub max_lat: f64,
    pub max_lon: f64,
}

impl BBox {
    pub fn new(min_lat: f64, min_lon: f64, max_lat: f64, max_lon: f64) -> Self {
        Self {
            min_lat,
            min_lon,
            max_lat,
            max_lon,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_geopoint_to_point() {
        let gp = GeoPoint2d {
            lon: -2.0,
            lat: 53.0,
        };
        let p: Point<f64> = gp.into();
        assert_eq!(p.x(), -2.0);
        assert_eq!(p.y(), 53.0);
    }
}
