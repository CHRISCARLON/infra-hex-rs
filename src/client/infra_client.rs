use std::future::Future;

use geojson::Feature;
use serde::Deserialize;

use super::pagination::{fetch_all_pages, PaginationConfig};
use super::types::{ApiResponse, BBox, GeoPoint2d, HttpClient, InfraResult};
use crate::error::InfraHexError;

pub trait InfraClient {
    type Record;

    fn fetch_by_bbox(
        &self,
        bbox: &BBox,
        limit: Option<usize>,
    ) -> impl Future<Output = Result<Vec<Self::Record>, InfraHexError>> + Send;

    fn fetch_all_by_bbox(
        &self,
        bbox: &BBox,
    ) -> impl Future<Output = InfraResult<Self::Record>> + Send;
}

#[derive(Debug, Deserialize)]
pub struct PipelineRecord {
    pub geo_point_2d: GeoPoint2d,
    pub geo_shape: Feature,

    #[serde(rename = "type")]
    pub pipe_type: Option<String>,
    pub pressure: Option<String>,
    pub material: Option<String>,
    pub diameter: Option<f64>,
    pub diam_unit: Option<String>,

    pub carr_mat: Option<String>,
    pub carr_dia: Option<f64>,
    pub carr_di_un: Option<String>,

    pub asset_id: Option<String>,
    pub depth: Option<f64>,
    pub ag_ind: Option<String>,
    pub inst_date: Option<String>,
}

pub struct CadentClient {
    http: HttpClient,
    base_url: String,
}

impl CadentClient {
    const DEFAULT_BASE_URL: &'static str = "https://cadentgas.opendatasoft.com/api/explore/v2.1/catalog/datasets/gas-pipe-infrastructure-gpi_open/records";

    pub fn new() -> Result<Self, InfraHexError> {
        let key = std::env::var("CADENT_API_KEY")
            .map_err(|_| InfraHexError::Config("CADENT_API_KEY not set".into()))?;

        Ok(Self {
            http: HttpClient::new().with_api_key(key),
            base_url: Self::DEFAULT_BASE_URL.to_string(),
        })
    }

    fn bbox_query(&self, bbox: &BBox) -> String {
        format!(
            "in_bbox(geo_point_2d,{},{},{},{})",
            bbox.min_lat, bbox.min_lon, bbox.max_lat, bbox.max_lon
        )
    }

    async fn fetch_page(
        &self,
        bbox: &BBox,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<PipelineRecord>, InfraHexError> {
        let url = format!(
            "{}?where={}&limit={}&offset={}",
            self.base_url,
            urlencoding::encode(&self.bbox_query(bbox)),
            limit,
            offset
        );

        let response: ApiResponse<PipelineRecord> = self.http.fetch_json(&url).await?;
        Ok(response.results)
    }
}

impl InfraClient for CadentClient {
    type Record = PipelineRecord;

    async fn fetch_by_bbox(
        &self,
        bbox: &BBox,
        limit: Option<usize>,
    ) -> Result<Vec<Self::Record>, InfraHexError> {
        let limit = limit.unwrap_or(100);
        let url = format!(
            "{}?where={}&limit={}",
            self.base_url,
            urlencoding::encode(&self.bbox_query(bbox)),
            limit
        );

        let response: ApiResponse<PipelineRecord> = self.http.fetch_json(&url).await?;
        Ok(response.results)
    }

    async fn fetch_all_by_bbox(&self, bbox: &BBox) -> InfraResult<Self::Record> {
        // Get total count first
        let url = format!(
            "{}?where={}&limit=1",
            self.base_url,
            urlencoding::encode(&self.bbox_query(bbox)),
        );

        let first = match self
            .http
            .fetch_json::<ApiResponse<PipelineRecord>>(&url)
            .await
        {
            Ok(resp) => resp,
            Err(e) => {
                let mut result = InfraResult::new();
                result.errors.push(e);
                return result;
            }
        };

        let total = first.total_count as usize;

        // Use pagination helper with OpenDataSoft config
        fetch_all_pages(total, PaginationConfig::opendatasoft(), |offset, limit| {
            self.fetch_page(bbox, limit, offset)
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    #[ignore]
    async fn test_fetch_pipeline_data() -> Result<(), InfraHexError> {
        let client = CadentClient::new()?;
        let bbox = BBox::new(53.47, -2.26, 53.49, -2.22);

        let results = client.fetch_by_bbox(&bbox, Some(5)).await?;
        println!("Got {} results", results.len());

        for pipe in &results {
            println!("Asset: {:?}, Type: {:?}", pipe.asset_id, pipe.pipe_type);
        }
        Ok(())
    }

    #[tokio::test]
    #[ignore]
    async fn test_fetch_all_parallel() -> Result<(), InfraHexError> {
        let client = CadentClient::new()?;
        let bbox = BBox::new(53.47, -2.26, 53.49, -2.22);

        let result = client.fetch_all_by_bbox(&bbox).await;
        println!(
            "Got {} records, {} errors",
            result.records.len(),
            result.errors.len()
        );
        Ok(())
    }
}
