use std::future::Future;

use geojson::Feature;

use super::types::{BBox, InfraResult};
use crate::error::InfraHexError;

/// Trait for infrastructure data clients that fetch records by bounding box.
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

/// Trait for pipeline records from different infrastructure clients.
/// Implement this for each client's record type to enable hex grid processing.
pub trait PipelineData: Send + Sync {
    /// Returns the GeoJSON feature containing the pipeline geometry.
    fn geo_shape(&self) -> &Feature;

    /// Returns the asset identifier, if available.
    fn asset_id(&self) -> Option<&str>;

    /// Returns the pipeline type (e.g., "LP", "MP", "IP"), if available.
    fn pipe_type(&self) -> Option<&str>;

    /// Returns the pipe material, if available.
    fn material(&self) -> Option<&str>;

    /// Returns the pressure classification, if available.
    fn pressure(&self) -> Option<&str>;
}
