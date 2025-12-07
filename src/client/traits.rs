use std::future::Future;

use super::types::{BBox, FetchResult};
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
    ) -> impl Future<Output = FetchResult<Self::Record>> + Send;
}
