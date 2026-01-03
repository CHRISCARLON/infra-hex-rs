use geojson::Feature;
use serde::Deserialize;

use crate::client::traits::PipelineData;
use crate::client::types::GeoPoint2d;

#[derive(Debug, Deserialize)]
pub struct CadentPipelineRecord {
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

impl PipelineData for CadentPipelineRecord {
    fn geo_shape(&self) -> &Feature {
        &self.geo_shape
    }

    fn asset_id(&self) -> Option<&str> {
        self.asset_id.as_deref()
    }

    fn pipe_type(&self) -> Option<&str> {
        self.pipe_type.as_deref()
    }

    fn material(&self) -> Option<&str> {
        self.material.as_deref()
    }

    fn pressure(&self) -> Option<&str> {
        self.pressure.as_deref()
    }
}
