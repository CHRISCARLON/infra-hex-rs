use thiserror::Error;

#[derive(Error, Debug)]
pub enum InfraHexError {
    #[error("HTTP request failed: {0}")]
    Http(#[from] reqwest::Error),

    #[error("JSON parsing failed: {0}")]
    Json(#[from] serde_json::Error),

    #[error("API error: {0}")]
    Api(String),

    #[error("Config error: {0}")]
    Config(String),

    #[error("Geometry error: {0}")]
    Geometry(String),

    #[error("Hex grid error: {0}")]
    HexGrid(#[from] n3gb_rs::N3gbError),
}
