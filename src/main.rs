use infra_hex_rs::{BBox, CadentClient, InfraClient, InfraHexError, get_hex_cells};

#[tokio::main]
async fn main() -> Result<(), InfraHexError> {
    let client = CadentClient::new()?;
    let bbox = BBox::new(53.47, -2.26, 53.49, -2.22);

    println!("Fetching all pipelines from Cadent API...");
    let result = client.fetch_all_by_bbox(&bbox).await;

    if result.has_errors() {
        eprintln!("Warning: {} fetch errors occurred", result.errors.len());
    }

    let pipelines = result.records;
    println!("Got {} pipelines", pipelines.len());

    for pipeline in &pipelines {
        let cells = get_hex_cells(pipeline, 10)?;
        println!(
            "Pipeline {:?}: {} hex cells",
            pipeline.asset_id.as_deref().unwrap_or("unknown"),
            cells.len()
        );
    }

    Ok(())
}
