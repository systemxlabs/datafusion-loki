use datafusion::{logical_expr::ScalarUDF, prelude::SessionContext};
use datafusion_loki::{LokiLogTable, MapGet};
use std::sync::Arc;

#[tokio::main]
pub async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let loki_table = LokiLogTable::try_new("http://localhost:33100")?;
    // let loki_table = LokiLogTable::try_new("http://192.168.0.159:42705")?;

    let ctx = SessionContext::new();
    ctx.register_table("loki", Arc::new(loki_table))?;
    ctx.register_udf(ScalarUDF::new_from_impl(MapGet::new()));

    ctx.sql("select * from loki where map_get(labels, 'app') = 'datafabric-executor-0' and timestamp > '2025-11-12T00:00:00Z'")
        .await?
        .show()
        .await?;

    Ok(())
}
