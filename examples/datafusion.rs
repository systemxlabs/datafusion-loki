use datafusion::{logical_expr::ScalarUDF, prelude::SessionContext};
use datafusion_loki::{LokiLogTable, MapGet};
use std::sync::Arc;

#[tokio::main]
pub async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let loki_table = LokiLogTable::try_new("")?;

    let ctx = SessionContext::new();
    ctx.register_table("loki", Arc::new(loki_table))?;
    ctx.register_udf(ScalarUDF::new_from_impl(MapGet::new()));

    ctx.sql("select * from loki where map_get(labels, 'app') = 'my_app' and timestamp > '2023-01-01T00:00:00Z'")
        .await?
        .show()
        .await?;

    Ok(())
}
