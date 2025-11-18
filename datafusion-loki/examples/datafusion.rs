use datafusion::prelude::SessionContext;
use datafusion_loki::LokiLogTable;
use std::sync::Arc;

#[tokio::main]
pub async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let loki_table = LokiLogTable::try_new("http://localhost:33100")?;
    // let loki_table = LokiLogTable::try_new("http://192.168.0.159:42705")?;

    let ctx = SessionContext::new();
    ctx.register_table("loki", Arc::new(loki_table))?;

    ctx.sql(
        "insert into loki values (current_timestamp(), Map{'app': 'my-app'}, 'user login failed')",
    )
    .await?
    .show()
    .await?;

    ctx.sql(
        r#"
select * 
from loki 
where labels['app'] = 'my-app' 
    and timestamp > '2025-11-12T00:00:00Z' 
    and line like '%login%' 
limit 2
"#,
    )
    .await?
    .show()
    .await?;

    Ok(())
}
