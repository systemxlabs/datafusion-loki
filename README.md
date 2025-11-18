# datafusion-loki
![License](https://img.shields.io/badge/license-MIT-blue.svg)
[![Crates.io](https://img.shields.io/crates/v/datafusion-loki.svg)](https://crates.io/crates/datafusion-loki)
[![Docs](https://docs.rs/datafusion-loki/badge.svg)](https://docs.rs/datafusion-loki/latest/datafusion_loki/)

A Datafusion table provider for querying Loki data.

## Features
1. Use SQL to query Loki logs
2. Insert logs into Loki
3. Support pushing down filters and limit to Loki
4. Execution plan can be serialized for distributed execution

## Usage
```rust
#[tokio::main]
pub async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let loki_table = LokiLogTable::try_new("http://localhost:33100")?;

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
```
Output
```
+-------+
| count |
+-------+
| 1     |
+-------+

+--------------------------------+--------------------------------------------------------------+-------------------+
| timestamp                      | labels                                                       | line              |
+--------------------------------+--------------------------------------------------------------+-------------------+
| 2025-11-17T09:34:36.418570400Z | {app: my-app, detected_level: unknown, service_name: my-app} | user login failed |
+--------------------------------+--------------------------------------------------------------+-------------------+
```