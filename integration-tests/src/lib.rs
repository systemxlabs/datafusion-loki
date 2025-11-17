mod cmd;
mod docker;
mod utils;

pub use cmd::*;
pub use docker::*;
pub use utils::*;

use datafusion::{logical_expr::ScalarUDF, prelude::SessionContext};
use datafusion_loki::{LokiLogTable, MapGet};
use std::sync::{Arc, OnceLock};
use tokio::sync::OnceCell;

static LOKI_CONTAINER: OnceLock<DockerCompose> = OnceLock::new();
static LOKI_INIT: OnceCell<()> = OnceCell::const_new();

pub async fn setup_loki() {
    let _ = LOKI_CONTAINER.get_or_init(|| {
        let compose =
            DockerCompose::new("loki", format!("{}/testdata", env!("CARGO_MANIFEST_DIR")));
        compose.down();
        compose.up();
        compose
    });
    let _ = LOKI_INIT
        .get_or_init(async || {
            wait_container_ready().await;
            init_loki().await;
        })
        .await;
}

async fn wait_container_ready() {
    let table = build_loki_table();

    let mut retry = 0;
    loop {
        match table.check_connection().await {
            Ok(_) => break,
            Err(e) => eprintln!("Connection error: {e}"),
        }
        retry += 1;
        if retry > 20 {
            panic!("container still not ready after 200 seconds");
        }
        tokio::time::sleep(std::time::Duration::from_secs(10)).await;
    }
}

pub async fn init_loki() {
    let ctx = build_session_context();
    let insert_sqls = include_str!("../testdata/init.sql");
    for sql in insert_sqls.split(';') {
        let sql = sql.trim();
        if sql.is_empty() {
            continue;
        }
        ctx.sql(sql).await.unwrap().collect().await.unwrap();
    }
}

pub fn build_loki_table() -> LokiLogTable {
    LokiLogTable::try_new("http://localhost:33100")
        .unwrap()
        .with_default_label(Some("app".to_string()))
}

pub fn build_session_context() -> SessionContext {
    let ctx = SessionContext::new();
    ctx.register_table("loki", Arc::new(build_loki_table()))
        .unwrap();
    ctx.register_udf(ScalarUDF::new_from_impl(MapGet::new()));
    ctx
}
