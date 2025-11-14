mod cmd;
mod docker;
mod utils;

pub use cmd::*;
pub use docker::*;
pub use utils::*;

use std::sync::OnceLock;

static LOKI_CONTAINER: OnceLock<DockerCompose> = OnceLock::new();
pub async fn setup_loki() {
    let _ = LOKI_CONTAINER.get_or_init(|| {
        let compose =
            DockerCompose::new("loki", format!("{}/testdata", env!("CARGO_MANIFEST_DIR")));
        compose.down();
        compose.up();
        compose
    });
}
