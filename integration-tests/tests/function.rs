use datafusion::{logical_expr::ScalarUDF, prelude::SessionContext};
use datafusion_loki::MapGet;
use integration_tests::assert_sql_output;

#[tokio::test]
async fn test_map_get() -> Result<(), Box<dyn std::error::Error>> {
    let ctx = SessionContext::new();
    ctx.register_udf(ScalarUDF::new_from_impl(MapGet::new()));

    // Get existing key
    assert_sql_output(
        &ctx,
        "select map_get(Map {'a': 1}, 'a') as value",
        r#"+-------+
| value |
+-------+
| 1     |
+-------+"#,
    )
    .await?;

    // Get non-existent key
    assert_sql_output(
        &ctx,
        "select map_get(Map {'a': 1}, 'b') as value",
        r#"+-------+
| value |
+-------+
|       |
+-------+"#,
    )
    .await?;

    // Duplicated keys
    let result = assert_sql_output(
        &ctx,
        "select map_get(Map {'a': 1, 'a': 2}, 'b') as value",
        r#""#,
    )
    .await;
    assert!(result.is_err());

    Ok(())
}
