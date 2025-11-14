use datafusion::{logical_expr::ScalarUDF, prelude::SessionContext};
use datafusion_loki::MapGet;
use integration_tests::assert_sql_output;

#[tokio::test]
async fn test_map_get() -> Result<(), Box<dyn std::error::Error>> {
    let ctx = SessionContext::new();
    ctx.register_udf(ScalarUDF::new_from_impl(MapGet::new()));

    assert_sql_output(
        &ctx,
        "select map_get(Map {'a': 1}, 'a') as value",
        r#"+-------+
| value |
+-------+
| 1     |
+-------+"#,
    )
    .await;

    assert_sql_output(
        &ctx,
        "select map_get(Map {'a': 1}, 'b') as value",
        r#"+-------+
| value |
+-------+
|       |
+-------+"#,
    )
    .await;

    Ok(())
}
