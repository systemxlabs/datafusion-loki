use integration_tests::assert_loki_output;

#[tokio::test]
async fn full_table_scan() -> Result<(), Box<dyn std::error::Error>> {
    assert_loki_output(
        "select * from loki",
        r#"+--------------------------------------------------------------+--------------------+
| labels                                                       | line               |
+--------------------------------------------------------------+--------------------+
| {app: my-app, detected_level: unknown, service_name: my-app} | this is a test log |
+--------------------------------------------------------------+--------------------+"#,
    )
    .await?;
    Ok(())
}

#[tokio::test]
async fn timestamp_filter() -> Result<(), Box<dyn std::error::Error>> {
    Ok(())
}

#[tokio::test]
async fn label_filter() -> Result<(), Box<dyn std::error::Error>> {
    Ok(())
}

#[tokio::test]
async fn line_filter() -> Result<(), Box<dyn std::error::Error>> {
    Ok(())
}
