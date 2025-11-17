use integration_tests::assert_sql;

#[tokio::test]
async fn full_table_scan() -> Result<(), Box<dyn std::error::Error>> {
    assert_sql("select * from loki", r#"+--------------------------------+--------------------------------------------------------------+--------------------+
| timestamp                      | labels                                                       | line               |
+--------------------------------+--------------------------------------------------------------+--------------------+
| 2025-11-17T06:37:37.293961100Z | {app: my-app, detected_level: unknown, service_name: my-app} | this is a test log |
+--------------------------------+--------------------------------------------------------------+--------------------+"#).await?;
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
