use integration_tests::assert_loki_output;

#[tokio::test]
async fn full_table_scan() -> Result<(), Box<dyn std::error::Error>> {
    assert_loki_output(
        "select * from loki",
        r#"+----------------------------------------------------------------+-----------------+
| labels                                                         | line            |
+----------------------------------------------------------------+-----------------+
| {app: my-app1, detected_level: unknown, service_name: my-app1} | this is aaa log |
| {app: my-app2, detected_level: unknown, service_name: my-app2} | this is bbb log |
+----------------------------------------------------------------+-----------------+"#,
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
    assert_loki_output(
        "select * from loki where map_get(labels, 'app') = 'my-app2'",
        r#"+----------------------------------------------------------------+-----------------+
| labels                                                         | line            |
+----------------------------------------------------------------+-----------------+
| {app: my-app2, detected_level: unknown, service_name: my-app2} | this is bbb log |
+----------------------------------------------------------------+-----------------+"#,
    )
    .await?;

    assert_loki_output(
        "select * from loki where map_get(labels, 'app') = 'not-existing'",
        r#"++
++"#,
    )
    .await?;
    Ok(())
}

#[tokio::test]
async fn line_filter() -> Result<(), Box<dyn std::error::Error>> {
    assert_loki_output(
        "select * from loki where line like '%bbb%'",
        r#"+----------------------------------------------------------------+-----------------+
| labels                                                         | line            |
+----------------------------------------------------------------+-----------------+
| {app: my-app2, detected_level: unknown, service_name: my-app2} | this is bbb log |
+----------------------------------------------------------------+-----------------+"#,
    )
    .await?;

    assert_loki_output(
        "select * from loki where line like '%not-existing%'",
        r#"++
++"#,
    )
    .await?;
    Ok(())
}
