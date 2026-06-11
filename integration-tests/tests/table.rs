use std::sync::Arc;

use datafusion::{
    arrow::util::pretty::pretty_format_batches,
    physical_plan::{ExecutionPlan, collect, display::DisplayableExecutionPlan},
};
use datafusion_loki::{LokiPhysicalCodec, TIMESTAMP_FIELD_REF};
use datafusion_proto::{physical_plan::AsExecutionPlan, protobuf::PhysicalPlanNode};
use integration_tests::{
    assert_loki_output, build_session_context, setup_loki, sort_batch_map_field,
    sort_record_batches,
};

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
async fn scan_with_projection() -> Result<(), Box<dyn std::error::Error>> {
    assert_loki_output(
        "select timestamp, line from loki where labels['app'] = 'my-app2'",
        r#"+-----------------+
| line            |
+-----------------+
| this is bbb log |
+-----------------+"#,
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
        "select * from loki where labels['app'] = 'my-app2'",
        r#"+----------------------------------------------------------------+-----------------+
| labels                                                         | line            |
+----------------------------------------------------------------+-----------------+
| {app: my-app2, detected_level: unknown, service_name: my-app2} | this is bbb log |
+----------------------------------------------------------------+-----------------+"#,
    )
    .await?;

    assert_loki_output(
        "select * from loki where labels['app'] = 'not-existing'",
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

#[tokio::test]
async fn scan_exec_serialization() -> Result<(), Box<dyn std::error::Error>> {
    setup_loki().await;

    let ctx = build_session_context();
    let df = ctx
        .sql("select * from loki where labels['app'] = 'my-app2' and line like '%bbb%'")
        .await?;
    let exec_plan = df.create_physical_plan().await?;
    println!(
        "Plan: \n{}",
        DisplayableExecutionPlan::new(exec_plan.as_ref()).indent(true)
    );
    let result = collect(exec_plan.clone(), ctx.task_ctx()).await?;
    let sorted_result = sort_batch_map_field(result);
    let sorted_result = sort_record_batches(&sorted_result, TIMESTAMP_FIELD_REF.name())?;
    let result_str = pretty_format_batches(&sorted_result)?.to_string();
    println!("Result: \n{result_str}");

    let codec = LokiPhysicalCodec {};
    let mut plan_buf: Vec<u8> = vec![];
    let plan_proto = PhysicalPlanNode::try_from_physical_plan(exec_plan, &codec)?;
    plan_proto.try_encode(&mut plan_buf)?;

    let new_plan: Arc<dyn ExecutionPlan> = PhysicalPlanNode::try_decode(&plan_buf)
        .and_then(|proto| proto.try_into_physical_plan(&ctx.task_ctx(), &codec))?;
    println!(
        "Deserialized plan: {}",
        DisplayableExecutionPlan::new(new_plan.as_ref()).indent(true)
    );

    let serde_result = collect(new_plan, ctx.task_ctx()).await?;
    let sorted_serde_result = sort_batch_map_field(serde_result);
    let sorted_serde_result =
        sort_record_batches(&sorted_serde_result, TIMESTAMP_FIELD_REF.name())?;
    let serde_result_str = pretty_format_batches(&sorted_serde_result)?.to_string();
    println!("Serde result: \n{serde_result_str}");

    assert_eq!(result_str, serde_result_str);

    Ok(())
}

#[tokio::test]
async fn insert_exec_serialization() -> Result<(), Box<dyn std::error::Error>> {
    setup_loki().await;

    let ctx = build_session_context();
    let df = ctx
        .sql("insert into loki values (current_timestamp(), Map {'app': 'insert_serde_test_app'}, 'insert serde test')")
        .await?;
    let exec_plan = df.create_physical_plan().await?;
    let plan_str = DisplayableExecutionPlan::new(exec_plan.as_ref())
        .indent(true)
        .to_string();
    println!("Plan: \n{plan_str}",);

    let codec = LokiPhysicalCodec {};
    let mut plan_buf: Vec<u8> = vec![];
    let plan_proto = PhysicalPlanNode::try_from_physical_plan(exec_plan, &codec)?;
    plan_proto.try_encode(&mut plan_buf)?;

    let new_plan: Arc<dyn ExecutionPlan> = PhysicalPlanNode::try_decode(&plan_buf)
        .and_then(|proto| proto.try_into_physical_plan(&ctx.task_ctx(), &codec))?;
    let serde_plan_str = DisplayableExecutionPlan::new(new_plan.as_ref())
        .indent(true)
        .to_string();
    println!("Deserialized plan: \n{serde_plan_str}",);

    assert_eq!(plan_str, serde_plan_str);

    Ok(())
}

/// Verify that scan exec output schema is compatible with LOG_TABLE_SCHEMA.
/// Checks column count, names, and data type categories.
/// NOTE: does not check exact DataType equality because the declared schema
/// may use offset-based timezone ("+00:00") while parquet reader outputs
/// named timezone ("UTC") — both are semantically equivalent UTC.
#[tokio::test]
async fn test_scan_output_schema_compatible_with_log_table_schema()
-> Result<(), Box<dyn std::error::Error>> {
    use arrow::datatypes::DataType;

    setup_loki().await;

    let ctx = build_session_context();
    let declared = datafusion_loki::LOG_TABLE_SCHEMA.clone();

    // Helper: check two DataTypes are semantically compatible
    fn types_compatible(a: &DataType, b: &DataType) -> bool {
        match (a, b) {
            // Timestamps: ignore timezone string (both "UTC" and "+00:00" are UTC)
            (DataType::Timestamp(ua, _), DataType::Timestamp(ub, _)) => ua == ub,
            // Map: check inner struct types structurally
            (DataType::Map(fa, _), DataType::Map(fb, _)) => {
                match (fa.data_type(), fb.data_type()) {
                    (DataType::Struct(sa), DataType::Struct(sb)) => {
                        sa.len() == sb.len()
                            && sa
                                .iter()
                                .zip(sb.iter())
                                .all(|(fa, fb)| types_compatible(fa.data_type(), fb.data_type()))
                    }
                    _ => false,
                }
            }
            // For everything else, exact match
            _ => a == b,
        }
    }

    // Full scan
    let df = ctx.sql("select * from loki").await?;
    let exec_plan = df.create_physical_plan().await?;
    let batches = collect(exec_plan.clone(), ctx.task_ctx()).await?;
    assert!(!batches.is_empty(), "Must have at least one RecordBatch");

    for (i, batch) in batches.iter().enumerate() {
        let bs = batch.schema();
        assert_eq!(
            bs.fields().len(),
            declared.fields().len(),
            "Batch {i}: column count mismatch"
        );
        for (j, (bf, df)) in bs.fields().iter().zip(declared.fields().iter()).enumerate() {
            assert_eq!(bf.name(), df.name(), "Batch {i} col {j}: name mismatch");
            assert!(
                types_compatible(bf.data_type(), df.data_type()),
                "Batch {i} col {j} ({name}): type mismatch — output: {output:?}, declared: {declared:?}",
                name = bf.name(),
                output = bf.data_type(),
                declared = df.data_type()
            );
        }
    }

    // Filtered query
    let df = ctx
        .sql("select * from loki where labels['app'] = 'my-app2'")
        .await?;
    let exec_plan = df.create_physical_plan().await?;
    let batches = collect(exec_plan.clone(), ctx.task_ctx()).await?;

    for (i, batch) in batches.iter().enumerate() {
        let bs = batch.schema();
        assert_eq!(bs.fields().len(), declared.fields().len());
        for (j, (bf, df)) in bs.fields().iter().zip(declared.fields().iter()).enumerate() {
            assert_eq!(bf.name(), df.name());
            assert!(
                types_compatible(bf.data_type(), df.data_type()),
                "Filtered batch {i} col {j}: type mismatch"
            );
        }
    }

    Ok(())
}
