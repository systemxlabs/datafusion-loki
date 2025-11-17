use datafusion::{
    arrow::util::pretty::pretty_format_batches,
    physical_plan::{collect, display::DisplayableExecutionPlan},
    prelude::SessionContext,
};

use crate::{build_session_context, setup_loki};

pub async fn assert_sql_output(
    ctx: &SessionContext,
    sql: &str,
    expected_output: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let df = ctx.sql(sql).await?;
    let plan = df.create_physical_plan().await?;
    println!(
        "Plan: \n{}",
        DisplayableExecutionPlan::new(plan.as_ref()).indent(true)
    );

    let batches = collect(plan, ctx.task_ctx()).await?;
    let output = pretty_format_batches(&batches)?.to_string();
    println!("Output: \n{output}");

    assert_eq!(output, expected_output);
    Ok(())
}

pub async fn assert_sql(
    sql: &str,
    expected_output: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    setup_loki().await;
    let ctx = build_session_context();
    assert_sql_output(&ctx, sql, expected_output).await
}
