use datafusion::{
    arrow::util::pretty::pretty_format_batches,
    physical_plan::{collect, display::DisplayableExecutionPlan},
    prelude::SessionContext,
};

pub async fn assert_sql_output(ctx: &SessionContext, sql: &str, expected_output: &str) {
    let df = ctx.sql(sql).await.unwrap();
    let plan = df.create_physical_plan().await.unwrap();
    println!(
        "Plan: \n{}",
        DisplayableExecutionPlan::new(plan.as_ref()).indent(true)
    );

    let batches = collect(plan, ctx.task_ctx()).await.unwrap();
    let output = pretty_format_batches(&batches).unwrap().to_string();
    println!("Output: \n{output}");

    assert_eq!(output, expected_output);
}
