use std::{collections::BTreeMap, sync::Arc};

use datafusion::{
    arrow::{
        array::{
            ArrayRef, MapArray, MapBuilder, MapFieldNames, RecordBatch, RecordBatchOptions,
            StringArray, StringBuilder,
        },
        datatypes::{DataType, Field},
        util::pretty::pretty_format_batches,
    },
    physical_plan::{collect, display::DisplayableExecutionPlan},
    prelude::SessionContext,
};
use datafusion_loki::{DFResult, TIMESTAMP_FIELD_REF};

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
    let sorted_batches = sort_batch_map_field(batches);
    let output = pretty_format_batches(&sorted_batches)?.to_string();
    println!("Output: \n{output}");

    assert_eq!(output, expected_output);
    Ok(())
}

pub async fn assert_loki_output(
    sql: &str,
    expected_output: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    setup_loki().await;

    let ctx = build_session_context();
    let df = ctx.sql(sql).await?;
    let plan = df.create_physical_plan().await?;
    println!(
        "Plan: \n{}",
        DisplayableExecutionPlan::new(plan.as_ref()).indent(true)
    );

    let batches = collect(plan, ctx.task_ctx()).await?;
    let sorted_batches = sort_batch_map_field(batches);
    let mut sorted_batches = sort_record_batches(&sorted_batches, TIMESTAMP_FIELD_REF.name())?;

    for batch in sorted_batches.iter_mut() {
        let Ok(idx) = batch.schema().index_of(TIMESTAMP_FIELD_REF.name()) else {
            continue;
        };
        batch.remove_column(idx);
    }

    let output = pretty_format_batches(&sorted_batches)?.to_string();
    println!("Output: \n{output}");

    assert_eq!(output, expected_output);

    Ok(())
}

pub fn sort_record_batches(batches: &[RecordBatch], sort_col: &str) -> DFResult<Vec<RecordBatch>> {
    if batches.is_empty() {
        return Ok(vec![]);
    }
    let record = datafusion::arrow::compute::concat_batches(&batches[0].schema(), batches)?;

    let sort_col_idx = record.schema().index_of(sort_col)?;
    let sort_array = record.column(sort_col_idx);

    let indices = datafusion::arrow::compute::sort_to_indices(sort_array, None, None)?;

    let sorted_arrays: Vec<ArrayRef> = record
        .columns()
        .iter()
        .map(|col| datafusion::arrow::compute::take(col, &indices, None))
        .collect::<datafusion::arrow::error::Result<_>>()?;

    let options = RecordBatchOptions::new().with_row_count(Some(record.num_rows()));
    let batch = RecordBatch::try_new_with_options(record.schema(), sorted_arrays, &options)?;
    Ok(vec![batch])
}

pub fn sort_batch_map_field(batches: Vec<RecordBatch>) -> Vec<RecordBatch> {
    let mut sorted_batches = Vec::with_capacity(batches.len());
    for batch in batches {
        let mut sorted_arrays = Vec::with_capacity(batch.num_columns());
        for i in 0..batch.num_columns() {
            let array = batch.column(i);
            let field = batch.schema_ref().field(i);

            if let Some(map_array) = array.as_any().downcast_ref::<MapArray>() {
                let sorted_map_array = sort_map_array(map_array, field);
                sorted_arrays.push(Arc::new(sorted_map_array) as ArrayRef);
            } else {
                sorted_arrays.push(array.clone());
            }
        }
        let sorted_batch = RecordBatch::try_new(batch.schema(), sorted_arrays).unwrap();
        sorted_batches.push(sorted_batch);
    }
    sorted_batches
}

fn sort_map_array(arr: &MapArray, map_field: &Field) -> MapArray {
    let DataType::Map(map_inner_field, _) = map_field.data_type() else {
        panic!("Expected Map data type");
    };
    let entry_name = map_inner_field.name();
    let DataType::Struct(struct_fields) = map_inner_field.data_type() else {
        panic!("Expected Struct data type");
    };
    let key_field = struct_fields[0].clone();
    let value_field = struct_fields[1].clone();

    let map_field_names = MapFieldNames {
        entry: entry_name.clone(),
        key: key_field.name().clone(),
        value: value_field.name().clone(),
    };

    let mut builder = MapBuilder::new(
        Some(map_field_names),
        StringBuilder::new(),
        StringBuilder::new(),
    )
    .with_keys_field(key_field)
    .with_values_field(value_field);

    for map in arr.iter() {
        match map {
            Some(map) => {
                let keys_arr = map.column(0);
                let keys_arr = keys_arr.as_any().downcast_ref::<StringArray>().unwrap();
                let values_arr = map.column(1);
                let values_arr = values_arr.as_any().downcast_ref::<StringArray>().unwrap();

                let mut btree_map = BTreeMap::new();
                keys_arr
                    .iter()
                    .zip(values_arr.iter())
                    .for_each(|(key, value)| {
                        let key = key.unwrap();
                        btree_map.insert(key, value);
                    });

                for (key, value) in btree_map {
                    builder.keys().append_value(key);
                    builder.values().append_option(value);
                }

                builder.append(true).unwrap();
            }
            None => {
                builder.append(false).unwrap();
            }
        }
    }
    builder.finish()
}
