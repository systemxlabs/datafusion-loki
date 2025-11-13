use std::sync::{Arc, LazyLock};

use datafusion::{
    arrow::datatypes::{DataType, Field, FieldRef, Schema, SchemaRef, TimeUnit},
    catalog::{Session, TableProvider},
    datasource::TableType,
    error::DataFusionError,
    logical_expr::TableProviderFilterPushDown,
    physical_plan::ExecutionPlan,
    prelude::Expr,
};

use crate::{
    DFResult, LokiLogScanExec, TimestampBound, expr_to_label_filter, expr_to_line_filter,
    parse_timestamp_bound,
};

#[derive(Debug)]
pub struct LokiLogTable {
    pub endpoint: String,
    pub labels: Vec<String>,
    pub schema: SchemaRef,
}

pub static TIMESTAMP_FIELD_REF: LazyLock<FieldRef> = LazyLock::new(|| {
    Arc::new(Field::new(
        "timestamp",
        DataType::Timestamp(TimeUnit::Nanosecond, None),
        false,
    ))
});
pub static LABELS_FIELD_REF: LazyLock<FieldRef> = LazyLock::new(|| {
    let key_field = Field::new("keys", DataType::Utf8, false);
    let value_field = Field::new("values", DataType::Int32, true); // 值允许为空
    let entry_struct = DataType::Struct(vec![key_field, value_field].into());
    let map_field = Arc::new(Field::new("map_entries", entry_struct, false));
    Arc::new(Field::new("labels", DataType::Map(map_field, false), true))
});
pub static LINE_FIELD_REF: LazyLock<FieldRef> =
    LazyLock::new(|| Arc::new(Field::new("line", DataType::Utf8, true)));

pub fn make_table_schema(labels: &[String]) -> SchemaRef {
    let mut fields = vec![TIMESTAMP_FIELD_REF.clone()];
    for label in labels.iter() {
        fields.push(Arc::new(Field::new(label, DataType::Utf8, true)));
    }
    fields.push(LINE_FIELD_REF.clone());

    Arc::new(Schema::new(fields))
}

impl LokiLogTable {
    pub fn try_new(endpoint: String, labels: Vec<String>) -> DFResult<Self> {
        if labels.contains(TIMESTAMP_FIELD_REF.name()) || labels.contains(LINE_FIELD_REF.name()) {
            return Err(DataFusionError::Plan(format!(
                "Labels should not contains {} or {}",
                TIMESTAMP_FIELD_REF.name(),
                LINE_FIELD_REF.name()
            )));
        }

        let schema = make_table_schema(&labels);

        Ok(LokiLogTable {
            endpoint,
            labels,
            schema,
        })
    }
}

#[async_trait::async_trait]
impl TableProvider for LokiLogTable {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let mut label_filters = Vec::with_capacity(filters.len());
        let mut line_filters = Vec::with_capacity(filters.len());
        let mut start = None;
        let mut end = None;
        for filter in filters {
            if let Some(label_filter) = expr_to_label_filter(filter, &self.labels) {
                label_filters.push(label_filter);
            } else if let Some(line_filter) = expr_to_line_filter(filter) {
                line_filters.push(line_filter);
            } else if let Some(timestamp_bound) = parse_timestamp_bound(filter) {
                match timestamp_bound {
                    TimestampBound::Start(v) => start = v,
                    TimestampBound::End(v) => end = v,
                }
            } else {
                return Err(DataFusionError::Execution(format!(
                    "Unsupported filter: {filter}"
                )));
            }
        }

        let log_query = format!("{} {}", label_filters.join(", "), line_filters.join(" "));
        let exec = LokiLogScanExec::try_new(
            self.endpoint.clone(),
            self.labels.clone(),
            log_query,
            start,
            end,
            projection.cloned(),
            limit,
        )?;
        Ok(Arc::new(exec))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DFResult<Vec<TableProviderFilterPushDown>> {
        let mut pushdown = Vec::with_capacity(filters.len());
        for filter in filters {
            if expr_to_label_filter(filter, &self.labels).is_some()
                || expr_to_line_filter(filter).is_some()
                || parse_timestamp_bound(filter).is_some()
            {
                pushdown.push(TableProviderFilterPushDown::Exact);
            } else {
                pushdown.push(TableProviderFilterPushDown::Unsupported);
            }
        }
        Ok(pushdown)
    }
}
