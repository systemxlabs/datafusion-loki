use std::sync::{Arc, LazyLock};

use datafusion::{
    arrow::datatypes::{DataType, Field, FieldRef, Schema, SchemaRef, TimeUnit},
    catalog::{Session, TableProvider},
    common::exec_err,
    datasource::TableType,
    error::DataFusionError,
    logical_expr::{TableProviderFilterPushDown, dml::InsertOp},
    physical_plan::ExecutionPlan,
    prelude::Expr,
};

use crate::{
    DFResult, LokiLogInsertExec, LokiLogScanExec, TimestampBound, expr_to_label_filter,
    expr_to_line_filter, parse_timestamp_bound,
};

pub static TIMESTAMP_FIELD_REF: LazyLock<FieldRef> = LazyLock::new(|| {
    Arc::new(Field::new(
        "timestamp",
        DataType::Timestamp(TimeUnit::Nanosecond, None),
        false,
    ))
});
pub static LABELS_FIELD_REF: LazyLock<FieldRef> = LazyLock::new(|| {
    let key_field = Field::new("keys", DataType::Utf8, false);
    let value_field = Field::new("values", DataType::Utf8, true); // 值允许为空
    let entry_struct = DataType::Struct(vec![key_field, value_field].into());
    let map_field = Arc::new(Field::new("entries", entry_struct, false));
    Arc::new(Field::new("labels", DataType::Map(map_field, false), true))
});
pub static LINE_FIELD_REF: LazyLock<FieldRef> =
    LazyLock::new(|| Arc::new(Field::new("line", DataType::Utf8, true)));

pub static LOG_TABLE_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        TIMESTAMP_FIELD_REF.clone(),
        LABELS_FIELD_REF.clone(),
        LINE_FIELD_REF.clone(),
    ]))
});

#[derive(Debug)]
pub struct LokiLogTable {
    pub endpoint: String,
    pub default_label: Option<String>,
}

impl LokiLogTable {
    pub fn try_new(endpoint: impl Into<String>) -> DFResult<Self> {
        let endpoint = endpoint.into();

        Ok(LokiLogTable {
            endpoint,
            default_label: None,
        })
    }

    pub fn with_default_label(mut self, default_label: Option<String>) -> Self {
        self.default_label = default_label;
        self
    }

    pub async fn check_connection(&self) -> DFResult<()> {
        let client = reqwest::Client::new();
        let resp = client
            .get(format!("{}/loki/api/v1/status/buildinfo", self.endpoint))
            .send()
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        if resp.status().is_success() {
            Ok(())
        } else {
            exec_err!("Failed to connect to loki with status {}", resp.status())
        }
    }
}

#[async_trait::async_trait]
impl TableProvider for LokiLogTable {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        LOG_TABLE_SCHEMA.clone()
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
            if let Some(label_filter) = expr_to_label_filter(filter) {
                label_filters.push(label_filter);
            } else if let Some(line_filter) = expr_to_line_filter(filter) {
                line_filters.push(line_filter);
            } else if let Some(timestamp_bound) = parse_timestamp_bound(filter) {
                match timestamp_bound {
                    TimestampBound::Start(v) => start = v,
                    TimestampBound::End(v) => end = v,
                }
            } else {
                return exec_err!("Unsupported filter: {filter}");
            }
        }

        if label_filters.is_empty() {
            if let Some(default_label) = &self.default_label {
                label_filters.push(format!("{default_label}=~\".+\""));
            } else {
                return exec_err!("No label filters or default label provided");
            }
        }

        let log_query = format!(
            "{{{}}} {}",
            label_filters.join(", "),
            line_filters.join(" ")
        );
        let exec = LokiLogScanExec::try_new(
            self.endpoint.clone(),
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
            if expr_to_label_filter(filter).is_some()
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

    async fn insert_into(
        &self,
        _state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        insert_op: InsertOp,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        match insert_op {
            InsertOp::Append => {}
            InsertOp::Overwrite | InsertOp::Replace => {
                return exec_err!("Only support append insert operation");
            }
        }

        let exec = LokiLogInsertExec::try_new(input, self.endpoint.clone())?;
        Ok(Arc::new(exec))
    }
}
