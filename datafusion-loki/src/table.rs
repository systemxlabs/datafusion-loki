use std::sync::{Arc, LazyLock};

use arrow::datatypes::{DataType, Field, FieldRef, Schema, SchemaRef, TimeUnit};
use datafusion_catalog::{Session, TableProvider};
use datafusion_common::{DataFusionError, exec_err};
use datafusion_expr::{Expr, TableProviderFilterPushDown, TableType, dml::InsertOp};
use datafusion_physical_plan::ExecutionPlan;

use crate::{
    DFResult, LokiLogInsertExec, LokiLogScanExec, TimestampBound, expr_to_label_filter,
    expr_to_line_filter, parse_timestamp_bound,
};

pub static TIMESTAMP_FIELD_REF: LazyLock<FieldRef> = LazyLock::new(|| {
    Arc::new(Field::new(
        "timestamp",
        DataType::Timestamp(TimeUnit::Nanosecond, Some("+00:00".into())),
        false,
    ))
});
pub static LABELS_FIELD_REF: LazyLock<FieldRef> = LazyLock::new(|| {
    let key_field = Field::new("key", DataType::Utf8, false);
    let value_field = Field::new("value", DataType::Utf8, false);
    let entry_struct = DataType::Struct(vec![key_field, value_field].into());
    let map_field = Arc::new(Field::new("key_value", entry_struct, false));
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

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use arrow::array::{MapArray, RecordBatch, StringArray, StructArray, TimestampNanosecondArray};
    use arrow::buffer::{OffsetBuffer, ScalarBuffer};
    use arrow::datatypes::{DataType, Field, Fields, TimeUnit};
    use datafusion_execution::TaskContext;
    use futures::TryStreamExt;
    use parquet::arrow::ArrowWriter;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use super::*;
    use crate::LOG_TABLE_SCHEMA;

    /// Build Parquet bytes matching Loki documented response schema:
    ///   timestamp: TIMESTAMP WITH TIME ZONE  (Arrow: Timestamp(ns, "UTC"))
    ///   labels:    MAP(VARCHAR, VARCHAR)      (Parquet std: key_value { key, value })
    ///   line:      VARCHAR
    fn build_loki_parquet_bytes() -> Vec<u8> {
        let ts_field = Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            false,
        );
        let key_field = Field::new("key", DataType::Utf8, false);
        let value_field = Field::new("value", DataType::Utf8, false);
        let entry_struct = DataType::Struct(Fields::from(vec![key_field, value_field]));
        let map_field = Arc::new(Field::new("key_value", entry_struct, false));
        let labels_field = Field::new(
            "labels",
            DataType::Map(map_field.clone(), false),
            true,
        );
        let line_field = Field::new("line", DataType::Utf8, true);
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            ts_field, labels_field, line_field,
        ]));

        let ts_arr = Arc::new(TimestampNanosecondArray::from(vec![
            Some(1_700_000_000_000_000_000i64),
            Some(1_700_000_000_001_000_000i64),
        ]));

        let keys0 = StringArray::from(vec!["app"]);
        let values0 = StringArray::from(vec!["test"]);
        let struct0 = StructArray::new(
            entry_struct.clone(),
            vec![Arc::new(keys0) as _, Arc::new(values0) as _],
            None,
        );
        let keys1 = StringArray::from(vec!["env"]);
        let values1 = StringArray::from(vec!["prod"]);
        let struct1 = StructArray::new(
            entry_struct.clone(),
            vec![Arc::new(keys1) as _, Arc::new(values1) as _],
            None,
        );

        let map_arr = Arc::new(
            MapArray::new(
                map_field,
                OffsetBuffer::new(ScalarBuffer::from(vec![0i32, 1, 2])),
                Arc::new(struct0) as _,
                arrow::buffer::NullBuffer::from_iter([false, false]),
            )
            .unwrap(),
        ) as arrow::array::ArrayRef;

        let line_arr = Arc::new(StringArray::from(vec![
            Some("log line 1"),
            Some("log line 2"),
        ]));

        let batch =
            RecordBatch::try_new(schema.clone(), vec![ts_arr, map_arr, line_arr]).unwrap();

        let mut buf = Vec::new();
        let cursor = Cursor::new(&mut buf);
        let mut writer = ArrowWriter::try_new(cursor, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        buf
    }

    /// Verify LOG_TABLE_SCHEMA fields match Loki's documented Parquet format:
    /// - Timestamp with offset-based timezone (no chrono-tz required)
    /// - Map with key_value/key/value naming (Parquet standard)
    /// - Map value is non-nullable (matching parquet reader output)
    #[test]
    fn test_log_table_schema_declaration() {
        let declared = &*LOG_TABLE_SCHEMA;
        assert_eq!(declared.fields().len(), 3);

        // Timestamp
        let ts_field = declared.field(0);
        assert_eq!(ts_field.name(), "timestamp");
        assert!(!ts_field.is_nullable());
        match ts_field.data_type() {
            DataType::Timestamp(TimeUnit::Nanosecond, tz) => {
                assert!(tz.is_some(), "Timestamp timezone must be set");
                let s = tz.as_ref().unwrap();
                assert!(
                    s.starts_with('+') || s.starts_with('-'),
                    "Timezone must be offset-based (+00:00), got: {}",
                    s
                );
            }
            other => panic!("Expected Timestamp(Nanosecond, _), got {:?}", other),
        }

        // Labels (Map)
        let labels_field = declared.field(1);
        assert_eq!(labels_field.name(), "labels");
        match labels_field.data_type() {
            DataType::Map(map_field, _sorted) => {
                assert_eq!(
                    map_field.name(),
                    "key_value",
                    "Map inner struct must be 'key_value'"
                );
                match map_field.data_type() {
                    DataType::Struct(fields) => {
                        assert_eq!(fields.len(), 2);
                        assert_eq!(fields[0].name(), "key");
                        assert!(!fields[0].is_nullable(), "key must be non-null");
                        assert_eq!(fields[1].name(), "value");
                        assert!(!fields[1].is_nullable(), "value must be non-null");
                    }
                    other => panic!("Expected Struct inside Map, got {:?}", other),
                }
            }
            other => panic!("Expected Map for labels, got {:?}", other),
        }

        // Line
        let line_field = declared.field(2);
        assert_eq!(line_field.name(), "line");
        match line_field.data_type() {
            DataType::Utf8 => {}
            other => panic!("Expected Utf8 for line, got {:?}", other),
        }
    }

    /// Integration test: mock Loki HTTP endpoint returning Parquet,
    /// scan through LokiLogScanExec, verify output stream schema
    /// exactly matches LOG_TABLE_SCHEMA.
    #[tokio::test]
    async fn test_scan_output_schema_matches_log_table_schema() {
        let parquet_bytes = build_loki_parquet_bytes();

        // Mini HTTP server serving Parquet bytes
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .unwrap();
        let addr = listener.local_addr().unwrap();
        let server_url = format!("http://{}", addr);

        let server_task = tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut buf = vec![0u8; 4096];
                let n = socket.read(&mut buf).await.unwrap();
                let _request = String::from_utf8_lossy(&buf[..n]);
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/vnd.apache.parquet\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    parquet_bytes.len()
                );
                let mut full = response.into_bytes();
                full.extend_from_slice(&parquet_bytes);
                socket.write_all(&full).await.unwrap();
                socket.flush().await.unwrap();
            }
        });

        // Build table with default label to satisfy scan requirement
        let table = LokiLogTable::try_new(&server_url)
            .unwrap()
            .with_default_label(Some("app=~\"test\"".to_string()));

        let exec = table
            .scan(
                &MockSession,
                None,
                &[],
                Some(10),
            )
            .await
            .unwrap();

        let task_ctx = Arc::new(TaskContext::default());
        let mut stream = exec.execute(0, task_ctx).unwrap();

        let declared_schema = &*LOG_TABLE_SCHEMA;
        let mut row_count = 0;
        let mut batch_count = 0;
        while let Some(batch) = stream.next().await {
            let batch = batch.unwrap();
            batch_count += 1;
            row_count += batch.num_rows();
            assert_eq!(
                batch.schema().as_ref(),
                declared_schema,
                "Batch {batch_count}: output schema must match LOG_TABLE_SCHEMA\nexpected: {declared_schema:?}\nactual:   {:?}",
                batch.schema()
            );
        }

        assert!(batch_count > 0, "Must produce at least 1 RecordBatch");
        assert_eq!(row_count, 2, "Must have 2 rows");

        server_task.abort();
    }

    /// Mock Session used for testing `LokiLogTable::scan()`.
    struct MockSession;

    impl Session for MockSession {
        fn table_exists(&self, _name: impl AsRef<str>) -> bool {
            false
        }
    }
}
