use std::{
    collections::HashMap,
    sync::{Arc, LazyLock},
};

use datafusion::{
    arrow::{
        array::{
            Array, ArrayRef, Int64Array, MapArray, RecordBatch, StringArray, StructArray,
            TimestampNanosecondArray,
        },
        datatypes::{DataType, Field, Schema, SchemaRef},
    },
    common::{plan_err, stats::Precision},
    error::DataFusionError,
    execution::{SendableRecordBatchStream, TaskContext},
    physical_expr::EquivalenceProperties,
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, Partitioning,
        PlanProperties, stream::RecordBatchStreamAdapter,
    },
};
use futures::StreamExt;
use reqwest::Client;
use serde::{Deserialize, Serialize};

use crate::{DFResult, LOG_TABLE_SCHEMA};

pub static COUNT_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![Field::new(
        "count",
        DataType::Int64,
        false,
    )]))
});

#[derive(Debug)]
pub struct LokiLogInsertExec {
    pub input: Arc<dyn ExecutionPlan>,
    pub endpoint: String,
    client: Client,
    plan_properties: PlanProperties,
}

impl LokiLogInsertExec {
    pub fn try_new(input: Arc<dyn ExecutionPlan>, endpoint: String) -> DFResult<Self> {
        if input.schema() != LOG_TABLE_SCHEMA.clone() {
            return plan_err!("input exec schema not matched: {:?}", input.schema());
        }

        let plan_properties = PlanProperties::new(
            EquivalenceProperties::new(COUNT_SCHEMA.clone()),
            Partitioning::UnknownPartitioning(1),
            input.pipeline_behavior(),
            input.boundedness(),
        );

        let client = Client::builder()
            .build()
            .map_err(|e| DataFusionError::Plan(format!("Failed to build http client: {e}")))?;

        Ok(Self {
            input,
            endpoint,
            client,
            plan_properties,
        })
    }
}

impl ExecutionPlan for LokiLogInsertExec {
    fn name(&self) -> &str {
        "LokiLogInsertExec"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.plan_properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let input = children[0].clone();
        let exec = Self::try_new(input, self.endpoint.clone())?;
        Ok(Arc::new(exec))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let mut input_stream = self.input.execute(partition, context)?;

        let endpoint = self.endpoint.clone();
        let client = self.client.clone();

        let stream = futures::stream::once(async move {
            let mut count = 0;
            while let Some(batch) = input_stream.next().await {
                let batch = batch?;
                push_logs(&endpoint, &client, &batch).await?;
                count += batch.num_rows();
            }
            make_result_batch(count as i64)
        })
        .boxed();

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            COUNT_SCHEMA.clone(),
            stream,
        )))
    }
}

impl DisplayAs for LokiLogInsertExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "LokiLogInsertExec: endpoint={}", self.endpoint)?;
        if let Ok(stats) = self.input.partition_statistics(None) {
            match stats.num_rows {
                Precision::Exact(rows) => write!(f, ", rows={rows}")?,
                Precision::Inexact(rows) => write!(f, ", rows~={rows}")?,
                Precision::Absent => {}
            }
        }
        Ok(())
    }
}

fn make_result_batch(count: i64) -> DFResult<RecordBatch> {
    let array = Arc::new(Int64Array::from(vec![count])) as ArrayRef;
    let batch = RecordBatch::try_new(COUNT_SCHEMA.clone(), vec![array])?;
    Ok(batch)
}

async fn push_logs(endpoint: &str, client: &Client, batch: &RecordBatch) -> DFResult<()> {
    let log_streams = build_log_streams(batch)?;
    let resp = client
        .post(format!("{endpoint}/loki/api/v1/push"))
        .json(&log_streams)
        .send()
        .await
        .map_err(|e| {
            DataFusionError::Execution(format!("Failed to send push request to loki: {e}"))
        })?;
    let status = resp.status();
    if !status.is_success() {
        let text = resp.text().await.ok();
        let with_text = if let Some(t) = text {
            format!(", text: {t}")
        } else {
            "".to_string()
        };
        return Err(DataFusionError::Execution(format!(
            "Failed to send push request to loki with status {status}{with_text}",
        )));
    }
    Ok(())
}

fn build_log_streams(batch: &RecordBatch) -> DFResult<LogStreams> {
    let timestamp_arr = batch.column(0);
    let timestamp_arr = timestamp_arr
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .ok_or_else(|| {
            DataFusionError::Execution("Failed to downcast timestamp array".to_string())
        })?;
    let labels_arr = batch.column(1);
    let labels_arr = labels_arr
        .as_any()
        .downcast_ref::<MapArray>()
        .ok_or_else(|| DataFusionError::Execution("Failed to downcast labels array".to_string()))?;
    let line_arr = batch.column(2);
    let line_arr = line_arr
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| DataFusionError::Execution("Failed to downcast line array".to_string()))?;

    let streams = timestamp_arr
        .iter()
        .zip(labels_arr.iter())
        .zip(line_arr.iter())
        .map(|((timestamp, labels), line)| {
            let timestamp = timestamp.ok_or_else(|| {
                DataFusionError::Execution("timestamp should not be null".to_string())
            })?;
            let label_map = if let Some(labels) = labels {
                struct_arr_to_map(&labels)?
            } else {
                HashMap::new()
            };
            let line = line.map(|s| s.to_string()).unwrap_or_default();
            Ok::<_, DataFusionError>(LogStream {
                stream: label_map,
                values: vec![[timestamp.to_string(), line.to_string()]],
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(LogStreams { streams })
}

fn struct_arr_to_map(arr: &StructArray) -> DFResult<HashMap<String, String>> {
    let keys_arr = arr.column(0);
    let keys_arr = keys_arr
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| DataFusionError::Execution("Failed to downcast keys array".to_string()))?;
    let values_arr = arr.column(1);
    let values_arr = values_arr
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| DataFusionError::Execution("Failed to downcast values array".to_string()))?;

    let mut map = HashMap::new();
    keys_arr
        .iter()
        .zip(values_arr.iter())
        .map(|(key, value)| {
            let key = key.ok_or_else(|| {
                DataFusionError::Execution("label key should not be null".to_string())
            })?;
            let value = value.map(|v| v.to_string()).unwrap_or_default();
            map.insert(key.to_string(), value);
            Ok::<_, DataFusionError>(())
        })
        .collect::<Result<Vec<_>, _>>()?;

    Ok(map)
}

#[derive(Debug, Serialize, Deserialize)]
struct LogStreams {
    streams: Vec<LogStream>,
}

#[derive(Debug, Serialize, Deserialize)]
struct LogStream {
    stream: HashMap<String, String>,
    values: Vec<[String; 2]>,
}
