use std::{any::Any, io::Cursor, pin::Pin, sync::Arc};

use datafusion::{
    arrow::array::RecordBatch,
    common::project_schema,
    error::DataFusionError,
    execution::{SendableRecordBatchStream, TaskContext},
    physical_expr::EquivalenceProperties,
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
        display::ProjectSchemaDisplay,
        execution_plan::{Boundedness, EmissionType},
        stream::RecordBatchStreamAdapter,
    },
};
use futures::{Stream, StreamExt, TryStreamExt};
use parquet::arrow::{ParquetRecordBatchStreamBuilder, ProjectionMask};
use reqwest::{Client, RequestBuilder};

use crate::{DFResult, LOG_TABLE_SCHEMA};

#[derive(Debug)]
pub struct LokiLogScanExec {
    pub endpoint: String,
    pub log_query: String,
    pub start: Option<i64>,
    pub end: Option<i64>,
    pub projection: Option<Vec<usize>>,
    pub limit: Option<usize>,
    client: Client,
    plan_properties: PlanProperties,
}

impl LokiLogScanExec {
    pub fn try_new(
        endpoint: String,
        log_query: String,
        start: Option<i64>,
        end: Option<i64>,
        projection: Option<Vec<usize>>,
        limit: Option<usize>,
    ) -> DFResult<Self> {
        let projected_schema = project_schema(&LOG_TABLE_SCHEMA, projection.as_ref())?;
        let plan_properties = PlanProperties::new(
            EquivalenceProperties::new(projected_schema),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        let client = Client::builder()
            .build()
            .map_err(|e| DataFusionError::Plan(format!("Failed to build http client: {e}")))?;
        Ok(LokiLogScanExec {
            endpoint,
            log_query,
            start,
            end,
            projection,
            limit,
            client,
            plan_properties,
        })
    }
}

impl ExecutionPlan for LokiLogScanExec {
    fn name(&self) -> &str {
        "LokiLogScanExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.plan_properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Execution(format!(
                "LokiLogScanExec does not support multiple partitions"
            )));
        }
        let mut query = Vec::new();
        if !self.log_query.is_empty() {
            query.push(("query", self.log_query.clone()));
        }
        if let Some(start) = self.start {
            query.push(("start", start.to_string()));
        }
        if let Some(end) = self.end {
            query.push(("end", end.to_string()));
        }
        if let Some(limit) = self.limit {
            query.push(("limit", limit.to_string()));
        }
        let req_builder = self
            .client
            .get(format!("{}/loki/api/v1/query_range", self.endpoint))
            .header("Accept", "application/vnd.apache.parquet")
            .query(&query);

        let fut = fetch_log_stream(req_builder, self.projection.clone());
        let stream = futures::stream::once(fut).try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }

    fn with_fetch(&self, limit: Option<usize>) -> Option<Arc<dyn ExecutionPlan>> {
        Self::try_new(
            self.endpoint.clone(),
            self.log_query.clone(),
            self.start,
            self.end,
            self.projection.clone(),
            limit,
        )
        .ok()
        .map(|exec| Arc::new(exec) as Arc<dyn ExecutionPlan>)
    }

    fn fetch(&self) -> Option<usize> {
        self.limit
    }
}

impl DisplayAs for LokiLogScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "LokiLogScanExec: endpoint={}, query={}",
            self.endpoint, self.log_query
        )?;
        if let Some(start) = self.start {
            write!(f, ", start={}", start)?;
        }
        if let Some(end) = self.end {
            write!(f, ", end={}", end)?;
        }
        if self.projection.is_some() {
            let projected_schema = self.schema();
            write!(
                f,
                ", projection={}",
                ProjectSchemaDisplay(&projected_schema)
            )?;
        }
        if let Some(limit) = self.limit {
            write!(f, ", limit={limit}")?;
        }
        Ok(())
    }
}

async fn fetch_log_stream(
    req_builder: RequestBuilder,
    projection: Option<Vec<usize>>,
) -> DFResult<Pin<Box<dyn Stream<Item = DFResult<RecordBatch>> + Send>>> {
    let resp = req_builder
        .send()
        .await
        .map_err(|e| DataFusionError::Execution(format!("Failed to send request to loki: {e}")))?;
    let status = resp.status();
    if !status.is_success() {
        let url = resp.url().clone();
        let text = resp.text().await;
        println!("LWZTEST resp text: {text:?}");
        return Err(DataFusionError::Execution(format!(
            "Request to logi failed with status {}, url: {}",
            status, url
        )));
    }
    let bytes = resp.bytes().await.map_err(|e| {
        DataFusionError::Execution(format!("Failed to get response body as bytes: {e}"))
    })?;
    let cursor = Cursor::new(bytes);

    let builder = ParquetRecordBatchStreamBuilder::new(cursor).await?;
    let parquet_schema = builder.parquet_schema();

    let projection_mask = match projection {
        Some(proj) => ProjectionMask::roots(parquet_schema, proj),
        None => ProjectionMask::all(),
    };

    let stream = builder
        .with_batch_size(4096)
        .with_projection(projection_mask)
        .build()?
        .map_err(|e| DataFusionError::ParquetError(Box::new(e)))
        .boxed();

    Ok(stream)
}
