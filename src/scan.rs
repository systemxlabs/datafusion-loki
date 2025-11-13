use std::{any::Any, sync::Arc};

use datafusion::{
    common::{Statistics, project_schema},
    error::DataFusionError,
    execution::{SendableRecordBatchStream, TaskContext},
    physical_expr::EquivalenceProperties,
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
        display::ProjectSchemaDisplay,
        execution_plan::{Boundedness, EmissionType},
    },
};

use crate::{DFResult, make_table_schema};

#[derive(Debug)]
pub struct LokiLogScanExec {
    pub endpoint: String,
    pub labels: Vec<String>,
    pub log_query: String,
    pub start: Option<i64>,
    pub end: Option<i64>,
    pub projection: Option<Vec<usize>>,
    pub limit: Option<usize>,
    plan_properties: PlanProperties,
}

impl LokiLogScanExec {
    pub fn try_new(
        endpoint: String,
        labels: Vec<String>,
        log_query: String,
        start: Option<i64>,
        end: Option<i64>,
        projection: Option<Vec<usize>>,
        limit: Option<usize>,
    ) -> DFResult<Self> {
        let schema = make_table_schema(&labels);
        let projected_schema = project_schema(&schema, projection.as_ref())?;
        let plan_properties = PlanProperties::new(
            EquivalenceProperties::new(projected_schema),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Ok(LokiLogScanExec {
            endpoint,
            labels,
            log_query,
            start,
            end,
            projection,
            limit,
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
        todo!()
    }

    fn partition_statistics(&self, partition: Option<usize>) -> DFResult<Statistics> {
        todo!()
    }

    fn with_fetch(&self, limit: Option<usize>) -> Option<Arc<dyn ExecutionPlan>> {
        Self::try_new(
            self.endpoint.clone(),
            self.labels.clone(),
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
