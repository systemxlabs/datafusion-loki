use std::sync::Arc;

use datafusion::{
    arrow::{
        array::RecordBatch,
        datatypes::Schema,
        ipc::{reader::StreamReader, writer::StreamWriter},
    },
    catalog::memory::{DataSourceExec, MemorySourceConfig},
    common::{internal_datafusion_err, internal_err, not_impl_err},
    datasource::source::DataSource,
    error::DataFusionError,
    execution::FunctionRegistry,
    physical_expr::LexOrdering,
    physical_plan::ExecutionPlan, prelude::SessionContext,
};
use datafusion_proto::physical_plan::{
    PhysicalExtensionCodec, from_proto::parse_physical_sort_exprs,
    to_proto::serialize_physical_sort_exprs,
};
use prost::Message;

use crate::{DFResult, LokiLogInsertExec, LokiLogScanExec, protobuf};

#[derive(Debug, Clone)]
pub struct LokiPhysicalCodec;

impl PhysicalExtensionCodec for LokiPhysicalCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        _registry: &dyn FunctionRegistry,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let loki_node = protobuf::LokiPhysicalPlanNode::decode(buf).map_err(|e| {
            internal_datafusion_err!("Failed to decode loki physical plan node: {e:?}")
        })?;
        let loki_plan = loki_node.loki_physical_plan_type.ok_or_else(|| {
            internal_datafusion_err!(
                "Failed to decode loki physical plan node due to physical plan type is none"
            )
        })?;

        match loki_plan {
            protobuf::loki_physical_plan_node::LokiPhysicalPlanType::Scan(proto) => {
                let projection = parse_projection(proto.projection.as_ref());
                let exec = LokiLogScanExec::try_new(
                    proto.endpoint,
                    proto.log_query,
                    proto.start,
                    proto.end,
                    projection,
                    proto.limit.map(|l| l as usize),
                )?;
                Ok(Arc::new(exec))
            }
            protobuf::loki_physical_plan_node::LokiPhysicalPlanType::Insert(proto) => {
                if inputs.len() != 1 {
                    return internal_err!("LokiLogInsertExec only support one input");
                }

                let input = inputs[0].clone();
                let exec = LokiLogInsertExec::try_new(input, proto.endpoint)?;
                Ok(Arc::new(exec))
            }
            protobuf::loki_physical_plan_node::LokiPhysicalPlanType::MemoryDatasource(proto) => {
                let partitions = parse_partitions(&proto.partitions)?;
                let schema = Schema::try_from(&proto.schema.unwrap())?;
                let projection = parse_projection(proto.projection.as_ref());

                let sort_information = proto
                    .sort_information
                    .iter()
                    .map(|sort_exprs| {
                        let sort_exprs = parse_physical_sort_exprs(
                            sort_exprs.physical_sort_expr_nodes.as_slice(),
                            &SessionContext::new(),
                            &schema,
                            self,
                        )?;
                        let lex_ordering =
                            LexOrdering::new(sort_exprs).expect("lex ordering is not empty");
                        Ok::<_, DataFusionError>(lex_ordering)
                    })
                    .collect::<Result<Vec<_>, _>>()?;

                let show_sizes = proto.show_sizes;
                let fetch = proto.fetch.map(|f| f as usize);
                let memory_source =
                    MemorySourceConfig::try_new(&partitions, Arc::new(schema), projection)?
                        .with_show_sizes(show_sizes)
                        .with_limit(fetch);

                let memory_source =
                    MemorySourceConfig::try_with_sort_information(memory_source, sort_information)?;
                Ok(DataSourceExec::from_data_source(memory_source))
            }
        }
    }

    fn try_encode(&self, node: Arc<dyn ExecutionPlan>, buf: &mut Vec<u8>) -> DFResult<()> {
        if let Some(exec) = node.as_any().downcast_ref::<LokiLogScanExec>() {
            let projection = serialize_projection(exec.projection.as_ref());

            let proto = protobuf::LokiPhysicalPlanNode {
                loki_physical_plan_type: Some(
                    protobuf::loki_physical_plan_node::LokiPhysicalPlanType::Scan(
                        protobuf::LokiLogScanExec {
                            endpoint: exec.endpoint.clone(),
                            log_query: exec.log_query.clone(),
                            start: exec.start,
                            end: exec.end,
                            projection,
                            limit: exec.limit.map(|l| l as i32),
                        },
                    ),
                ),
            };

            proto.encode(buf).map_err(|e| {
                internal_datafusion_err!("Failed to encode loki log scan exec plan: {e:?}")
            })?;
            Ok(())
        } else if let Some(exec) = node.as_any().downcast_ref::<LokiLogInsertExec>() {
            let proto = protobuf::LokiPhysicalPlanNode {
                loki_physical_plan_type: Some(
                    protobuf::loki_physical_plan_node::LokiPhysicalPlanType::Insert(
                        protobuf::LokiLogInsertExec {
                            endpoint: exec.endpoint.clone(),
                        },
                    ),
                ),
            };

            proto.encode(buf).map_err(|e| {
                internal_datafusion_err!("Failed to encode loki log scan exec plan: {e:?}")
            })?;
            Ok(())
        } else if let Some(exec) = node.as_any().downcast_ref::<DataSourceExec>() {
            let source = exec.data_source();
            if let Some(memory_source) = source.as_any().downcast_ref::<MemorySourceConfig>() {
                let proto_partitions = serialize_partitions(memory_source.partitions())?;
                let projection = serialize_projection(memory_source.projection().as_ref());
                let sort_information = memory_source
                    .sort_information()
                    .iter()
                    .map(|ordering| {
                        let sort_exprs = serialize_physical_sort_exprs(ordering.clone(), self)?;
                        Ok::<_, DataFusionError>(
                            datafusion_proto::protobuf::PhysicalSortExprNodeCollection {
                                physical_sort_expr_nodes: sort_exprs,
                            },
                        )
                    })
                    .collect::<Result<Vec<_>, _>>()?;

                let proto = protobuf::LokiPhysicalPlanNode {
                    loki_physical_plan_type: Some(
                        protobuf::loki_physical_plan_node::LokiPhysicalPlanType::MemoryDatasource(
                            protobuf::MemoryDatasourceNode {
                                partitions: proto_partitions,
                                schema: Some(memory_source.original_schema().try_into()?),
                                projection,
                                sort_information,
                                show_sizes: memory_source.show_sizes(),
                                fetch: memory_source.fetch().map(|f| f as u32),
                            },
                        ),
                    ),
                };

                proto.encode(buf).map_err(|e| {
                    internal_datafusion_err!("Failed to encode memory datasource node: {e:?}")
                })?;

                Ok(())
            } else {
                not_impl_err!(
                    "LokiPhysicalCodec only support encoding MemorySourceConfig, got {source:?}"
                )
            }
        } else {
            not_impl_err!(
                "LokiPhysicalCodec does not support encoding {}",
                node.name()
            )
        }
    }
}

fn serialize_projection(projection: Option<&Vec<usize>>) -> Option<protobuf::Projection> {
    projection.map(|p| protobuf::Projection {
        projection: p.iter().map(|n| *n as u32).collect(),
    })
}

fn parse_projection(projection: Option<&protobuf::Projection>) -> Option<Vec<usize>> {
    projection.map(|p| p.projection.iter().map(|n| *n as usize).collect())
}

fn serialize_partitions(partitions: &[Vec<RecordBatch>]) -> DFResult<Vec<Vec<u8>>> {
    let mut proto_partitions = vec![];
    for partition in partitions {
        if partition.is_empty() {
            proto_partitions.push(vec![]);
            continue;
        }
        let mut proto_partition = vec![];
        let mut stream_writer =
            StreamWriter::try_new(&mut proto_partition, &partition[0].schema())?;
        for batch in partition {
            stream_writer.write(batch)?;
        }
        stream_writer.finish()?;
        proto_partitions.push(proto_partition);
    }
    Ok(proto_partitions)
}

fn parse_partitions(proto_partitions: &[Vec<u8>]) -> DFResult<Vec<Vec<RecordBatch>>> {
    let mut partitions = vec![];
    for proto_partition in proto_partitions {
        if proto_partition.is_empty() {
            partitions.push(vec![]);
            continue;
        }
        let mut partition = vec![];
        let stream_reader = StreamReader::try_new(proto_partition.as_slice(), None)?;
        for batch in stream_reader {
            partition.push(batch?);
        }
        partitions.push(partition);
    }
    Ok(partitions)
}
