use std::sync::Arc;

use datafusion::{
    common::{internal_datafusion_err, internal_err, not_impl_err},
    execution::FunctionRegistry,
    physical_plan::ExecutionPlan,
};
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
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
