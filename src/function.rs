use std::{any::Any, sync::Arc};

use datafusion::{
    arrow::{
        array::{Array, ArrayRef, Capacities, MapArray, MutableArrayData, make_array},
        datatypes::{DataType, Fields},
    },
    common::{cast::as_map_array, exec_err, internal_err, utils::take_function_args},
    logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility},
    scalar::ScalarValue,
};

use crate::DFResult;

#[derive(Debug)]
pub struct MapGet {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for MapGet {
    fn default() -> Self {
        Self::new()
    }
}

impl MapGet {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
            aliases: vec![],
        }
    }
}

impl ScalarUDFImpl for MapGet {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "map_get"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> DFResult<DataType> {
        let [map_type, _] = take_function_args(self.name(), arg_types)?;
        let map_fields = get_map_entry_field(map_type)?;
        let value_type = map_fields.last().unwrap().data_type().clone();
        Ok(value_type)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        make_scalar_function(map_extract_inner)(&args.args)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> DFResult<Vec<DataType>> {
        let [map_type, _] = take_function_args(self.name(), arg_types)?;

        let field = get_map_entry_field(map_type)?;
        Ok(vec![
            map_type.clone(),
            field.first().unwrap().data_type().clone(),
        ])
    }
}

pub(crate) fn get_map_entry_field(data_type: &DataType) -> DFResult<&Fields> {
    match data_type {
        DataType::Map(field, _) => {
            let field_data_type = field.data_type();
            match field_data_type {
                DataType::Struct(fields) => Ok(fields),
                _ => {
                    internal_err!("Expected a Struct type, got {:?}", field_data_type)
                }
            }
        }
        _ => internal_err!("Expected a Map type, got {:?}", data_type),
    }
}

/// array function wrapper that differentiates between scalar (length 1) and array.
pub(crate) fn make_scalar_function<F>(
    inner: F,
) -> impl Fn(&[ColumnarValue]) -> DFResult<ColumnarValue>
where
    F: Fn(&[ArrayRef]) -> DFResult<ArrayRef>,
{
    move |args: &[ColumnarValue]| {
        // first, identify if any of the arguments is an Array. If yes, store its `len`,
        // as any scalar will need to be converted to an array of len `len`.
        let len = args
            .iter()
            .fold(Option::<usize>::None, |acc, arg| match arg {
                ColumnarValue::Scalar(_) => acc,
                ColumnarValue::Array(a) => Some(a.len()),
            });

        let is_scalar = len.is_none();

        let args = ColumnarValue::values_to_arrays(args)?;

        let result = (inner)(&args);

        if is_scalar {
            // If all inputs are scalar, keeps output as scalar
            let result = result.and_then(|arr| ScalarValue::try_from_array(&arr, 0));
            result.map(ColumnarValue::Scalar)
        } else {
            result.map(ColumnarValue::Array)
        }
    }
}

fn general_map_extract_inner(
    map_array: &MapArray,
    query_keys_array: &dyn Array,
) -> DFResult<ArrayRef> {
    let keys = map_array.keys();
    let mut offsets = vec![0_i32];

    let values = map_array.values();
    let original_data = values.to_data();
    let capacity = Capacities::Array(original_data.len());

    let mut mutable = MutableArrayData::with_capacities(vec![&original_data], true, capacity);

    for (row_index, offset_window) in map_array.value_offsets().windows(2).enumerate() {
        let start = offset_window[0] as usize;
        let end = offset_window[1] as usize;
        let len = end - start;

        let query_key = query_keys_array.slice(row_index, 1);

        let value_index =
            (0..len).find(|&i| keys.slice(start + i, 1).as_ref() == query_key.as_ref());

        match value_index {
            Some(index) => {
                mutable.extend(0, start + index, start + index + 1);
            }
            None => {
                mutable.extend_nulls(1);
            }
        }
        offsets.push(offsets[row_index] + 1);
    }

    let data = mutable.freeze();

    Ok(Arc::new(make_array(data)))
}

fn map_extract_inner(args: &[ArrayRef]) -> DFResult<ArrayRef> {
    let [map_arg, key_arg] = take_function_args("map_extract", args)?;

    let map_array = match map_arg.data_type() {
        DataType::Map(_, _) => as_map_array(&map_arg)?,
        _ => return exec_err!("The first argument in map_get must be a map"),
    };

    let key_type = map_array.key_type();

    if key_type != key_arg.data_type() {
        return exec_err!(
            "The key type {} does not match the map key type {}",
            key_arg.data_type(),
            key_type
        );
    }

    general_map_extract_inner(map_array, key_arg)
}
