use std::sync::Arc;

use datafusion::arrow::{
    array::{MapBuilder, RecordBatch, StringBuilder},
    datatypes::{DataType, Field, Schema},
    util::pretty::pretty_format_batches,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let key_field = Arc::new(Field::new("keys", DataType::Utf8, false));
    let value_field = Arc::new(Field::new("values", DataType::Utf8, true)); // 值允许为空
    let entry_struct = DataType::Struct(vec![key_field.clone(), value_field.clone()].into());
    let map_field = Arc::new(Field::new("entries", entry_struct, false));
    let map_field = Arc::new(Field::new("labels", DataType::Map(map_field, false), true));

    let schema = Arc::new(Schema::new(vec![map_field]));

    let mut map_builder = MapBuilder::new(None, StringBuilder::new(), StringBuilder::new())
        .with_keys_field(key_field)
        .with_values_field(value_field);
    map_builder.keys().append_value("name");
    map_builder.values().append_value("John");
    map_builder.append(true);
    map_builder.keys().append_value("name");
    map_builder.values().append_value("John");
    map_builder.keys().append_value("age");
    map_builder.values().append_value("12");
    map_builder.append(true);
    let map_array = map_builder.finish();

    let batch = RecordBatch::try_new(schema, vec![Arc::new(map_array)])?;

    println!("{}", pretty_format_batches(&[batch])?);
    Ok(())
}
