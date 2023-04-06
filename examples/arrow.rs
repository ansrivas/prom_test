use arrow::array::{Float64Array, Int32Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

fn main() {
    // Create a schema with two fields: "field1" of type Int32 and "field2" of type Float64
    let schema = Arc::new(Schema::new(vec![
        Field::new("field1", DataType::Int32, false),
        Field::new("field2", DataType::Float64, false),
    ]));

    // Create two RecordBatches with different data
    let batch1 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(Float64Array::from(vec![1.1, 2.2, 3.3])),
        ],
    )
    .unwrap();

    let batch2 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![4, 5, 6])),
            Arc::new(Float64Array::from(vec![4.4, 5.5, 6.6])),
        ],
    )
    .unwrap();

    // Merge the two RecordBatches into a single RecordBatch
    let merged_batch = arrow::compute::concat_batches(&schema.clone(), &[batch1, batch2]).unwrap();

    // Print the merged RecordBatch
    println!("{:?}", merged_batch);
}
