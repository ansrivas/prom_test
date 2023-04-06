use datafusion::arrow::array::StringArray;
use datafusion::arrow::{
    array::ArrayRef, array::Float32Array, datatypes::DataType, record_batch::RecordBatch,
};
use datafusion::from_slice::FromSlice;
use datafusion::{error::Result, physical_plan::Accumulator};
use datafusion::{logical_expr::Volatility, prelude::*, scalar::ScalarValue};
use datafusion_expr::create_udaf;
use std::sync::Arc;

// create local session context with an in-memory table
fn create_context() -> Result<SessionContext> {
    use datafusion::arrow::datatypes::{Field, Schema};
    use datafusion::datasource::MemTable;
    // define a schema.
    let schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, true),
        Field::new("value", DataType::Float32, true),
    ]));

    // define data in two partitions
    let batch1 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from_slice(["joy", "jim", "john"])),
            Arc::new(Float32Array::from_slice([2.0, 4.0, 8.0])),
        ],
    )?;
    let batch2 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from_slice(["joy", "jim"])),
            Arc::new(Float32Array::from_slice([64.0, 32.0])),
        ],
    )?;

    // declare a new context. In spark API, this corresponds to a new spark SQLsession
    let ctx = SessionContext::new();

    // declare a table in memory. In spark API, this corresponds to createDataFrame(...).
    let provider = MemTable::try_new(schema, vec![vec![batch1], vec![batch2]])?;
    ctx.register_table("t", Arc::new(provider))?;
    Ok(ctx)
}

/// A UDAF has state across multiple rows, and thus we require a `struct` with that state.
#[derive(Debug)]
struct SumMean {
    total: f64,
}

impl SumMean {
    // how the struct is initialized
    pub fn new() -> Self {
        SumMean { total: 0.0 }
    }
}

// UDAFs are built using the trait `Accumulator`, that offers DataFusion the necessary functions
// to use them.
impl Accumulator for SumMean {
    // This function serializes our state to `ScalarValue`, which DataFusion uses
    // to pass this state between execution stages.
    // Note that this can be arbitrary data.
    fn state(&self) -> Result<Vec<ScalarValue>> {
        println!("state: called");
        Ok(vec![ScalarValue::from(self.total)])
    }

    // DataFusion expects this function to return the final value of this aggregator.
    // in this case, this is the formula of the geometric mean
    fn evaluate(&self) -> Result<ScalarValue> {
        println!("evaluate: called");
        let value = self.total;
        Ok(ScalarValue::from(value))
    }

    // DataFusion calls this function to update the accumulator's state for a batch
    // of inputs rows. In this case the product is updated with values from the first column
    // and the count is updated based on the row count
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        println!("update_batch: called, values: {:?}", values);
        if values.is_empty() {
            return Ok(());
        }
        let arr = &values[0];
        (0..arr.len()).try_for_each(|index| {
            let v = ScalarValue::try_from_array(arr, index)?;

            if let ScalarValue::Float64(Some(value)) = v {
                self.total += value;
            } else {
                unreachable!("")
            }
            Ok(())
        })
    }

    // Optimization hint: this trait also supports `update_batch` and `merge_batch`,
    // that can be used to perform these operations on arrays instead of single values.
    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        println!("merge_batch: called, states: {:?}", states);
        if states.is_empty() {
            return Ok(());
        }
        let arr = &states[0];
        (0..arr.len()).try_for_each(|index| {
            let v = states
                .iter()
                .map(|array| ScalarValue::try_from_array(array, index))
                .collect::<Result<Vec<_>>>()?;
            if let ScalarValue::Float64(Some(prod)) = &v[0] {
                self.total += prod;
            } else {
                unreachable!("")
            }
            Ok(())
        })
    }

    fn size(&self) -> usize {
        println!("size: called, size: {:?}", std::mem::size_of_val(self));
        std::mem::size_of_val(self)
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = create_context()?;

    // here is where we define the UDAF. We also declare its signature:
    let sum_mean = create_udaf(
        // the name; used to represent it in plan descriptions and in the registry, to use in SQL.
        "sum_mean",
        // the input type; DataFusion guarantees that the first entry of `values` in `update` has this type.
        DataType::Float64,
        // the return type; DataFusion expects this to match the type returned by `evaluate`.
        Arc::new(DataType::Float64),
        Volatility::Immutable,
        // This is the accumulator factory; DataFusion uses it to create new accumulators.
        Arc::new(|_| Ok(Box::new(SumMean::new()))),
        // This is the description of the state. `state()` must match the types here.
        Arc::new(vec![DataType::Float64]),
    );
    ctx.register_udaf(sum_mean);

    let df = ctx
        .sql("SELECT name, sum_mean(value) AS num FROM t GROUP BY name ORDER by num")
        .await?;
    df.show().await?;

    Ok(())
}
