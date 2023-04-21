use datafusion::error::{DataFusionError, Result};

use crate::value::StackValue;

pub(crate) fn histogram_quantile(phi: f64, _buckets: StackValue) -> Result<StackValue> {
    if !(0. ..=1.).contains(&phi) {
        return Err(DataFusionError::Internal(
            "histogram_quantile: the first argument must be between 0 and 1".to_owned(),
        ));
    }
    todo!("XXX phi={phi}")
}
