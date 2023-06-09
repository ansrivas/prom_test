use datafusion::error::{DataFusionError, Result};

use crate::value::{InstantValue, RangeValue, Sample, Value};

mod avg_over_time;
mod count_over_time;
mod delta;
mod histogram;
mod idelta;
mod increase;
mod irate;
mod max_over_time;
mod min_over_time;
mod rate;
mod sum_over_time;

pub(crate) use avg_over_time::avg_over_time;
pub(crate) use count_over_time::count_over_time;
pub(crate) use delta::delta;
pub(crate) use histogram::histogram_quantile;
pub(crate) use idelta::idelta;
pub(crate) use increase::increase;
pub(crate) use irate::irate;
pub(crate) use max_over_time::max_over_time;
pub(crate) use min_over_time::min_over_time;
pub(crate) use rate::rate;
pub(crate) use sum_over_time::sum_over_time;

use strum::EnumString;

/// Reference: https://prometheus.io/docs/prometheus/latest/querying/functions/
#[derive(Debug, Clone, Copy, PartialEq, EnumString)]
#[strum(serialize_all = "snake_case")]
pub(crate) enum Func {
    Abs,
    Absent,
    AbsentOverTime,
    AvgOverTime,
    Ceil,
    Changes,
    Clamp,
    ClampMax,
    ClampMin,
    CountOverTime,
    DayOfMonth,
    DayOfWeek,
    DayOfYear,
    DaysInMonth,
    Delta,
    Deriv,
    Exp,
    Floor,
    HistogramCount,
    HistogramFraction,
    HistogramQuantile,
    HistogramSum,
    HoltWinters,
    Hour,
    Idelta,
    Increase,
    Irate,
    LabelJoin,
    LabelReplace,
    Ln,
    Log10,
    Log2,
    MaxOverTime,
    MinOverTime,
    Minute,
    Month,
    PredictLinear,
    QuantileOverTime,
    Rate,
    Resets,
    Round,
    Scalar,
    Sgn,
    Sort,
    SortDesc,
    SumOverTime,
    Time,
    Timestamp,
    Vector,
    Year,
}

pub(crate) fn eval_idelta(
    data: &Value,
    fn_name: &str,
    fn_handler: fn(&RangeValue) -> f64,
) -> Result<Value> {
    let data = match data {
        Value::Matrix(v) => v,
        Value::None => return Ok(Value::None),
        _ => {
            return Err(DataFusionError::Internal(format!(
                "{fn_name}: matrix argument expected"
            )))
        }
    };

    let rate_values = data
        .iter()
        .map(|metric| {
            let value = fn_handler(metric);
            InstantValue {
                labels: metric.labels.clone(),
                value: Sample {
                    timestamp: metric.time_range.unwrap().1,
                    value,
                },
            }
        })
        .collect();
    Ok(Value::Vector(rate_values))
}
