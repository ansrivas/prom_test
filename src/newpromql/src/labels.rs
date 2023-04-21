use std::collections::HashSet;

use serde::{Deserialize, Serialize};

//XXX // Well-known label names used by Prometheus components.
//XXX // Source: https://github.com/prometheus/prometheus/blob/f7c6130ff27a2a12412c02cce223f7a8abc59e49/model/labels/labels_string.go#L30-L36
//XXX pub(crate) const METRIC_NAME: &str = "__name__";
//XXX pub(crate) const BUCKET_NAME: &str = "le";

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Label {
    pub name: String,
    pub value: String,
}

pub type Signature = HashSet<Label>;
