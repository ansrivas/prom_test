use std::{ops::Index, sync::Arc};

use serde::ser::{Serialize, SerializeMap, Serializer};

/// Label is a key/value pair of strings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Label {
    pub(crate) name: String,
    pub(crate) value: String,
}

/// `Labels` is a sorted set of `Label`s.
#[derive(Debug, Clone, Default)]
pub struct Labels(Vec<Arc<Label>>);

impl Labels {
    pub fn new<I, S>(labels: I) -> Self
    where
        I: IntoIterator<Item = (S, S)>,
        S: AsRef<str>,
    {
        let mut labels = labels
            .into_iter()
            .map(|(k, v)| {
                Arc::new(Label {
                    name: k.as_ref().to_owned(),
                    value: v.as_ref().to_owned(),
                })
            })
            .collect::<Vec<_>>();
        labels.sort_by(|a, b| a.name.cmp(&b.name));
        assert!(
            !labels.windows(2).any(|w| w[0].name == w[1].name),
            "label names are not unique"
        );
        Self(labels)
    }

    /// Returns the value of the label with given name.
    pub fn get(&self, name: &str) -> Option<&str> {
        self.0
            .binary_search_by_key(&name, |label| label.name.as_str())
            .ok()
            .map(|index| self.0[index].value.as_str())
    }

    /// Retains only the labels specified by the predicate.
    pub(crate) fn retain<P>(&mut self, mut predicate: P)
    where
        P: FnMut(&Label) -> bool,
    {
        self.0.retain(|label| predicate(label.as_ref()))
    }

    pub fn signature(&self) -> Signature {
        self.signature_without_labels(&[])
    }

    /// `signature_without_labels` is just as `signature`, but only for labels
    /// not matching `names`
    pub(crate) fn signature_without_labels(&self, exclude_names: &[&str]) -> Signature {
        let mut hasher = blake3::Hasher::new();
        self.0
            .iter()
            .filter(|label| !exclude_names.contains(&label.name.as_str()))
            .for_each(|label| {
                hasher.update(label.name.as_bytes());
                hasher.update(label.value.as_bytes());
            });
        Signature(hasher.finalize().into())
    }
}

impl Index<&str> for Labels {
    type Output = str;

    fn index(&self, key: &str) -> &Self::Output {
        let found = self
            .0
            .iter()
            .find(|label| label.name == key)
            .expect("no label found for key {key:?}");
        found.value.as_str()
    }
}

impl Serialize for Labels {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(Some(self.0.len()))?;
        for label in &self.0 {
            map.serialize_entry(&label.name, &label.value)?;
        }
        map.end()
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Hash)]
pub struct Signature([u8; 32]);

impl From<Signature> for String {
    fn from(sig: Signature) -> Self {
        hex::encode(sig.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use expect_test::expect;

    #[test]
    fn test_labels() {
        let labels = Labels::new([("b", "1"), ("a", "2"), ("c", "2")]);
        expect![[r#"
            Labels(
                [
                    Label {
                        name: "a",
                        value: "2",
                    },
                    Label {
                        name: "b",
                        value: "1",
                    },
                    Label {
                        name: "c",
                        value: "2",
                    },
                ],
            )
        "#]]
        .assert_debug_eq(&labels);

        expect![[r#"
            Labels(
                [],
            )
        "#]]
        .assert_debug_eq(&Labels::default());
    }

    #[test]
    #[should_panic]
    fn test_labels_not_unique() {
        Labels::new([("b", "1"), ("a", "2"), ("a", "3"), ("a", "2")]);
    }

    #[test]
    fn test_labels_get_retain() {
        let mut labels = Labels::new([("a", "1"), ("b", "2")]);
        assert_eq!(labels.get("b"), Some("2"));
        assert!(labels.get("x").is_none());

        // NOTE: if you are sure that the name exists, you can index by it
        assert_eq!(&labels["a"], "1");

        labels.retain(|label| label.name != "b");
        assert!(labels.get("b").is_none());
    }

    #[test]
    fn test_labels_index() {
        let labels = Labels::new([("a", "1"), ("b", "2")]);
        assert_eq!(&labels["a"], "1");
        assert_eq!(&labels["b"], "2");
    }

    #[test]
    #[should_panic]
    fn test_labels_index_not_found() {
        let labels = Labels::new([("a", "1")]);
        let _ = &labels["b"];
    }

    #[test]
    fn test_labels_signature() {
        let labels = Labels::new([("b", "2"), ("a", "1"), ("c", "3"), ("d", "4")]);

        let sig = labels.signature();
        expect![[r#"
            "f287fde2994111abd7740b5c7c28b0eeabe3f813ae65397bb6acb684e2ab6b22"
        "#]]
        .assert_debug_eq(&String::from(sig));

        let sig: String = labels.signature_without_labels(&["a", "c"]).into();
        expect![[r#"
            "ec9c3a0c9c03420d330ab62021551cffe993c07b20189c5ed831dad22f54c0c7"
        "#]]
        .assert_debug_eq(&sig);
        assert_eq!(sig.len(), 64);

        assert_eq!(labels.signature(), labels.signature_without_labels(&[]));
    }

    #[test]
    fn test_labels_serialize() {
        let labels = Labels::new([("b", "1"), ("a", "2"), ("c", "2")]);
        expect![[r#"
            {
              "a": "2",
              "b": "1",
              "c": "2"
            }"#]]
        .assert_eq(&serde_json::to_string_pretty(&labels).unwrap());
    }
}
