use crate::Profile;
use mysql::Value;

use serde::{ser::SerializeMap, Serialize};
/// The list of returned records.
#[derive(Debug, Serialize, Default)]
pub struct ResultSet {
    /// The headers of the table.
    pub header: Vec<String>,
    /// The result of the table.
    #[serde(serialize_with = "serialize_value_table")]
    pub table: Vec<Vec<Value>>,
}

// impl Default for ResultSet {
//     fn default() -> Self {
//         ResultSet { header: vec![], table: vec![] }
//     }
// }
impl ResultSet {
    pub fn new() -> Self {
        ResultSet::default()
    }

    pub fn set_header(&mut self, header: impl IntoIterator<Item = String>) {
        self.header = header.into_iter().collect();
    }
}

struct ValueWrapper<'a>(&'a Value);

impl<'a> Serialize for ValueWrapper<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self.0 {
            Value::NULL => serializer.serialize_none(),
            Value::Bytes(bytes) => {
                let str = std::str::from_utf8(bytes).unwrap();
                serializer.collect_str(str)
            }
            Value::Int(v) => serializer.serialize_i64(*v),
            Value::UInt(v) => serializer.serialize_u64(*v),
            Value::Float(v) => serializer.serialize_f32(*v),
            Value::Double(v) => serializer.serialize_f64(*v),
            Value::Date(..) => unreachable!(),
            Value::Time(..) => unreachable!(),
        }
    }
}

fn serialize_value_table<S>(table: &[Vec<Value>], serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    use serde::ser::SerializeSeq;
    let mut seq = serializer.serialize_seq(Some(table.len()))?;
    for row in table {
        let wrapper_vec: Vec<ValueWrapper<'_>> = row.iter().map(ValueWrapper).collect();
        seq.serialize_element(&wrapper_vec)?;
    }
    seq.end()
}

/// The result of an execution.
#[derive(Debug, Default)]
pub struct ExecuteResult {
    /// The result set in table format.
    pub result_set: Option<ResultSet>,
    /// The elapsed time during the execution, in seconds.
    pub profile: Profile,
}

impl Serialize for ExecuteResult {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut object = serializer.serialize_map(None)?;

        object.serialize_entry("resultSet", &self.result_set)?;
        object.serialize_entry("profile", &self.profile)?;

        object.end()
    }
}
