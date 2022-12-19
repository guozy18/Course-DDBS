use super::{Result, RuntimeError};
use mysql::{from_value_opt, prelude::FromValue, FromValueError, Row, Value};
use serde::{Deserialize, Serialize};
use std::{
    fmt::Display,
    ops::{Deref, DerefMut},
};

#[derive(Debug, Serialize, Deserialize)]
#[serde(remote = "Value")]
pub enum ValueDef {
    NULL,
    Bytes(Vec<u8>),
    Int(i64),
    UInt(u64),
    Float(f32),
    Double(f64),
    /// year, month, day, hour, minutes, seconds, micro seconds
    Date(u16, u8, u8, u8, u8, u8, u32),
    /// is negative, days, hours, minutes, seconds, micro seconds
    Time(bool, u32, u8, u8, u8, u32),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ValueAdaptor(#[serde(with = "ValueDef")] Value);
impl ValueAdaptor {
    pub fn new(v: Value) -> Self {
        Self(v)
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct MyRow(Vec<Option<ValueAdaptor>>);

impl Deref for MyRow {
    type Target = Vec<Option<ValueAdaptor>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for MyRow {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl From<Row> for MyRow {
    fn from(row: Row) -> Self {
        let values = row
            .unwrap_raw()
            .into_iter()
            .map(|ele| ele.map(ValueAdaptor::new))
            .collect::<Vec<_>>();
        Self(values)
    }
}

pub struct BeRead<'a> {
    pub aid: &'a str,
    read_num: u64,
    read_uid_list: &'a str,
    comment_num: u64,
    comment_uid_list: &'a str,
    agree_num: u64,
    agree_uid_list: &'a str,
    share_num: u64,
    share_uid_list: &'a str,
}

impl<'a> Display for BeRead<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            r#"{}, {}, "{}", {}, "{}", {}, "{}", {}, "{}""#,
            self.aid,
            self.read_num,
            self.read_uid_list,
            self.comment_num,
            self.comment_uid_list,
            self.agree_num,
            self.agree_uid_list,
            self.share_num,
            self.share_uid_list
        )
    }
}

impl MyRow {
    pub fn get_row_value<T>(&mut self, idx: usize) -> Result<T>
    where
        T: FromValue,
    {
        let v = self
            .get_mut(idx)
            .ok_or_else(|| RuntimeError::DBTypeParseError(format!("idx {idx} out of range")))?
            .take()
            .ok_or_else(|| RuntimeError::DBTypeParseError(format!("idx {idx} has been taken")))?
            .0;
        // if converting to type T failed, need to restore the Value in the Row
        match from_value_opt(v) {
            Ok(val) => Ok(val),
            Err(FromValueError(val)) => {
                self.get_mut(idx).unwrap().replace(ValueAdaptor::new(val));
                Err(RuntimeError::DBTypeParseError(format!(
                    "cannot parse idx {idx} to desired type"
                )))
            }
        }
    }

    pub fn get_row_bytes(&self, idx: usize) -> Result<&[u8]> {
        let value = self
            .get(idx)
            .ok_or_else(|| {
                RuntimeError::DBTypeParseError(format!("cannot get idx {idx} of row: {self:?}"))
            })?
            .as_ref()
            .ok_or_else(|| RuntimeError::DBTypeParseError(format!("idx {idx} has been taken")))?;
        match &value.0 {
            Value::Bytes(v) => Ok(v),
            Value::NULL => Ok(&[]),
            _ => Err(RuntimeError::DBTypeParseError(format!(
                "cannot get bytes in {idx} of row: {self:?}"
            ))),
        }
    }

    pub fn get_row_str(&self, idx: usize) -> Result<&str> {
        Ok(std::str::from_utf8(self.get_row_bytes(idx)?)?)
    }

    pub fn get_raw_value(&self) -> Result<Vec<Value>> {
        let res = self
            .0
            .clone()
            .into_iter()
            .map(|value| {
                value
                    .map(|value_adaptor| value_adaptor.0)
                    .ok_or_else(|| RuntimeError::ReadlineError)
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(res)
    }
}

impl<'a> BeRead<'a> {
    /// this function takes a reference instead of owner ship
    pub fn from(row: &'a mut MyRow) -> Result<BeRead<'a>> {
        let row_ptr = row as *mut MyRow;
        // SAFETY: all reference access non-overlap fields of row
        unsafe {
            Ok(Self {
                aid: row.get_row_str(0)?,
                read_num: (*row_ptr).get_row_value(1)?,
                read_uid_list: row.get_row_str(2)?,
                agree_num: (*row_ptr).get_row_value(3)?,
                agree_uid_list: row.get_row_str(4)?,
                comment_num: (*row_ptr).get_row_value(5)?,
                comment_uid_list: row.get_row_str(6)?,
                share_num: (*row_ptr).get_row_value(7)?,
                share_uid_list: row.get_row_str(8)?,
            })
        }
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Copy)]
pub enum MyDate {
    Simple(u64),
    Date(time::Date),
}

impl TryFrom<Value> for MyDate {
    type Error = RuntimeError;

    fn try_from(v: Value) -> Result<Self> {
        let date = match from_value_opt::<u64>(v) {
            Ok(val) => MyDate::Simple(val),
            Err(mysql::FromValueError(v)) => MyDate::Date(
                from_value_opt::<time::Date>(v)
                    .map_err(|e| RuntimeError::DBTypeParseError(e.to_string()))?,
            ),
        };
        Ok(date)
    }
}

impl TryFrom<ValueAdaptor> for MyDate {
    type Error = RuntimeError;

    fn try_from(v: ValueAdaptor) -> Result<Self> {
        v.0.try_into()
    }
}

impl Display for MyDate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Simple(val) => write!(f, "{val}"),
            Self::Date(date) => write!(f, "{date}"),
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct PopularArticle {
    pub aid: String,
    pub date: MyDate,
    pub read_num: u64,
}

impl TryFrom<MyRow> for PopularArticle {
    type Error = RuntimeError;

    fn try_from(mut row: MyRow) -> Result<Self> {
        let row_ptr = &mut row as *mut MyRow;
        let date = match row.get_row_value::<u64>(1) {
            Ok(val) => MyDate::Simple(val),
            Err(_) => MyDate::Date(row.get_row_value::<time::Date>(1)?),
        };
        unsafe {
            Ok(Self {
                aid: (*row_ptr).get_row_value(0)?,
                date,
                read_num: (*row_ptr).get_row_value(2)?,
            })
        }
    }
}

impl PartialOrd for PopularArticle {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PopularArticle {
    // used for min-heap
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match self.read_num.cmp(&other.read_num) {
            core::cmp::Ordering::Equal => {}
            core::cmp::Ordering::Greater => return core::cmp::Ordering::Less,
            core::cmp::Ordering::Less => return core::cmp::Ordering::Greater,
        };
        match self.aid.cmp(&other.aid) {
            core::cmp::Ordering::Equal => {}
            ord => return ord,
        };
        self.date.cmp(&other.date)
    }
}
