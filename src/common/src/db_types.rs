use std::fmt::Display;

use super::{Result, RuntimeError};
use mysql::{from_value_opt, prelude::FromValue, Row, Value};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
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

fn get_row_value<T>(row: &mut Row, idx: usize) -> Result<T>
where
    T: FromValue,
{
    let v = row.take(idx).ok_or(RuntimeError::DBTypeParseError(format!(
        "cannot get idx {idx} of row: {row:?}"
    )))?;
    from_value_opt(v).map_err(|e| RuntimeError::DBTypeParseError(e.to_string()))
}

fn get_row_bytes(row: &Row, idx: usize) -> Result<&[u8]> {
    let value = row
        .as_ref(idx)
        .ok_or(RuntimeError::DBTypeParseError(format!(
            "cannot get idx {idx} of row: {row:?}"
        )))?;
    match value {
        Value::Bytes(v) => Ok(v),
        Value::NULL => Ok(&[]),
        _ => Err(RuntimeError::DBTypeParseError(format!(
            "cannot get bytes in {idx} of row: {row:?}"
        ))),
    }
}

fn get_row_str(row: &Row, idx: usize) -> Result<&str> {
    Ok(std::str::from_utf8(get_row_bytes(row, idx)?)?)
}

impl<'a> BeRead<'a> {
    pub fn from(row: &'a mut Row) -> Result<BeRead<'a>> {
        let row_ptr = row as *mut Row;
        // SAFETY: all reference access non-overlap fields of row
        unsafe {
            Ok(Self {
                aid: get_row_str(row, 0)?,
                read_num: get_row_value(&mut *row_ptr, 1)?,
                read_uid_list: get_row_str(row, 2)?,
                agree_num: get_row_value(&mut *row_ptr, 3)?,
                agree_uid_list: get_row_str(row, 4)?,
                comment_num: get_row_value(&mut *row_ptr, 5)?,
                comment_uid_list: get_row_str(row, 6)?,
                share_num: get_row_value(&mut *row_ptr, 7)?,
                share_uid_list: get_row_str(row, 8)?,
            })
        }
    }
}
