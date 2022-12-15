pub use common::{
    Result, RuntimeError,
    RuntimeError::{JsonContentError, JsonParseError},
};

pub use std::{fmt::Write, str::FromStr};

pub use prettytable::{cell, color, row, Attr, Cell, Row, Table};
pub use serde_json::Value;

pub fn format_output(data: &str) -> Result<String> {
    let v: Value = serde_json::from_str(data).map_err(|_| JsonParseError)?;

    let profile = &v["profile"];
    let total_time = profile["totalTime"].as_f64().unwrap_or(f64::NAN);
    let parser_time = profile["parserTime"].as_f64().unwrap_or(f64::NAN);
    let rewrite_time = profile["rewriteTime"].as_f64().unwrap_or(f64::NAN);
    let exec_time = profile["execTime"].as_f64().unwrap_or(f64::NAN);

    let mut ret = String::new();

    if let Value::Object(result_set) = &v["resultSet"] {
        let header = result_set
            .get("header")
            .and_then(|h| h.as_array())
            .ok_or(JsonContentError)?
            .iter()
            .map(|v| v.as_str().map(|x| x.to_string()))
            .collect::<Option<Vec<_>>>()
            .ok_or(JsonContentError)?;

        let result_set_results = result_set
            .get("table")
            .ok_or(JsonContentError)?
            .as_array()
            .ok_or(JsonContentError)?
            .iter()
            .map(|row| {
                Ok::<_, RuntimeError>(
                    row.as_array()
                        .ok_or(JsonContentError)?
                        .iter()
                        .map(format_value)
                        .collect(),
                )
            });

        let mut result_set = vec![];
        for x in result_set_results {
            result_set.push(x?);
        }
        if !header.is_empty() {
            ret += &table_string(&header, &result_set);
        }
        ret += &format!("{} lines returned.\n", result_set.len());
    } else {
        ret += "No result produced.\n";
    }

    ret += &format!(
        "Time: total {total_time:.2} ms, parser {parser_time:.2} ms, rewrite {rewrite_time:.2} ms, execution {exec_time:.2} ms\n"
    );
    Ok(ret)
}

fn table_string(titles: &[String], contents: &[Vec<String>]) -> String {
    let mut table = Table::new();
    table.add_row(Row::new(titles.iter().map(|t| Cell::new(t)).collect()));
    for row_content in contents.iter() {
        table.add_row(Row::new(row_content.iter().map(|r| Cell::new(r)).collect()));
    }
    table.to_string()
}

fn format_value(val: &Value) -> String {
    val.to_string()
}
