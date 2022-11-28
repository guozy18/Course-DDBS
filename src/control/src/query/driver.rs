use super::QueryContext;
use common::Profiler;
use std::sync::Arc;

#[derive(Default)]
pub struct Driver {
    query: String,
    ctx: Arc<QueryContext>,
    profiler: Profiler,
}

impl Driver {
    pub fn new() -> Self {
        Driver::default()
    }
}
