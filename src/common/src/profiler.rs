use std::time::Instant;

/// The performance profile of a query execution.
#[derive(Default, Debug, Clone, Copy, serde::Serialize)]
#[serde(rename_all(serialize = "camelCase"))]
pub struct Profile {
    /// The total time elapsed during execution, in milliseconds.
    pub total_time: f64,
    /// The parser time, in milliseconds.
    pub parser_time: f64,
    /// The rewrite time, in milliseconds.
    pub rewrite_time: f64,
    /// The execution time, in milliseconds.
    pub exec_time: f64,
}

/// A profiler that measures the performance of stages during query.
#[derive(Debug)]
pub struct Profiler {
    profile: Profile,
    start_time: Instant,
    last_time: Instant,
}

impl Default for Profiler {
    fn default() -> Self {
        Profiler::new()
    }
}

impl Profiler {
    pub fn new() -> Profiler {
        Profiler {
            profile: Default::default(),
            start_time: Instant::now(),
            last_time: Instant::now(),
        }
    }

    /// Get the time interval from last interval in ms,
    /// then reset the `last_time`.
    fn phase_time(&mut self) -> f64 {
        let interval = self.last_time.elapsed().as_secs_f64() * 1000.0;
        self.last_time = Instant::now();
        interval
    }

    pub fn parse_finished(&mut self) {
        self.profile.parser_time = self.phase_time();
    }

    pub fn rewrite_finished(&mut self) {
        self.profile.rewrite_time = self.phase_time();
    }

    pub fn exec_finished(&mut self) {
        self.profile.exec_time = self.phase_time();
    }

    #[allow(dead_code)]
    pub fn reset_last(&mut self) {
        self.last_time = Instant::now();
    }

    pub fn finished(&mut self) -> Profile {
        self.profile.total_time = self.start_time.elapsed().as_secs_f64() * 1000.0;
        self.profile
    }
}
