use crate::config::config::{LoggerType, RunnerConfig};
use miette::{IntoDiagnostic, Result};
use std::{
    fs::File,
    io::Write,
    sync::{Arc, Mutex},
};
use tracing::error;

pub type FrameLogger = Box<dyn FrameLoggerT + Sync + Send>;

pub trait FrameLoggerT {
    // Open logger for writing
    fn writeln(&self, line: &str);
}

pub struct FrameLoggerBuilder {}

impl FrameLoggerBuilder {
    pub fn fromLoggerConfig(
        path: String,
        logger_config: &RunnerConfig,
    ) -> Result<impl FrameLoggerT> {
        match logger_config.logger {
            crate::config::config::LoggerType::File => {
                FrameFileLogger::init(path, logger_config.prepend_timestamp)
            }
        }
    }
}

pub struct FrameFileLogger {
    path: String,
    prepend_timestamp: bool,
    file_descriptor: Mutex<File>,
}

impl FrameFileLogger {
    pub fn init(path: String, prepend_timestamp: bool) -> Result<Self> {
        let file_descriptor = Mutex::new(File::create(path.clone()).into_diagnostic()?);
        // TODO: Check if dir exists, check permissions and cycle old logs
        Ok(FrameFileLogger {
            path,
            prepend_timestamp,
            file_descriptor,
        })
    }
}

impl FrameLoggerT for FrameFileLogger {
    fn writeln(&self, line: &str) {
        let mut fd = self.file_descriptor.lock().unwrap();
        if let Err(io_err) = fd
            .write_all(line.as_bytes())
            .and_then(|_| fd.write_all(b"\n"))
        {
            error!("Failed to write line to frame logger. {io_err:?}")
        };
    }
}
